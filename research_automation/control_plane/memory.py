"""Scoped Learning decisions and bounded context projection for P5.

This module is read-only with respect to Learning Commit state.  It consumes
structured, already committed facts and never confers authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from collections import deque
from collections.abc import Mapping, Sequence
from copy import deepcopy
from hashlib import sha256
import json


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

_EVIDENCE_GRADE_RANK = {
    "EXPLORATORY": 0,
    "STRICT_FORWARD_VALIDATED": 1,
    "INDEPENDENTLY_REPRODUCED": 2,
}
_LEARNING_CLAIM_KINDS = frozenset(
    {"POSITIVE", "NEGATIVE", "PARTIAL", "ANTI_FACTOR", "FAILED_USAGE"}
)
_NEGATIVE_CLAIM_KINDS = frozenset({"NEGATIVE", "ANTI_FACTOR", "FAILED_USAGE"})
_REOPEN_PREDICATES = frozenset(
    {
        "NEW_MECHANISM",
        "NEW_USAGE_MODE",
        "NEW_MARKET_REGIME",
        "NEW_TIME_WINDOW",
        "NEW_UNIVERSE",
        "NEW_LIQUIDITY_BUCKET",
        "DATA_DRIFT",
        "STRONGER_EVIDENCE",
        "DECLARED_RESEARCH_GAP",
    }
)
_CONCLUSION_CODES = frozenset(
    {
        "POSITIVE_DIRECTIONAL",
        "NEGATIVE_DIRECTIONAL",
        "HARD_GATE_FAILED",
        "USAGE_FAILED",
        "ANTI_FACTOR",
        "REGIME_CONDITIONAL",
        "NO_MATERIAL_FINDING",
        "DO_NOT_HARD_GATE",
    }
)
_DIRECTIONAL_STATUSES = frozenset(
    {
        "research_only",
        "not_promoted",
        "do_not_hard_gate",
        "positive_directional",
        "negative_directional",
        "anti_factor",
        "regime_conditional",
    }
)
_IDENTIFIER_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:/-"
)
_PROJECTED_CLAIM_FIELDS = frozenset(
    {
        "claim_id",
        "kind",
        "conclusion",
        "scope",
        "audit_grade",
        "evidence_grade",
        "evidence_refs",
        "taint_refs",
        "invalidation_codes",
        "reopen_predicates",
        "parent_claim_ids",
        "directional_status",
    }
)
_EXCLUSION_CODES = frozenset(
    {
        "AUDIT_GRADE_NOT_PASS",
        "TAINTED_CLAIM",
        "INVALIDATED_CLAIM",
        "PARENT_INVALIDATED",
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
            or not item[0].isalnum()
            or any(character not in _IDENTIFIER_CHARS for character in item)
            for item in value
        )
        or value != sorted(set(value))
    ):
        raise ValueError(f"scope.{field_name} must be a sorted unique string array")
    return tuple(value)


def _canonical_refs(value: object, field_name: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or any(
            not isinstance(item, str)
            or not item
            or item != item.strip()
            or len(item) > 256
            for item in value
        )
        or value != sorted(set(value))
    ):
        raise ValueError(f"{field_name} must be a sorted unique string array")
    return tuple(value)


def _canonical_identifier(value: object, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 256
        or not value[0].isalnum()
        or any(character not in _IDENTIFIER_CHARS for character in value)
    ):
        raise ValueError(f"{field_name} must be a canonical identifier")
    return value


def _canonical_identifiers(value: object, field_name: str) -> tuple[str, ...]:
    refs = _canonical_refs(value, field_name)
    for item in refs:
        _canonical_identifier(item, field_name)
    return refs


def _opaque_ref(domain: str, value: str) -> str:
    return sha256(
        f"control_plane.context_projection.v1:{domain}\0{value}".encode("utf-8")
    ).hexdigest()


def _opaque_scope(scope_value: ClaimScope) -> dict[str, object]:
    scope_mapping = scope_value.to_mapping()
    for field_name in _SCOPE_FIELDS - {"time_windows"}:
        scope_mapping[field_name] = [
            _opaque_ref(f"scope.{field_name}", item)
            for item in scope_mapping[field_name]
        ]
    return scope_mapping


def _is_opaque_ref(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _validate_projected_scope(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != _SCOPE_FIELDS:
        raise ValueError("context projection claim scope is invalid")
    result: dict[str, object] = {}
    for field_name in _SCOPE_FIELDS - {"time_windows"}:
        items = value.get(field_name)
        if (
            not isinstance(items, list)
            or not items
            or len(items) != len(set(items))
            or any(not _is_opaque_ref(item) for item in items)
        ):
            raise ValueError("context projection claim scope is invalid")
        result[field_name] = list(items)
    windows = value.get("time_windows")
    if not isinstance(windows, list) or not windows:
        raise ValueError("context projection claim scope is invalid")
    parsed = [TimeWindow.from_mapping(item) for item in windows]
    if parsed != sorted(set(parsed)):
        raise ValueError("context projection claim scope is invalid")
    result["time_windows"] = [window.to_mapping() for window in parsed]
    return result


def _claim_scope_from_projected(value: Mapping[str, object]) -> ClaimScope:
    return ClaimScope(
        mechanisms=tuple(value["mechanisms"]),
        usage_modes=tuple(value["usage_modes"]),
        market_regimes=tuple(value["market_regimes"]),
        time_windows=tuple(TimeWindow.from_mapping(item) for item in value["time_windows"]),
        universes=tuple(value["universes"]),
        liquidity_buckets=tuple(value["liquidity_buckets"]),
        label_protocol_families=tuple(value["label_protocol_families"]),
        generation_families=tuple(value["generation_families"]),
    )


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

    def to_mapping(self) -> dict[str, object]:
        return {
            "mechanisms": list(self.mechanisms),
            "usage_modes": list(self.usage_modes),
            "market_regimes": list(self.market_regimes),
            "time_windows": [window.to_mapping() for window in self.time_windows],
            "universes": list(self.universes),
            "liquidity_buckets": list(self.liquidity_buckets),
            "label_protocol_families": list(self.label_protocol_families),
            "generation_families": list(self.generation_families),
        }


def _nonempty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a canonical non-empty string")
    return value


def _validate_claim_lineage(
    claims: Sequence[Mapping[str, object]],
) -> tuple[list[Mapping[str, object]], set[str]]:
    if not isinstance(claims, Sequence) or isinstance(claims, (str, bytes)):
        raise ValueError("claims must be a sequence")
    claim_rows = list(claims)
    parents_by_claim: dict[str, tuple[str, ...]] = {}
    invalidated_claim_ids: set[str] = set()
    for raw_claim in claim_rows:
        if not isinstance(raw_claim, Mapping):
            raise ValueError("claim must be a mapping")
        claim_id = _nonempty_string(raw_claim.get("claim_id"), "claim.claim_id")
        if claim_id in parents_by_claim:
            raise ValueError("claim.claim_id must be unique")
        parent_ids = raw_claim.get("parent_claim_ids", [])
        if (
            not isinstance(parent_ids, list)
            or any(
                not isinstance(item, str) or not item or item != item.strip()
                for item in parent_ids
            )
            or parent_ids != sorted(set(parent_ids))
        ):
            raise ValueError(
                "claim.parent_claim_ids must be a sorted unique string array"
            )
        invalidation_codes = raw_claim.get("invalidation_codes")
        if not isinstance(invalidation_codes, list) or any(
            not isinstance(item, str) or not item or item != item.strip()
            for item in invalidation_codes
        ):
            raise ValueError("claim.invalidation_codes must be a string array")
        audit_grade = raw_claim.get("audit_grade")
        if not isinstance(audit_grade, str) or not audit_grade:
            raise ValueError("claim.audit_grade must be a non-empty string")
        taint_refs = raw_claim.get("taint_refs")
        if not isinstance(taint_refs, list) or any(
            not isinstance(item, str) or not item or item != item.strip()
            for item in taint_refs
        ):
            raise ValueError("claim.taint_refs must be a string array")
        parents_by_claim[claim_id] = tuple(parent_ids)
        if audit_grade != "PASS" or taint_refs or invalidation_codes:
            invalidated_claim_ids.add(claim_id)
    known_claim_ids = set(parents_by_claim)
    for claim_id, parent_ids in parents_by_claim.items():
        if claim_id in parent_ids:
            raise ValueError("claim lineage cannot contain a self-parent edge")
        if not set(parent_ids).issubset(known_claim_ids):
            raise ValueError("claim lineage references an unknown parent")
    children_by_parent: dict[str, list[str]] = {
        claim_id: [] for claim_id in known_claim_ids
    }
    remaining_parent_count = {
        claim_id: len(parent_ids)
        for claim_id, parent_ids in parents_by_claim.items()
    }
    for claim_id, parent_ids in parents_by_claim.items():
        for parent_id in parent_ids:
            children_by_parent[parent_id].append(claim_id)
    ready = deque(
        claim_id
        for claim_id, parent_count in remaining_parent_count.items()
        if parent_count == 0
    )
    parent_invalidated_ids: set[str] = set()
    visited_count = 0
    while ready:
        parent_id = ready.popleft()
        visited_count += 1
        parent_is_invalid = (
            parent_id in invalidated_claim_ids
            or parent_id in parent_invalidated_ids
        )
        for child_id in children_by_parent[parent_id]:
            if parent_is_invalid:
                parent_invalidated_ids.add(child_id)
            remaining_parent_count[child_id] -= 1
            if remaining_parent_count[child_id] == 0:
                ready.append(child_id)
    if visited_count != len(known_claim_ids):
        raise ValueError("claim lineage cannot contain a cycle")
    return claim_rows, parent_invalidated_ids


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
        claim_rows, parent_invalidated_ids = _validate_claim_lineage(claims)
        matches: list[dict[str, object]] = []
        excluded_claims: list[dict[str, object]] = []
        hard_blocks: list[str] = []
        scoped_blocks: list[dict[str, object]] = []
        warning_codes: set[str] = set()
        for raw_claim in claim_rows:
            if not isinstance(raw_claim, Mapping):
                raise ValueError("claim must be a mapping")
            universal_rejection = raw_claim.get("universal_factor_rejection", False)
            if type(universal_rejection) is not bool:
                raise ValueError(
                    "claim.universal_factor_rejection must be an exact boolean"
                )
            if universal_rejection:
                raise ValueError(
                    "universal_factor_rejection is derived, not manually supplied"
                )
            claim_id = _nonempty_string(raw_claim.get("claim_id"), "claim.claim_id")
            kind = raw_claim.get("kind")
            if kind not in {
                "POSITIVE",
                "NEGATIVE",
                "PARTIAL",
                "ANTI_FACTOR",
                "FAILED_USAGE",
            }:
                raise ValueError("claim.kind is invalid")
            audit_grade = raw_claim.get("audit_grade")
            taint_refs = raw_claim.get("taint_refs")
            invalidation_codes = raw_claim.get("invalidation_codes")
            if not isinstance(audit_grade, str) or not audit_grade:
                raise ValueError("claim.audit_grade must be a non-empty string")
            for value, field_name in (
                (taint_refs, "claim.taint_refs"),
                (invalidation_codes, "claim.invalidation_codes"),
            ):
                if not isinstance(value, list) or any(
                    not isinstance(item, str) or not item or item != item.strip()
                    for item in value
                ):
                    raise ValueError(f"{field_name} must be a string array")
            exclusion_codes: list[str] = []
            if audit_grade != "PASS":
                exclusion_codes.append("AUDIT_GRADE_NOT_PASS")
            if taint_refs:
                exclusion_codes.append("TAINTED_CLAIM")
            if invalidation_codes:
                exclusion_codes.append("INVALIDATED_CLAIM")
            if claim_id in parent_invalidated_ids:
                exclusion_codes.append("PARENT_INVALIDATED")
            if exclusion_codes:
                excluded_claims.append(
                    {"claim_id": claim_id, "reason_codes": exclusion_codes}
                )
                continue
            claim_execution = _nonempty_string(
                raw_claim.get("execution_identity"),
                "claim.execution_identity",
            )
            claim_semantic = _nonempty_string(
                raw_claim.get("semantic_identity"),
                "claim.semantic_identity",
            )
            learned_scope = ClaimScope.from_mapping(raw_claim.get("scope"))
            relation = proposal_scope.classify_proposal(learned_scope)
            exact_execution = claim_execution == proposal_execution
            semantic_match = claim_semantic == proposal_semantic
            applicable_scope = proposal_scope.intersection(learned_scope)
            if exact_execution and relation is not ScopeMatch.DISJOINT:
                if kind == "PARTIAL":
                    scoped_blocks.append(
                        {
                            "claim_id": claim_id,
                            "applicable_scope": applicable_scope,
                        }
                    )
                else:
                    hard_blocks.append(claim_id)
            if semantic_match and not exact_execution:
                warning_codes.add("SEMANTIC_SIMILARITY_ONLY")
            matches.append(
                {
                    "claim_id": claim_id,
                    "kind": kind,
                    "scope_match": relation.value,
                    "applicable_scope": applicable_scope,
                    "exact_execution_identity": exact_execution,
                    "semantic_similarity": semantic_match,
                }
            )
        return {
            "schema_version": "control_plane.learning_gate_decision.v1",
            "enforcement": (
                "HARD_BLOCK"
                if hard_blocks
                else "SCOPED_BLOCK"
                if scoped_blocks
                else "ALLOW"
            ),
            "hard_block_claim_ids": hard_blocks,
            "scoped_block_claims": scoped_blocks,
            "warning_codes": sorted(warning_codes),
            "matches": matches,
            "excluded_claims": excluded_claims,
        }


class ConflictClassifier:
    """Classify contradictory Learning facts without resolving them implicitly."""

    def classify(
        self,
        left: Mapping[str, object],
        right: Mapping[str, object],
        *,
        actor_event: Mapping[str, object],
    ) -> dict[str, object]:
        if not isinstance(left, Mapping) or not isinstance(right, Mapping):
            raise ValueError("conflict claims must be mappings")
        if not isinstance(actor_event, Mapping) or set(actor_event) != {
            "event_id",
            "actor_id",
        }:
            raise ValueError("conflict actor_event has an invalid field contract")
        event = {
            "event_id": _nonempty_string(
                actor_event.get("event_id"), "actor_event.event_id"
            ),
            "actor_id": _nonempty_string(
                actor_event.get("actor_id"), "actor_event.actor_id"
            ),
        }
        left_id = _nonempty_string(left.get("claim_id"), "left.claim_id")
        right_id = _nonempty_string(right.get("claim_id"), "right.claim_id")
        if left_id == right_id:
            raise ValueError("conflict claims must be distinct")
        left_execution = _nonempty_string(
            left.get("execution_identity"), "left.execution_identity"
        )
        right_execution = _nonempty_string(
            right.get("execution_identity"), "right.execution_identity"
        )
        left_kind = _nonempty_string(left.get("kind"), "left.kind")
        right_kind = _nonempty_string(right.get("kind"), "right.kind")
        if (
            left_kind not in _LEARNING_CLAIM_KINDS
            or right_kind not in _LEARNING_CLAIM_KINDS
        ):
            raise ValueError("conflict claim kind is invalid")
        left_scope = ClaimScope.from_mapping(left.get("scope"))
        right_scope = ClaimScope.from_mapping(right.get("scope"))
        classification = "NONE"
        resolution_owner = None
        positive_negative = (
            left_kind == "POSITIVE" and right_kind in _NEGATIVE_CLAIM_KINDS
        ) or (right_kind == "POSITIVE" and left_kind in _NEGATIVE_CLAIM_KINDS)
        if left_execution == right_execution and positive_negative:
            classification = "REPRODUCIBILITY_FAILURE"
            resolution_owner = "reproducibility_owner"
        elif left_execution != right_execution:
            left_semantic = _nonempty_string(
                left.get("semantic_identity"), "left.semantic_identity"
            )
            right_semantic = _nonempty_string(
                right.get("semantic_identity"), "right.semantic_identity"
            )
            if left_semantic == right_semantic and positive_negative:
                if "legacy_unaudited" in {
                    left.get("trust_state"),
                    right.get("trust_state"),
                }:
                    classification = "LEGACY_EVIDENCE_CONFLICT"
                    resolution_owner = "legacy_evidence_owner"
                elif left_scope.generation_families != right_scope.generation_families:
                    classification = "DATA_DRIFT_CONFLICT"
                    resolution_owner = "data_steward"
                else:
                    classification = "SCOPE_OR_PROTOCOL_CONFLICT"
                    resolution_owner = "scope_protocol_owner"
        return {
            "schema_version": "control_plane.learning_conflict.v1",
            "claim_ids": sorted([left_id, right_id]),
            "classification": classification,
            "resolution_owner": resolution_owner,
            "actor_event": event,
        }


class ReopenPredicateEvaluator:
    """Evaluate declared reopen predicates from structured proposal facts."""

    def evaluate(
        self,
        proposal: Mapping[str, object],
        claim: Mapping[str, object],
    ) -> dict[str, object]:
        if not isinstance(proposal, Mapping) or not isinstance(claim, Mapping):
            raise ValueError("reopen proposal and claim must be mappings")
        if "manual_bypass" in proposal:
            raise ValueError("manual bypass cannot qualify a reopen decision")
        claim_id = _nonempty_string(claim.get("claim_id"), "claim.claim_id")
        proposal_scope = ClaimScope.from_mapping(proposal.get("scope"))
        learned_scope = ClaimScope.from_mapping(claim.get("scope"))
        predicates = claim.get("reopen_predicates")
        if (
            not isinstance(predicates, list)
            or any(
                not isinstance(item, str) or not item or item != item.strip()
                for item in predicates
            )
            or predicates != sorted(set(predicates))
        ):
            raise ValueError("claim.reopen_predicates must be a sorted unique string array")
        if not set(predicates).issubset(_REOPEN_PREDICATES):
            raise ValueError("claim.reopen_predicates contains an unknown predicate")
        reasons: list[str] = []
        if "NEW_MECHANISM" in predicates and not set(
            proposal_scope.mechanisms
        ).issubset(learned_scope.mechanisms):
            reasons.append("NEW_MECHANISM")
        categorical_reopen_fields = (
            ("NEW_USAGE_MODE", proposal_scope.usage_modes, learned_scope.usage_modes),
            (
                "NEW_MARKET_REGIME",
                proposal_scope.market_regimes,
                learned_scope.market_regimes,
            ),
            ("NEW_UNIVERSE", proposal_scope.universes, learned_scope.universes),
            (
                "NEW_LIQUIDITY_BUCKET",
                proposal_scope.liquidity_buckets,
                learned_scope.liquidity_buckets,
            ),
        )
        for predicate, proposed_values, learned_values in categorical_reopen_fields:
            if predicate in predicates and not set(proposed_values).issubset(
                learned_values
            ):
                reasons.append(predicate)
        if "NEW_TIME_WINDOW" in predicates and any(
            not any(window.is_within(prior) for prior in learned_scope.time_windows)
            for window in proposal_scope.time_windows
        ):
            reasons.append("NEW_TIME_WINDOW")
        if (
            "DATA_DRIFT" in predicates
            and proposal_scope.generation_families
            != learned_scope.generation_families
        ):
            reasons.append("DATA_DRIFT")
        if "STRONGER_EVIDENCE" in predicates:
            proposal_grade = _nonempty_string(
                proposal.get("evidence_grade"), "proposal.evidence_grade"
            )
            learned_grade = _nonempty_string(
                claim.get("evidence_grade"), "claim.evidence_grade"
            )
            if (
                proposal_grade not in _EVIDENCE_GRADE_RANK
                or learned_grade not in _EVIDENCE_GRADE_RANK
            ):
                raise ValueError("evidence_grade is not a recognized grade")
            if (
                _EVIDENCE_GRADE_RANK[proposal_grade]
                > _EVIDENCE_GRADE_RANK[learned_grade]
            ):
                reasons.append("STRONGER_EVIDENCE")
        if "DECLARED_RESEARCH_GAP" in predicates:
            proposal_gaps = _canonical_refs(
                proposal.get("research_gap_refs"), "research_gap_refs"
            )
            declared_gaps = _canonical_refs(
                claim.get("declared_research_gap_refs"),
                "declared_research_gap_refs",
            )
            if not set(proposal_gaps).isdisjoint(declared_gaps):
                reasons.append("DECLARED_RESEARCH_GAP")
        return {
            "schema_version": "control_plane.learning_reopen_decision.v1",
            "claim_id": claim_id,
            "qualified": bool(reasons),
            "reason_codes": reasons,
        }


class ContextProjection:
    """Project committed Learning claims into a structured, safe fact view."""

    def project(
        self, claims: Sequence[Mapping[str, object]]
    ) -> dict[str, object]:
        claim_rows, parent_invalidated_ids = _validate_claim_lineage(claims)
        projected_claims: list[dict[str, object]] = []
        excluded_claims: list[dict[str, object]] = []
        for raw_claim in claim_rows:
            claim_id = _canonical_identifier(
                raw_claim.get("claim_id"), "claim.claim_id"
            )
            kind = _nonempty_string(raw_claim.get("kind"), "claim.kind")
            if kind not in _LEARNING_CLAIM_KINDS:
                raise ValueError("projection claim kind is invalid")
            scope_value = ClaimScope.from_mapping(raw_claim.get("scope"))
            audit_grade = _nonempty_string(
                raw_claim.get("audit_grade"), "claim.audit_grade"
            )
            evidence_grade = _nonempty_string(
                raw_claim.get("evidence_grade"), "claim.evidence_grade"
            )
            if evidence_grade not in _EVIDENCE_GRADE_RANK:
                raise ValueError("projection evidence_grade is invalid")
            reopen_predicates = _canonical_refs(
                raw_claim.get("reopen_predicates"), "claim.reopen_predicates"
            )
            if not set(reopen_predicates).issubset(_REOPEN_PREDICATES):
                raise ValueError("claim.reopen_predicates contains an unknown predicate")
            taint_refs = _canonical_identifiers(
                raw_claim.get("taint_refs"), "claim.taint_refs"
            )
            invalidation_codes = _canonical_identifiers(
                raw_claim.get("invalidation_codes"),
                "claim.invalidation_codes",
            )
            exclusion_codes: list[str] = []
            if audit_grade != "PASS":
                exclusion_codes.append("AUDIT_GRADE_NOT_PASS")
            if taint_refs:
                exclusion_codes.append("TAINTED_CLAIM")
            if invalidation_codes:
                exclusion_codes.append("INVALIDATED_CLAIM")
            if claim_id in parent_invalidated_ids:
                exclusion_codes.append("PARENT_INVALIDATED")
            if exclusion_codes:
                excluded_claims.append(
                    {
                        "claim_id": _opaque_ref("claim_id", claim_id),
                        "reason_codes": exclusion_codes,
                    }
                )
                continue
            conclusion = _canonical_identifier(
                raw_claim.get("conclusion"), "claim.conclusion"
            )
            if conclusion not in _CONCLUSION_CODES:
                raise ValueError("projection conclusion is invalid")
            directional_status = _canonical_identifier(
                raw_claim.get("directional_status"),
                "claim.directional_status",
            )
            if directional_status not in _DIRECTIONAL_STATUSES:
                raise ValueError("projection directional_status is invalid")
            projected_claims.append(
                {
                    "claim_id": _opaque_ref("claim_id", claim_id),
                    "kind": kind,
                    "conclusion": conclusion,
                    "scope": _opaque_scope(scope_value),
                    "audit_grade": audit_grade,
                    "evidence_grade": evidence_grade,
                    "evidence_refs": list(
                        _opaque_ref("evidence_ref", item)
                        for item in _canonical_identifiers(
                            raw_claim.get("evidence_refs"), "claim.evidence_refs"
                        )
                    ),
                    "taint_refs": [
                        _opaque_ref("taint_ref", item) for item in taint_refs
                    ],
                    "invalidation_codes": [
                        _opaque_ref("invalidation_code", item)
                        for item in invalidation_codes
                    ],
                    "reopen_predicates": list(reopen_predicates),
                    "parent_claim_ids": list(
                        _opaque_ref("claim_id", item)
                        for item in _canonical_identifiers(
                            raw_claim.get("parent_claim_ids"),
                            "claim.parent_claim_ids",
                        )
                    ),
                    "directional_status": directional_status,
                }
            )
        return {
            "schema_version": "control_plane.context_projection.v1",
            "claims": projected_claims,
            "excluded_claims": excluded_claims,
        }


class _RegisteredTokenizerAdapter:
    kind = ""

    def __init__(self, *, name: str) -> None:
        self.name = _canonical_identifier(name, "tokenizer_adapter.name")


class AG2TokenizerAdapter(_RegisteredTokenizerAdapter):
    kind = "AG2"

    def __init__(self, *, name: str) -> None:
        super().__init__(name=name)
        from autogen.token_count_utils import count_token

        self._count_token = count_token

    def count_tokens(self, text: str) -> int:
        return self._count_token(text, model=self.name)


class TiktokenTokenizerAdapter(_RegisteredTokenizerAdapter):
    kind = "TIKTOKEN"

    def __init__(self, *, name: str) -> None:
        super().__init__(name=name)
        import tiktoken

        self._encoding = tiktoken.encoding_for_model(name)

    def count_tokens(self, text: str) -> int:
        return len(self._encoding.encode(text))


class ContextAssembler:
    """Build deterministic, role-specific views from ContextProjection only."""

    _ROLE_PRIORITIES = {
        "source_librarian": {},
        "alpha_hunter": {
            "POSITIVE": 0,
            "PARTIAL": 1,
            "NEGATIVE": 2,
            "ANTI_FACTOR": 2,
            "FAILED_USAGE": 2,
        },
        "falsification_officer": {
            "NEGATIVE": 0,
            "ANTI_FACTOR": 0,
            "FAILED_USAGE": 0,
            "PARTIAL": 1,
            "POSITIVE": 2,
        },
        "factor_engineer": {
            "PARTIAL": 0,
            "POSITIVE": 1,
            "NEGATIVE": 1,
            "ANTI_FACTOR": 1,
            "FAILED_USAGE": 1,
        },
    }

    def __init__(
        self,
        *,
        tokenizer_kind: str | None = None,
        tokenizer_name: str | None = None,
        tokenizer_adapter: object | None = None,
    ) -> None:
        if tokenizer_adapter is not None:
            raise ValueError("mutable tokenizer object configuration is forbidden")
        if tokenizer_kind is None and tokenizer_name is None:
            self._tokenizer_kind = "UNKNOWN"
            self._tokenizer_ref = None
            self._tokenizer_counter = None
        else:
            if tokenizer_kind not in {"TIKTOKEN", "AG2"}:
                raise ValueError("tokenizer_kind is invalid")
            name = _canonical_identifier(tokenizer_name, "tokenizer_name")
            self._tokenizer_kind = tokenizer_kind
            self._tokenizer_ref = _opaque_ref("tokenizer_adapter", name)
            if tokenizer_kind == "TIKTOKEN":
                import tiktoken

                encoding = tiktoken.encoding_for_model(name)
                self._tokenizer_counter = lambda text: len(
                    encoding.encode_ordinary(text)
                )
            else:
                from autogen.token_count_utils import count_token

                self._tokenizer_counter = lambda text: count_token(text, model=name)

    def assemble(
        self,
        projection: Mapping[str, object],
        *,
        role: str,
        learning_token_budget: int = 1500,
        control_token_budget: int = 500,
        untrusted_sources: Sequence[Mapping[str, object]] | None = None,
        target_scope: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        if not isinstance(projection, Mapping) or set(projection) != {
            "schema_version",
            "claims",
            "excluded_claims",
        }:
            raise ValueError("context projection has an invalid field contract")
        if projection.get("schema_version") != "control_plane.context_projection.v1":
            raise ValueError("context projection schema is invalid")
        if role not in self._ROLE_PRIORITIES:
            raise ValueError("context role is invalid")
        for budget, maximum, field_name in (
            (learning_token_budget, 1500, "learning_token_budget"),
            (control_token_budget, 500, "control_token_budget"),
        ):
            if type(budget) is not int or budget <= 0 or budget > maximum:
                raise ValueError(
                    f"{field_name} must be an integer from 1 through {maximum}"
                )
        claims = projection.get("claims")
        excluded_claims = projection.get("excluded_claims")
        if not isinstance(claims, list) or not isinstance(excluded_claims, list):
            raise ValueError("context projection collections are invalid")
        priority = self._ROLE_PRIORITIES[role]
        target_scope_mapping = (
            None
            if target_scope is None
            else _opaque_scope(ClaimScope.from_mapping(target_scope))
        )
        target_projected_scope = (
            None
            if target_scope_mapping is None
            else _claim_scope_from_projected(target_scope_mapping)
        )

        def scope_relevance(claim: Mapping[str, object]) -> tuple[int, int]:
            if target_scope_mapping is None:
                return (0, 0)
            claim_scope = claim.get("scope")
            if not isinstance(claim_scope, Mapping):
                raise ValueError("context projection claim scope is invalid")
            claim_projected_scope = _claim_scope_from_projected(claim_scope)
            relation = target_projected_scope.classify_proposal(
                claim_projected_scope
            )
            relation_rank = {
                ScopeMatch.EXACT: 4,
                ScopeMatch.SUBSET: 3,
                ScopeMatch.OVERLAP: 2,
                ScopeMatch.DISJOINT: 0,
            }[relation]
            relevance = 0
            for field_name in _SCOPE_FIELDS - {"time_windows"}:
                claim_values = claim_scope.get(field_name)
                target_values = target_scope_mapping[field_name]
                if not isinstance(claim_values, list):
                    raise ValueError("context projection claim scope is invalid")
                relevance += len(set(claim_values) & set(target_values))
            claim_windows = claim_scope.get("time_windows")
            if not isinstance(claim_windows, list):
                raise ValueError("context projection claim scope is invalid")
            target_windows = target_scope_mapping["time_windows"]
            if any(
                TimeWindow.from_mapping(left).overlaps(TimeWindow.from_mapping(right))
                for left in claim_windows
                for right in target_windows
            ):
                relevance += 1
            return (relation_rank, relevance)
        indexed_claims: list[tuple[int, Mapping[str, object]]] = []
        for index, claim in enumerate(claims):
            if (
                not isinstance(claim, Mapping)
                or set(claim) != _PROJECTED_CLAIM_FIELDS
                or claim.get("kind") not in _LEARNING_CLAIM_KINDS
            ):
                raise ValueError("context projection claim is invalid")
            ref_fields = ("evidence_refs", "taint_refs", "invalidation_codes", "parent_claim_ids")
            if (
                not _is_opaque_ref(claim.get("claim_id"))
                or claim.get("conclusion") not in _CONCLUSION_CODES
                or claim.get("audit_grade") != "PASS"
                or claim.get("evidence_grade") not in _EVIDENCE_GRADE_RANK
                or claim.get("directional_status") not in _DIRECTIONAL_STATUSES
            ):
                raise ValueError("context projection claim is invalid")
            rebuilt_claim = dict(claim)
            rebuilt_claim["scope"] = _validate_projected_scope(claim.get("scope"))
            for field_name in ref_fields:
                refs = claim.get(field_name)
                if (
                    not isinstance(refs, list)
                    or len(refs) != len(set(refs))
                    or any(not _is_opaque_ref(item) for item in refs)
                ):
                    raise ValueError("context projection claim is invalid")
                rebuilt_claim[field_name] = list(refs)
            predicates = claim.get("reopen_predicates")
            if (
                not isinstance(predicates, list)
                or predicates != sorted(set(predicates))
                or not set(predicates).issubset(_REOPEN_PREDICATES)
            ):
                raise ValueError("context projection claim is invalid")
            rebuilt_claim["reopen_predicates"] = list(predicates)
            indexed_claims.append((index, rebuilt_claim))
        validated_excluded_claims: list[dict[str, object]] = []
        for excluded in excluded_claims:
            if not isinstance(excluded, Mapping) or set(excluded) != {
                "claim_id",
                "reason_codes",
            }:
                raise ValueError("context projection excluded claim is invalid")
            claim_ref = excluded.get("claim_id")
            reason_codes = excluded.get("reason_codes")
            if (
                not isinstance(claim_ref, str)
                or len(claim_ref) != 64
                or any(character not in "0123456789abcdef" for character in claim_ref)
                or not isinstance(reason_codes, list)
                or not reason_codes
                or len(reason_codes) != len(set(reason_codes))
                or not set(reason_codes).issubset(_EXCLUSION_CODES)
            ):
                raise ValueError("context projection excluded claim is invalid")
            validated_excluded_claims.append(
                {"claim_id": claim_ref, "reason_codes": list(reason_codes)}
            )
        ordered_claims = [
            deepcopy(claim)
            for _, claim in sorted(
                indexed_claims,
                key=lambda item: (
                    -scope_relevance(item[1])[0],
                    -scope_relevance(item[1])[1],
                    priority.get(str(item[1]["kind"]), 99),
                    -_EVIDENCE_GRADE_RANK.get(
                        str(item[1].get("evidence_grade")), -1
                    ),
                    item[0],
                ),
            )
        ]
        source_rows: Sequence[Mapping[str, object]] = (
            [] if untrusted_sources is None else untrusted_sources
        )
        if not isinstance(source_rows, Sequence) or isinstance(
            source_rows, (str, bytes)
        ):
            raise ValueError("untrusted_sources must be a sequence")
        untrusted_data: list[dict[str, object]] = []
        for source in source_rows:
            if not isinstance(source, Mapping) or set(source) != {
                "source_ref",
                "content",
            }:
                raise ValueError("untrusted source has an invalid field contract")
            source_ref = _canonical_identifier(
                source.get("source_ref"), "untrusted_source.source_ref"
            )
            content = source.get("content")
            if (
                not isinstance(content, str)
                or not content
                or content != content.strip()
                or len(content.encode("utf-8")) > 16 * 1024
            ):
                raise ValueError("untrusted source content is invalid")
            untrusted_data.append(
                {
                    "source_ref": _opaque_ref("untrusted_source", source_ref),
                    "content": content,
                    "trust_label": "UNTRUSTED_DATA",
                    "capabilities": [],
                    "authority_effect": "NONE",
                }
            )
        def count_tokens(value: object) -> int:
            canonical = json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            if self._tokenizer_counter is None:
                encoded = canonical.encode("utf-8")
                return max(1, len(encoded))
            token_count = self._tokenizer_counter(canonical)
            if type(token_count) is not int or token_count < 1:
                raise ValueError("tokenizer adapter returned an invalid token count")
            return token_count

        selected_claims: list[dict[str, object]] = []
        omitted_claim_ids: list[str] = []
        learning_memory = {
            "schema_version": "control_plane.learning_memory.v1",
            "claims": selected_claims,
            "untrusted_data": untrusted_data,
        }
        if count_tokens(learning_memory) <= learning_token_budget:
            for claim in ordered_claims:
                candidate = {
                    **learning_memory,
                    "claims": [*selected_claims, claim],
                }
                if count_tokens(candidate) <= learning_token_budget:
                    selected_claims.append(claim)
                else:
                    omitted_claim_ids.append(str(claim["claim_id"]))
        else:
            omitted_claim_ids.extend(str(claim["claim_id"]) for claim in ordered_claims)
        control_metadata = {
            "projection_schema_version": projection["schema_version"],
            "excluded_claims": validated_excluded_claims,
            "omitted_claim_count": len(omitted_claim_ids),
            "omitted_claims_digest": (
                None
                if not omitted_claim_ids
                else sha256(
                    (
                        "control_plane.omitted_claims.v1\0"
                        + "\0".join(omitted_claim_ids)
                    ).encode("utf-8")
                ).hexdigest()
            ),
        }

        learning_required = count_tokens(learning_memory)
        method = "ESTIMATED" if self._tokenizer_counter is None else "EXACT"
        control_required = 0
        status = (
            "CONTEXT_BUDGET_EXCEEDED"
            if learning_required > learning_token_budget
            else "OK"
        )
        converged = False
        for _ in range(16):
            token_usage = {
                "method": method,
                "tokenizer_kind": self._tokenizer_kind,
                "tokenizer_ref": self._tokenizer_ref,
                "learning_required": learning_required,
                "learning_budget": learning_token_budget,
                "control_required": control_required,
                "control_budget": control_token_budget,
            }
            candidate_required = count_tokens(
                {
                    "schema_version": "control_plane.context_assembly.v1",
                    "status": status,
                    "role": role,
                    "control_metadata": control_metadata,
                    "token_usage": token_usage,
                }
            )
            candidate_status = (
                "CONTEXT_BUDGET_EXCEEDED"
                if learning_required > learning_token_budget
                or candidate_required > control_token_budget
                else "OK"
            )
            if candidate_required == control_required and candidate_status == status:
                converged = True
                break
            control_required = candidate_required
            status = candidate_status
        if not converged:
            raise ValueError("control token accounting did not converge")
        token_usage = {
            "method": method,
            "tokenizer_kind": self._tokenizer_kind,
            "tokenizer_ref": self._tokenizer_ref,
            "learning_required": learning_required,
            "learning_budget": learning_token_budget,
            "control_required": control_required,
            "control_budget": control_token_budget,
        }
        return {
            "schema_version": "control_plane.context_assembly.v1",
            "status": status,
            "role": role,
            "learning_memory": (
                None if learning_required > learning_token_budget else learning_memory
            ),
            "control_metadata": (
                None if control_required > control_token_budget else control_metadata
            ),
            "token_usage": token_usage,
        }


class LearningContextRouter:
    """The sole V3.4 hot path from explicit committed claims to context."""

    def __init__(
        self,
        *,
        tokenizer_kind: str | None = None,
        tokenizer_name: str | None = None,
    ) -> None:
        self._projection = ContextProjection()
        self._assembler = ContextAssembler(
            tokenizer_kind=tokenizer_kind,
            tokenizer_name=tokenizer_name,
        )

    def build_context(
        self,
        claims: Sequence[Mapping[str, object]],
        *,
        role: str,
        learning_token_budget: int = 1500,
        control_token_budget: int = 500,
        untrusted_sources: Sequence[Mapping[str, object]] | None = None,
        target_scope: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        projection = self._projection.project(claims)
        return self._assembler.assemble(
            projection,
            role=role,
            learning_token_budget=learning_token_budget,
            control_token_budget=control_token_budget,
            untrusted_sources=untrusted_sources,
            target_scope=target_scope,
        )


__all__ = [
    "AG2TokenizerAdapter",
    "ClaimScope",
    "ConflictClassifier",
    "ContextAssembler",
    "ContextProjection",
    "LearningGate",
    "LearningContextRouter",
    "ReopenPredicateEvaluator",
    "ScopeMatch",
    "TimeWindow",
    "TiktokenTokenizerAdapter",
]
