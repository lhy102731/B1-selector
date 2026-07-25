# V3.4.1 Scoped Memory and Control-Plane Campaign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use Superpowers subagent-driven-development and test-driven-development. Implementation must remain dry-run/research-only until a separate campaign authorization.

**Goal:** Consume only safe, scoped Learning projections in the next round and provide a resumable, budgeted, roster-frozen two-round control-plane Campaign without opening Final Holdout.

**Architecture:** LearningContextRouter reads the committed Ledger projection, applies exact identity/scoped conflict rules, taint/audit filtering and deterministic context compression. CampaignController owns Campaign/Cycle state, roster, cumulative budget, cycle locks, leases, resume tokens and dry-run overlays. Legacy AutonomousRunner remains an input adapter and cannot write memory directly.

**Tech Stack:** Python standard library, existing MemoryRouter/RegistryGate only as legacy read adapters, unittest; no new dependency.

---

## Files and responsibilities

### Create

- research_automation/control_plane/memory.py — ClaimScope predicates, LearningGate, bounded Memory projection and context builder.
- research_automation/control_plane/campaign.py — Campaign/Cycle FSM, roster manifest, budget ledger, locks/leases, resume and dry-run.
- tests/test_control_plane_memory.py — scoped conflict and safe-context tests.
- tests/test_control_plane_campaign.py — state, budget, concurrency, roster and dry-run tests.

### Modify

- research_automation/control_plane/contracts.py — scope, campaign state, budget and context result types.
- research_automation/autonomous_runner.py:45-80,114-250,691-800 — expose legacy results through the adapter and stop direct memory/KBase writeback.
- run_research_cycle.py:30-78 — route resume/dry-run through CampaignController; preserve CLI compatibility.
- ag2_research/orchestrator.py:86-215 — mark RegistryGate/MemoryRouter as legacy adapters for the new path; do not silently replace their existing behavior for old callers.
- research_automation/automation_controller.py:105-204 — emit Learning Commit references and consume only LearningContextRouter projections.
- tests/test_ag2_memory_and_registry.py, tests/test_automation_resilience.py — assert legacy behavior remains isolated.

## Memory contract

memory.py exposes:

~~~python
@dataclass(frozen=True)
class ClaimScope:
    market_regime: tuple[str, ...]
    time_windows: tuple[tuple[str, str], ...]
    universes: tuple[str, ...]
    liquidity_buckets: tuple[str, ...]
    factor_usage_modes: tuple[str, ...]

class ScopeMatch(str, Enum):
    EXACT = "EXACT"; SUBSET = "SUBSET"; OVERLAP = "OVERLAP"; DISJOINT = "DISJOINT"

class LearningGate:
    def classify(self, proposal: dict, claims: list[dict]) -> dict: ...

class LearningContextRouter:
    def build_context(self, strategy_id: str, proposal: dict, token_budget: int = 1500) -> dict: ...
~~~

Rules:

- Exact execution-spec identity may hard-block a duplicate.
- Semantic similarity only warns.
- PARTIAL applies only to the scope intersection; disjoint scope is not globally blocked.
- INVALID, low-audit, HOLDOUT-tainted and TEST-derived claims are excluded from full context; a redacted audit reference may remain.
- Context over budget uses deterministic field-priority compression or returns CONTEXT_BUDGET_EXCEEDED; it never silently drops scope, evidence grade, taint, invalidation or reopen conditions.

## Campaign contract

campaign.py exposes:

~~~python
class CampaignState(str, Enum):
    DRAFT = "DRAFT"; FROZEN = "FROZEN"; AUTHORIZED = "AUTHORIZED"
    RUNNING = "RUNNING"; PAUSED = "PAUSED"; COMPLETED = "COMPLETED"
    BLOCKED = "BLOCKED"; CLOSED = "CLOSED"

class CampaignController:
    def freeze(self, manifest: dict) -> str: ...
    def authorize_test(self, token: str, rounds: int = 2) -> None: ...
    def start_cycle(self, cycle_id: str) -> None: ...
    def finish_cycle(self, cycle_id: str, result: dict) -> None: ...
    def resume(self, campaign_id: str) -> dict: ...
~~~

The roster manifest stores profile/model/provider identity hash, role, prompt/config hash, required-member flag and resolved API capability. A missing member or drift is BLOCKED, never a roster shrink. Campaign budgets are cumulative across cycles and use atomic reserve/settle records for tokens, API spend, wall time, data access and disk growth. Cycle locks are keyed by campaign_id/cycle_id; shared projection writes use a separate short lock.

## Task 1: Machine-readable scope and LearningGate

**Files:** create memory.py, extend contracts.py, create tests/test_control_plane_memory.py.

- [ ] **Step 1 RED:** Add test_scope_disjoint_is_not_hard_rejected, test_partial_scope_applies_only_to_intersection, test_exact_execution_spec_is_hard_rejected, test_semantic_similarity_warns_only, test_invalid_or_tainted_claim_is_excluded, and test_universal_rejection_is_not_manual. Run:

  python -m unittest tests.test_control_plane_memory -v

  Expected: FAIL because no new Memory implementation exists.

- [ ] **Step 2 GREEN:** Implement ClaimScope validation and deterministic set/range intersection. Return a structured verdict with enforcement level, matched claim ids, warning codes, and reopen requirements. Treat universal_factor_rejection as a derived value; reject hand-authored true.

- [ ] **Step 3 GREEN verification:** Run targeted tests with regime/time/universe/liquidity/usage fixtures. Expected: a failed result in one regime does not block a disjoint regime.

- [ ] **Step 4 REFACTOR:** Keep old RegistryGate untouched for legacy callers; the new path calls LearningGate explicitly and records the legacy warning when old data is consulted.

- [ ] **Step 5 Evidence/rollback:** Save verdict fixtures and hashes; rollback removes only the new test fixtures.

## Task 2: Bounded Memory projection and context

**Files:** memory.py, tests/test_control_plane_memory.py.

- [ ] **Step 1 RED:** Add test_projection_contains_only_safe_fields, test_projection_excludes_raw_logs, test_context_budget_is_enforced, test_context_overflow_has_explicit_status, and test_parent_invalidation_propagates. Expected FAIL.

- [ ] **Step 2 GREEN:** Build a compact projection containing claim id/type, scoped conclusion, audit grade, evidence refs, invalidation/reopen codes, parent ids, and directional factor status. Sort by scope relevance and information gain, then compress deterministically to the requested token budget.

- [ ] **Step 3 GREEN verification:** Run targeted tests with a large synthetic Ledger. Expected: raw reports, test-derived metrics and tainted text never enter the full Prompt.

- [ ] **Step 4 REFACTOR:** Keep projection reads side-effect free and bounded; do not scan arbitrary recent files.

- [ ] **Step 5 Evidence/rollback:** Record context JSON and estimated token count. Rollback only the projection fixture.

## Task 3: Campaign and Cycle state machine

**Files:** campaign.py, tests/test_control_plane_campaign.py.

- [ ] **Step 1 RED:** Add test_campaign_rejects_illegal_transition, test_cycle_requires_frozen_manifest, test_phase_implementation_does_not_authorize_campaign, test_terminal_campaign_cannot_resume, and test_duplicate_cycle_start_is_idempotent. Expected FAIL.

- [ ] **Step 2 GREEN:** Implement explicit transitions DRAFT→FROZEN→AUTHORIZED→RUNNING→PAUSED/COMPLETED/BLOCKED→CLOSED. Persist revisions and idempotency keys in the control-plane SQLite store. start_cycle requires a consumed test authorization and a frozen generation/roster/config hash.

- [ ] **Step 3 GREEN verification:** Run targeted tests twice with the same cycle id. Expected: one cycle event and no duplicate start.

- [ ] **Step 4 REFACTOR:** Do not infer state from directory age, latest file name or Runner status.json.

- [ ] **Step 5 Evidence/rollback:** Save transition events and state hash. Rollback is an explicit state rebuild, not a manifest overwrite.

## Task 4: Roster, cumulative budget and cycle leases

**Files:** campaign.py, tests/test_control_plane_campaign.py.

- [ ] **Step 1 RED:** Add test_roster_drift_blocks_cycle, test_required_member_failure_blocks_without_shrink, test_budget_reservation_is_atomic, test_cross_round_budget_cap, test_unknown_usage_is_not_zero, test_cycle_lock_is_cycle_scoped, test_stale_lease_fence_rejected, and test_resume_token_prevents_duplicate_cycle. Expected FAIL.

- [ ] **Step 2 GREEN:** Freeze a canonical roster manifest; implement budget reserve/settle in one SQLite transaction; implement cycle-scoped lock, PID/start-time/heartbeat lease and fencing token. Stale lease cleanup must require a verified process identity, never only file age.

- [ ] **Step 3 GREEN verification:** Run two concurrent temporary controllers. Expected: reservations never exceed the campaign cap, unrelated cycles can proceed, and an old fencing token cannot write.

- [ ] **Step 4 REFACTOR:** Keep token accounting separate from authorization nonces. Record provider-reported, estimated or unknown usage exactly as received.

- [ ] **Step 5 Evidence/rollback:** Save budget/lease event rows and concurrency output. Rollback only temporary databases.

## Task 5: Dry-run overlay and legacy runner adapter

**Files:** campaign.py, autonomous_runner.py, run_research_cycle.py, automation_controller.py, tests/test_control_plane_campaign.py, tests/test_automation_resilience.py.

- [ ] **Step 1 RED:** Add test_dry_run_writes_only_sandbox, test_dry_run_runs_duplicate_precheck, test_dry_run_does_not_commit_ledger, test_legacy_runner_result_is_raw_only, and test_next_cycle_consumes_safe_projection_not_raw_candidate_pool. Expected FAIL.

- [ ] **Step 2 GREEN:** Implement a dry-run overlay with a separate output namespace and budget. Run the same protocol/scope/duplicate/conflict prechecks, but never write formal Learning Packet/Ledger/Registry. Adapt old Runner outputs into raw evidence with legacy_unaudited status.

- [ ] **Step 3 GREEN verification:** Run the targeted campaign/resilience tests. Expected: repeated previews do not create formal duplicate facts, and raw candidate summaries do not become trusted memory.

- [ ] **Step 4 REFACTOR:** Keep old CLI flags compatible; make the new controller the only path for V3.4 campaign state.

- [ ] **Step 5 Evidence/rollback:** Save dry-run manifest and output tree. Rollback deletes only the sandbox.

## Task 6: P5/P6 gate report

**Files:** create docs/superpowers/plans/2026-07-25-v34-p5-p6-gate-report.md.

- [ ] **Step 1:** Run all memory/campaign tests plus existing automation and Registry/Memory tests.
- [ ] **Step 2:** Verify scoped PARTIAL behavior, taint/audit filtering, deterministic context budget, roster freeze, cumulative budget, cycle fencing, dry-run isolation and legacy quarantine.
- [ ] **Step 3:** Confirm no real Campaign starts from implementation commands and no production memory writes occur.
- [ ] **Step 4:** Write gate_status=PASS|FAIL; only PASS permits P7.

## Task exit criteria

P5/P6 is complete only when the next-round context is safe and scoped, Campaign state is explicit and resumable, budgets are cumulative and atomic, unrelated cycles are not unnecessarily serialized, dry-run previews are isolated, and no real two-round run has been started.
