# V3.4.1 Long-Run Operations and Trusted Evaluator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use Superpowers subagent-driven-development and test-driven-development. This plan implements gates and telemetry only; it never consumes a real market Final Holdout.

**Goal:** Make long Campaign operation observable and recoverable, then implement a separately authorized Trusted Evaluator that consumes a protected Final Holdout exactly once and closes the iteration state.

**Architecture:** Operational counters and actor events extend CampaignController's control-plane store. TrustedEvaluator has a separate low-privilege interface and path policy; research runners cannot read or reconstruct the protected holdout. Final Eval is an irreversible state transition to CLOSED.

**Tech Stack:** Python standard library, existing Windows process/file APIs, unittest; no new dependency and no automatic production promotion.

---

## Files and responsibilities

### Create

- research_automation/control_plane/evaluator.py — Trusted Evaluator capability boundary, one-time Final Eval gate, live-forward result and CLOSED transition.
- tests/test_control_plane_evaluator.py — authorization, isolation, replay and closure tests.

### Modify

- research_automation/control_plane/campaign.py — operational counters, heartbeat/recovery, cumulative budget reporting and actor audit events.
- research_automation/control_plane/entry_guard.py — final path/capability checks and no-research-runner access rule.
- run_research.py and run_research_cycle.py — expose status/audit-only commands; no command opens Final Holdout implicitly.
- research_automation/kbase_ag2_full_cycle.py — include actor/invocation and operational evidence refs in terminal reports.
- tests/test_control_plane_campaign.py, tests/test_kbase_ag2_full_cycle.py — regression coverage.

## Operational contract

CampaignController must record provider-reported/estimated/unknown tokens, wall time, retries, data-window exposure, disk reserve, round count, roster status, actor, invocation and result for every cycle. It must report budget exhaustion or information-gain failure as a boundary that prevents the next cycle without terminating the current process.

Heartbeat leases contain PID, process-start-time, host identity, monotonic heartbeat sequence and fencing token. A stale lease may be reaped only after process identity is disproven. A short projection lock is independent from cycle locks.

## Task 1: Operational telemetry and recovery

**Files:** campaign.py, tests/test_control_plane_campaign.py.

- [ ] **Step 1 RED:** Add test_operational_event_records_unknown_usage, test_budget_exhaustion_blocks_next_cycle_only, test_current_cycle_is_not_killed_by_budget_gate, test_lease_heartbeat_has_fencing_token, test_process_identity_prevents_false_reap, test_resume_replays_without_duplicate_commit, and test_actor_audit_covers_manual_resolution. Run:

  python -m unittest tests.test_control_plane_campaign -v

  Expected: FAIL for new behavior.

- [ ] **Step 2 GREEN:** Implement an append-only operational event record and status query. On budget or disk threshold failure, transition Campaign to BLOCKED/PAUSED for the next start, emit an actor event, and return control to the current process. Resume uses the last durable idempotency key and replays only missing projection events.

- [ ] **Step 3 GREEN verification:** Run targeted tests with fake clocks, fake PIDs and temporary SQLite stores. Expected: no active subprocess is terminated and no event is double counted.

- [ ] **Step 4 REFACTOR:** Keep telemetry aggregation O(number of events), not a full rewrite of historical JSON per round. Do not place full logs or data rows in the hot path.

- [ ] **Step 5 Evidence/rollback:** Save operational event samples and projection hash. Rollback is rebuilding the projection from the event store.

## Task 2: Trusted Evaluator capability boundary

**Files:** create evaluator.py, tests/test_control_plane_evaluator.py.

Expose:

~~~python
@dataclass(frozen=True)
class FinalEvalRequest:
    campaign_id: str
    candidate_set_hash: str
    code_identity_hash: str
    execution_spec_hash: str
    holdout_id: str
    authorization_nonce: str
    actor: Actor

class TrustedEvaluator:
    def evaluate_once(self, request: FinalEvalRequest) -> dict: ...
~~~

- [ ] **Step 1 RED:** Add test_final_eval_requires_frozen_candidate_and_protocol, test_final_eval_requires_separate_authorization, test_runner_path_cannot_access_holdout_root, test_holdout_hash_mismatch_fails, test_final_eval_nonce_replay_fails, and test_failed_eval_remains_consumed. Expected FAIL.

- [ ] **Step 2 GREEN:** Validate CampaignState=CLOSED-eligible/FROZEN, candidate/code/model/threshold/protocol hashes, independent actor capability and the one-time HoldoutBroker marker before obtaining any data handle. Run the evaluator in a restricted data-root abstraction; never let the research Runner receive a raw path or reconstructable labels. Persist attempt before access.

- [ ] **Step 3 GREEN verification:** Run targeted tests with a fake protected root and a fake evaluator. Expected: all unauthorized/replay/path traversal cases fail closed.

- [ ] **Step 4 REFACTOR:** Keep the evaluator independent of AG2/LLM clients and production promotion code. Return a structured result plus evidence refs only.

- [ ] **Step 5 Evidence/rollback:** Save request/result hashes and consumed marker. Rollback is impossible for a consumed Final Eval; test fixtures only may be deleted.

## Task 3: Final state closure and live-forward boundary

**Files:** evaluator.py, campaign.py, run_research.py, tests/test_control_plane_evaluator.py.

- [ ] **Step 1 RED:** Add test_final_eval_transitions_to_consumed_then_closed, test_closed_campaign_cannot_start_new_cycle, test_live_forward_is_separate_from_research_memory, and test_production_promotion_remains_manual. Expected FAIL.

- [ ] **Step 2 GREEN:** On a successful or failed Final Eval, append FINAL_EVAL_CONSUMED and transition to CLOSED. Reject all subsequent automatic iteration requests. Store live-forward observations as a new, unpromoted run/cycle; never mutate production strategy or infer promotion from evaluator output.

- [ ] **Step 3 GREEN verification:** Run evaluator and campaign tests twice with the same request. Expected: one terminal transition and replay rejection.

- [ ] **Step 4 REFACTOR:** Make terminal state checks explicit and independent of directory names or latest-file selection.

- [ ] **Step 5 Evidence/rollback:** Save terminal transition journal and result hash. No rollback path exists for real consumed state; document that in the gate report.

## Task 4: Audit/status CLI and report

**Files:** run_research.py, run_research_cycle.py, evaluator.py, create tests/test_control_plane_evaluator.py fixtures.

- [ ] **Step 1 RED:** Add test_status_command_is_read_only, test_audit_export_contains_actor_and_hash_refs, test_status_command_does_not_open_holdout, and test_unknown_token_usage_is_reported_unknown. Expected FAIL.

- [ ] **Step 2 GREEN:** Add read-only status/audit commands that display campaign state, budget, lease, roster hash, generation id, evidence grade, access counts, and token-status category. They must not start a run, mutate state, or reveal protected data.

- [ ] **Step 3 GREEN verification:** Run the CLI tests with a temporary control-plane root and inspect exit codes/output. Expected: no files outside the control-plane root change.

- [ ] **Step 4 REFACTOR:** Keep reports compact and reference artifacts by hash/path; never embed raw logs, labels or tainted content.

- [ ] **Step 5 Evidence/rollback:** Save command output and filesystem diff. Rollback removes only test fixtures.

## Task 5: P7/P8 gate report

**Files:** create docs/superpowers/plans/2026-07-25-v34-p7-p8-gate-report.md.

- [ ] **Step 1:** Run campaign/evaluator/status tests plus all prior control-plane tests.
- [ ] **Step 2:** Verify cumulative budget, lease fencing, crash resume, actor audit, one-time Final Eval, protected path isolation, CLOSED terminal state and manual-only production promotion.
- [ ] **Step 3:** Confirm no Final Holdout was read during implementation and no active process was stopped.
- [ ] **Step 4:** Write gate_status=PASS|FAIL. A PASS still does not authorize real Final Eval; that requires the separate exact user command.

## Task exit criteria

P7/P8 is complete only when long runs can pause/resume without double commits or silent fallback, operational usage is auditable, Final Eval is independently authorized and one-time, and its terminal CLOSED state cannot return to automatic iteration.
