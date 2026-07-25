# V3.4.1 Access, Evidence and Learning Commit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use Superpowers subagent-driven-development and test-driven-development. This plan is control-plane-only and never opens market Final Holdout.

**Goal:** Record every data/outcome access, propagate taint through derived artifacts, independently evaluate Runner evidence, and commit only valid, content-addressed learning packets to a rebuildable Ledger.

**Architecture:** AccessStore and TaintGraph persist append-only events in the existing control-plane SQLite store. EvidenceAdapter reads raw Runner artifacts plus frozen ExecutionSpec/GenerationPin and computes its own verdict. LearningCommitService writes create-only packet files and a journal event; the Ledger is a rebuildable projection.

**Tech Stack:** Python standard library (sqlite3, json, hashlib, pathlib, dataclasses), existing YAML/JSON readers, unittest; no new dependency.

---

## Files and responsibilities

### Create

- research_automation/control_plane/access.py — event schema/store, taint propagation, one-time Holdout broker.
- research_automation/control_plane/evidence_learning.py — Evidence Adapter, semantic checks, historical audit addendum, Learning Packet/Commit/Ledger.
- tests/test_control_plane_access.py — event, taint, holdout and crash tests.
- tests/test_control_plane_evidence_learning.py — evidence, packet, journal and rebuild tests.

### Modify

- research_automation/control_plane/contracts.py — shared event/taint/evidence enums and immutable references.
- research_automation/discovery_execution_bridge.py:26-38,267-349 — consume the frozen ExecutionSpec fields from Plan 01.
- research_automation/kbase_ag2_full_cycle.py:389-458 — call EvidenceAdapter after raw execution and before any terminal scientific status.
- research_automation/autonomous_runner.py:272-331 — emit access/taint events and expose raw artifacts only through the adapter seam.
- research_automation/kb_gate.py — automatic Commit/Evidence operations fail closed on unavailable validation.
- tests/test_kbase_ag2_full_cycle.py, tests/test_discovery_handoff_bridge.py — update fixtures with explicit legacy/evidence status.

## Shared event and taint contracts

access.py exposes:

~~~python
class AccessType(str, Enum):
    READ = "READ"; MATERIALIZE = "MATERIALIZE"; DERIVE = "DERIVE"
    DISPLAY = "DISPLAY"; CONSUME = "CONSUME"; EXPORT = "EXPORT"

class Taint(str, Enum):
    CLEAN = "CLEAN"; TEST_LABEL = "TEST_LABEL"
    TEST_DERIVED = "TEST_DERIVED"; HOLDOUT = "HOLDOUT"
    INVALID = "INVALID"

@dataclass(frozen=True)
class AccessEvent:
    event_id: str
    event_type: AccessType
    actor_id: str
    actor_type: str
    invocation_id: str
    run_id: str
    generation_id: str | None
    data_role: str
    fields: tuple[str, ...]
    date_start: str | None
    date_end: str | None
    input_artifact_refs: tuple[str, ...]
    output_artifact_refs: tuple[str, ...]
    taint_in: tuple[Taint, ...]
    taint_out: tuple[Taint, ...]
    sequence: int

class AccessStore:
    def append(self, event: AccessEvent) -> int: ...
    def list_for_run(self, run_id: str) -> list[AccessEvent]: ...
    def replay(self) -> list[AccessEvent]: ...

class TaintGraph:
    def derive(self, inputs: list[str], output: str, event_id: str) -> tuple[Taint, ...]: ...
    def taint_for(self, artifact_ref: str) -> tuple[Taint, ...]: ...

class HoldoutBroker:
    def attempt_once(self, holdout_id: str, actor: Actor, action_token: str) -> str: ...
~~~

The Holdout broker persists an attempt/consumed marker before returning a path/handle. There is no reusable release; failure, timeout and process death leave the marker consumed. query_holdout.yaml remains a KBase retrieval holdout and is never treated as market Final Holdout.

## Task 1: Durable AccessEvent store

**Files:** create access.py, extend contracts.py, create tests/test_control_plane_access.py.

- [ ] **Step 1 RED:** Add test_access_event_requires_actor_and_run, test_access_event_sequence_is_monotonic, test_access_event_append_is_durable, test_access_replay_after_reopen, and test_invalid_event_type_fails_closed. Run:

  python -m unittest tests.test_control_plane_access -v

  Expected: FAIL because the module/types do not exist.

- [ ] **Step 2 GREEN:** Create SQLite tables access_events, taint_edges, and holdout_attempts with unique event_id, monotonic sequence, actor fields, and JSON columns for refs/fields/taint. Use one transaction per append and reject duplicate sequence/event ids.

- [ ] **Step 3 GREEN verification:** Run targeted tests, close/reopen the store, and replay events. Expected: exact event order and no duplicate rows.

- [ ] **Step 4 REFACTOR:** Make serialization canonical and bounded; never store raw full data frames or raw logs in the event table.

- [ ] **Step 5 Evidence/rollback:** Save a temporary database dump and event count. Rollback deletes only the temporary database.

## Task 2: Taint propagation and one-time Holdout broker

**Files:** access.py, tests/test_control_plane_access.py.

- [ ] **Step 1 RED:** Add test_test_label_taint_propagates_to_rankic, test_display_and_consume_preserve_taint, test_invalid_taint_blocks_consume, test_holdout_second_attempt_rejected, test_holdout_failure_still_consumed, and test_holdout_wrong_action_token_rejected. Expected FAIL.

- [ ] **Step 2 GREEN:** Implement TaintGraph.derive as the union of input taints plus the transformation rule: outcome-derived outputs inherit TEST_DERIVED; any Holdout input inherits HOLDOUT; invalid inputs inherit INVALID. Implement HoldoutBroker.attempt_once with a transaction that inserts the attempt before returning a broker handle; duplicate holdout_id returns HoldoutAlreadyConsumed.

- [ ] **Step 3 GREEN verification:** Run targeted tests twice, including a subprocess that exits after the transaction but before handle return. Expected: the second attempt is rejected.

- [ ] **Step 4 REFACTOR:** Keep DISPLAY and CONSUME separate so LLM Prompt exposure is auditable. Do not use file-open hooks as the only access signal.

- [ ] **Step 5 Evidence/rollback:** Record taint lineage JSON and holdout attempt rows. Rollback only temporary fixtures; never clear a real consumed marker.

## Task 3: Deterministic Evidence Adapter

**Files:** create evidence_learning.py, tests/test_control_plane_evidence_learning.py.

EvidenceAdapter.evaluate returns:

~~~python
@dataclass(frozen=True)
class EvidenceResult:
    verdict: str  # VALID|NO_MATERIAL_FINDING|RESEARCH_ONLY|EVIDENCE_INVALID|MATERIAL_UNAPPROVED
    protocol_conformance: str
    audit_grade: str
    scientific_outcome: str
    promotion_eligible: bool
    evidence_refs: tuple[dict[str, str], ...]
    access_event_ids: tuple[str, ...]
    taint_refs: tuple[str, ...]
    invalidation_codes: tuple[str, ...]
~~~

- [ ] **Step 1 RED:** Add test_evidence_adapter_rejects_missing_status, test_runner_boolean_cannot_set_outcome, test_semantic_label_mismatch_is_invalid, test_test_derived_rankic_is_tainted_even_when_not_in_gate, test_clean_run_with_no_claim_is_no_material_finding, test_unknown_runner_schema_is_invalid, and test_material_unapproved_has_no_learning_packet. Expected FAIL.

- [ ] **Step 2 GREEN:** Parse known JSON/YAML/Parquet metadata deterministically; verify artifact hashes, schema, label definition/horizon/exit rule, date ranges/fold roles, runner/code identity, generation id, and access/taint references. Recompute gate inputs from train/validation-only artifacts. Treat missing/contradictory fields as EVIDENCE_INVALID; treat approved/executed material changes as MATERIAL_UNAPPROVED; treat a clean, complete run with no scoped claim as NO_MATERIAL_FINDING.

- [ ] **Step 3 GREEN verification:** Run targeted tests using tiny JSON/CSV/Parquet fixtures. Expected: a self-reported promotion_gate_passed=true never produces promotion_eligible=true.

- [ ] **Step 4 REFACTOR:** Keep parser and semantic checker pure; no network, LLM or real data reads. Use an explicit adapter version in every result.

- [ ] **Step 5 Evidence/rollback:** Save fixture hashes, verdict JSON and invalidation codes. Rollback removes only new test fixtures.

## Task 4: Historical P0B audit addendum

**Files:** evidence_learning.py, create tests/test_control_plane_historical_addendum.py, create tests/fixtures/control_plane/.

- [ ] **Step 1 RED:** Add test_addendum_does_not_modify_original, test_addendum_supersedes_false_unseen_claim, test_addendum_marks_test_derived_rankic, and test_addendum_quarantines_downstream_parent. Expected FAIL.

- [ ] **Step 2 GREEN:** Implement build_audit_addendum for the known 2026-07-23/24 artifacts. It records 0/9, cutoff 2026-07-08, TEST_LABELS_AND_TEST_DERIVED_RANKIC_MATERIALIZED_NOT_USED_FOR_PREFLIGHT_GATE, protocol_reconstruction=PARTIAL, source hashes, supersedes, and invalidated_fields. It writes only a new addendum under research_state/control_plane/audit_addenda/.

- [ ] **Step 3 GREEN verification:** Run the historical fixture tests and compare original file hashes before/after.

- [ ] **Step 4 REFACTOR:** Do not rewrite FINAL_CONCLUSION.md, do not call the result unseen/OOS, and do not allow the contaminated artifacts to become parent_learning_id.

- [ ] **Step 5 Evidence/rollback:** Record addendum hash and original hashes. Rollback is deleting only a test addendum, never historical source files.

## Task 5: Content-addressed Learning Packet and rebuildable Ledger

**Files:** evidence_learning.py, tests/test_control_plane_evidence_learning.py.

Expose:

~~~python
class LearningCommitService:
    def commit(self, evidence: EvidenceResult, claim: dict, actor: Actor) -> str: ...
    def rebuild_ledger(self) -> dict: ...

class LearningPacket:
    packet_hash: str
    schema_version: str
    identity: IdentityBundle
    claim: dict
    evidence_refs: tuple[dict[str, str], ...]
    access_event_refs: tuple[str, ...]
    taint_refs: tuple[str, ...]
    audit_grade: str
    enforcement_level: str
    invalidation_codes: tuple[str, ...]
~~~

- [ ] **Step 1 RED:** Add test_invalid_evidence_cannot_commit, test_no_material_finding_does_not_create_empty_packet, test_packet_is_content_addressed_and_create_only, test_duplicate_commit_is_idempotent, test_commit_journal_rebuilds_projection, test_tampered_packet_is_rejected, and test_packet_projection_never_contains_raw_log. Expected FAIL.

- [ ] **Step 2 GREEN:** Canonically serialize a packet to research_state/control_plane/learning_packets/<packet_hash>.json using exclusive create. Append a commit event in SQLite with a unique idempotency key, then rebuild the Ledger projection from events. A missing/invalid/tainted evidence result is audit-only and cannot commit; NO_MATERIAL_FINDING writes an audit/round outcome but never creates an empty Learning Packet.

- [ ] **Step 3 GREEN verification:** Run targeted tests, delete the projection, rebuild from the journal, and compare canonical projection hashes. Expected: no double count and stable packet hash.

- [ ] **Step 4 REFACTOR:** Keep packet storage create-only; make Ledger a projection, not a second writer. Use compact evidence refs, not full reports or raw Parquet.

- [ ] **Step 5 Evidence/rollback:** Save packet/journal/projection hashes. Rollback is an explicit projection rebuild; never overwrite a packet.

## Task 6: Integrate EvidenceAdapter into full-cycle

**Files:** kbase_ag2_full_cycle.py, autonomous_runner.py, discovery_execution_bridge.py, existing cycle tests.

- [ ] **Step 1 RED:** Add test_full_cycle_requires_evidence_adapter_verdict, test_incomplete_evidence_archives_raw_only, test_protocol_drift_never_commits_learning, and test_legacy_runner_output_is_quarantined. Expected FAIL.

- [ ] **Step 2 GREEN:** After process completion, invoke EvidenceAdapter.evaluate with the execution plan, pinned generation and access refs. Store raw status as evidence only. Set scientific/audit/promotion fields from the adapter result; never from status.json booleans. Call LearningCommitService only for a valid, untainted, conformance-approved result.

- [ ] **Step 3 GREEN verification:** Run python -m unittest tests.test_kbase_ag2_full_cycle tests.test_control_plane_evidence_learning -v. Existing dry-run and production-boundary assertions must remain PASS.

- [ ] **Step 4 REFACTOR:** Keep old runner files readable and archived; place invalid/quarantined outputs under an explicit control-plane quarantine path.

- [ ] **Step 5 Evidence/rollback:** Record cycle manifest diff and adapter verdict. Rollback only the allowlisted controller seam.

## Task 7: P3/P4 gate report

**Files:** create docs/superpowers/plans/2026-07-25-v34-p3-p4-gate-report.md.

- [ ] **Step 1:** Run access, taint, evidence, historical addendum, packet and full-cycle regression tests.
- [ ] **Step 2:** Verify one-time Holdout rejection, semantic evidence checking, no false unseen claim, create-only packets and ledger rebuild.
- [ ] **Step 3:** Compare access/taint event refs against every evidence result and record unknown/invalid artifacts.
- [ ] **Step 4:** Write gate_status=PASS|FAIL; only PASS permits P5.

## Task exit criteria

P3/P4 is complete only when every protected read/derive/display/consume is traceable, Holdout access is one-time and crash-safe, evidence is independently semantically checked, invalid results cannot commit, and a deleted Ledger can be rebuilt without double counting.
