# V3.4.1 Entry Contracts and Protocol Identity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use Superpowers `subagent-driven-development` and `test-driven-development`. Do not modify strategy or production signal logic.

**Goal:** Close every executable entry and create deterministic phase/protocol identities before any data or learning feature can run.

**Architecture:** Add a small `research_automation.control_plane` contract layer. It is fail-closed, side-effect free until a single-use phase token is consumed, and stores only control-plane metadata under `research_state/control_plane/`. Existing `Experiment` remains a legacy adapter.

**Tech Stack:** Python 3.13 standard library plus existing PyYAML; `unittest`; no new dependency.

---

## Files and responsibilities

### Create

- `research_automation/control_plane/__init__.py` — export the public contract types only.
- `research_automation/control_plane/contracts.py` — enums, dataclasses, canonical serialization, SHA-256 identities, actor and scope validation.
- `research_automation/control_plane/entry_guard.py` — entry inventory, baseline delta, phase authorization and side-effect guard.
- `tests/test_control_plane_contracts.py` — deterministic contract and hash tests.
- `tests/test_control_plane_entry_guard.py` — inventory, authorization and filesystem-delta tests.

### Modify

- `research_automation/discovery_execution_bridge.py:26-38,267-349` — add approved/executed protocol and runner identity fields without changing runner selection rules.
- `research_automation/kbase_ag2_full_cycle.py:141-228,232-458` — validate enumerated transitions and require a controller token for execution.
- `research_automation/autonomous_runner.py:294-331` — fail closed for KBase writeback and route through the guard.
- `run_research.py:121-187,338-482` — expose read-only inventory/plan commands and reject unauthorized full-cycle side effects.
- `run_research_cycle.py:30-78` — require the control-plane entry seam and keep legacy mode exploratory.
- `research_automation/kb_gate.py:52-90` — automatic execution/commit failures become fail-closed; proposal-only callers may still receive a warning.
- `research_automation/safety.py:16-63` — allow only `research_state/control_plane/` in addition to existing staging roots.
- `tests/test_kbase_ag2_full_cycle.py`, `tests/test_discovery_handoff_bridge.py`, `tests/test_automation_resilience.py` — preserve existing behavior and add bypass assertions.

## Shared contract definitions

`contracts.py` must expose:

```python
class Phase(str, Enum):
    P0 = "P0"; P1 = "P1"; P2 = "P2"; P3 = "P3"; P4 = "P4"
    P5 = "P5"; P6 = "P6"; P7 = "P7"; P8 = "P8"

class SideEffect(str, Enum):
    READ = "READ"; WRITE_STAGING = "WRITE_STAGING"
    WRITE_CONTROL_PLANE = "WRITE_CONTROL_PLANE"
    RUN_RESEARCH = "RUN_RESEARCH"; WRITE_KBASE = "WRITE_KBASE"
    OPEN_HOLDOUT = "OPEN_HOLDOUT"; GIT_MUTATION = "GIT_MUTATION"

@dataclass(frozen=True)
class Actor:
    actor_id: str
    actor_type: str          # human|automation|llm|scheduler|legacy_runner
    invocation_id: str

@dataclass(frozen=True)
class IdentityBundle:
    research_identity_id: str
    execution_spec_id: str
    run_id: str
    learning_id: str | None
    campaign_id: str | None
    cycle_id: str | None

@dataclass(frozen=True)
class ArtifactFingerprint:
    relative_path: str
    kind: str
    size: int
    mtime_ns: int
    sha256: str | None
    hash_state: str  # CONTENT_HASHED|LAZY|MISSING

@dataclass(frozen=True)
class GenerationManifest:
    generation_id: str
    generation_nonce: str
    parent_generation_id: str | None
    created_at: str
    data_cutoff: str
    raw_csv_root: str
    raw_parquet_root: str
    indicator_cache_root: str
    signal_cache_root: str
    trading_calendar_id: str
    point_in_time_universe_id: str
    adjustment_scheme_id: str
    semantic_health_report: str
    missing_reason_policy: str
    artifacts: tuple[ArtifactFingerprint, ...]
    manifest_sha256: str

def canonical_json(value: object) -> str: ...
def canonical_sha256(value: object) -> str: ...
def identity_id(namespace: str, value: object) -> str: ...
```

Canonical rules are UTF-8 JSON, sorted keys, normalized forward-slash paths, explicit UTC timestamps, preserved numeric values, and rejection of `NaN`, `Infinity`, sets, bytes, and non-finite floats. Dynamic timestamps and token counters are excluded from plan/scope hashes.

## Task 1: Freeze contracts and canonical identity

**Files:** create `contracts.py`, `__init__.py`, `tests/test_control_plane_contracts.py`.

- [ ] **Step 1 RED:** Add tests `test_canonical_hash_ignores_mapping_order`, `test_canonical_hash_normalizes_windows_path`, `test_canonical_hash_rejects_non_finite_float`, `test_identity_bundle_requires_nonempty_ids`, and `test_scope_predicate_requires_all_declared_dimensions`. Run:

  `python -m unittest tests.test_control_plane_contracts -v`

  Expected: FAIL because the module and functions do not exist.

- [ ] **Step 2 GREEN:** Implement the enums/dataclasses and canonical functions exactly as listed above. Use `json.dumps(..., sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)`. Normalize only path fields; do not round scientific values.

- [ ] **Step 3 GREEN verification:** Run the targeted test command. Expected: all contract tests PASS and no warning output.

- [ ] **Step 4 REFACTOR:** Keep validation in pure functions; no filesystem, network, LLM or Git calls. Re-run the targeted command.

- [ ] **Step 5 Evidence/rollback:** Save the test output and SHA-256 of `contracts.py` in the task report. Rollback is deleting only the newly created control-plane files if the gate fails; no existing file is reverted.

## Task 2: Build the executable/import/scheduler inventory

**Files:** create `entry_guard.py`, extend `tests/test_control_plane_entry_guard.py`; read but do not rewrite every root `*.py`, `*.bat`, `*.ps1`, `*.sh`, `apps/`, `tools/`, and the two research runners.

`EntryInventory.scan(root)` must return deterministic records:

```python
EntryRecord(
    entry_id, path, kind, callable_name, actor_type,
    declared_side_effects: tuple[SideEffect, ...],
    declared_phase: Phase | None,
)
```

The scanner must inspect only bounded code/metadata paths, never recurse into `data/` or raw research outputs. Scheduler inventory is an explicit manifest entry with `source="external_scheduler_inventory"` when Windows task metadata is unavailable.

- [ ] **Step 1 RED:** Add `test_inventory_contains_required_root_entrypoints`, `test_inventory_rejects_undeclared_entry`, `test_inventory_never_scans_data_root`, and `test_scheduler_unknown_is_explicit_not_silent`. Run the targeted test command; expected FAIL.

- [ ] **Step 2 GREEN:** Implement `EntryInventory.scan`, `load_manifest`, `assert_declared`, and `write_manifest` under `research_state/control_plane/entry_inventory.json`. A missing manifest or undeclared entry raises `EntryNotDeclaredError`.

- [ ] **Step 3 GREEN verification:** Run the targeted tests and inspect the generated fixture manifest. Expected: root `run_research.py`, `run_research_cycle.py`, `daily_run.py`, `main.py`, all root batch files, `AutonomousRunnerV1.run`, `run_kbase_ag2_full_cycle`, and `execute_plan` are represented.

- [ ] **Step 4 REFACTOR:** Make path ordering and JSON serialization deterministic; add no scheduler dependency.

- [ ] **Step 5 Evidence/rollback:** Record inventory count and manifest hash. Rollback removes only the generated fixture/manifest, never project entrypoints.

## Task 3: Implement phase and dangerous-action authorization

**Files:** `entry_guard.py`, `tests/test_control_plane_entry_guard.py`.

`PhaseAuthorizer` must enforce `P0→P1→...→P8`, require a PASS gate for the previous phase, consume each token once, and never auto-advance. `issue_action_token` supports only `CAMPAIGN` and `FINAL_EVAL`; Final Eval tokens include `holdout_id` and nonce and become permanently consumed on the first attempt, including failure/timeout.

- [ ] **Step 1 RED:** Add `test_phase_authorizer_rejects_phase_skip`, `test_phase_authorizer_requires_previous_gate`, `test_phase_token_replay_is_rejected`, `test_p6_does_not_authorize_campaign`, `test_final_eval_nonce_is_permanently_consumed_after_failure`, and `test_unknown_side_effect_is_denied`. Expected FAIL.

- [ ] **Step 2 GREEN:** Store authorization state in a small SQLite database at `research_state/control_plane/control_plane.sqlite3`; use a transaction with a unique token id/nonce. Expose `consume_phase_token`, `consume_campaign_token`, and `consume_final_eval_token`.

- [ ] **Step 3 GREEN verification:** Run targeted tests twice in separate temporary directories to prove idempotency and crash-safe replay behavior.

- [ ] **Step 4 REFACTOR:** Keep `PhaseAuthorizer` independent from CLI parsing and LLM clients. Do not use file age as a lock or authorization signal.

- [ ] **Step 5 Evidence/rollback:** Store schema version and test output. Rollback is removing only a temporary SQLite database; do not delete an existing control-plane database.

## Task 4: Compile and freeze the execution protocol

**Files:** `contracts.py`, `discovery_execution_bridge.py`, `tests/test_discovery_handoff_bridge.py`, `tests/test_control_plane_contracts.py`.

Extend `DiscoveryExecutionPlan` with:

```python
execution_spec_id: str
approved_protocol_hash: str | None
executed_protocol_hash: str
code_identity: dict[str, str]
input_artifact_refs: list[dict[str, str]]
dataset_roles: dict[str, dict[str, str]]
label_definition_id: str
conformance: str  # IDENTICAL|IMMATERIAL_ALLOWLISTED|APPROVED_AMENDMENT|MATERIAL_UNAPPROVED
```

`build_execution_plan` must reject ambiguous label/horizon/fold choices and classify runner/label/gate changes as `MATERIAL_UNAPPROVED` unless an approved amendment is present. It must not infer approval from a self-created preregistration hash.

- [ ] **Step 1 RED:** Add `test_execution_plan_records_code_and_input_identity`, `test_protocol_label_change_is_material_unapproved`, and `test_ambiguous_fold_role_is_rejected`. Expected FAIL.

- [ ] **Step 2 GREEN:** Implement deterministic protocol compilation around the existing handoff document and selected runner. Preserve existing factor routing and command construction.

- [ ] **Step 3 GREEN verification:** Run the bridge test module. Expected: existing runner-selection tests remain green; new fields are present and stable.

- [ ] **Step 4 REFACTOR:** Keep the old dataclass constructor backward compatible in tests by providing explicit defaults only for legacy fixture paths; production execution requires all identity fields.

- [ ] **Step 5 Evidence/rollback:** Save the execution-plan JSON fixture and hash. Rollback only the bridge/contract changes from this task.

## Task 5: Close legacy write seams and CLI bypasses

**Files:** `kbase_ag2_full_cycle.py`, `autonomous_runner.py`, `run_research.py`, `run_research_cycle.py`, `kb_gate.py`, `safety.py`, existing test files.

- [ ] **Step 1 RED:** Add `test_kbase_writeback_default_is_denied`, `test_legacy_runner_without_ticket_is_blocked`, `test_full_cycle_status_value_must_be_enum`, `test_runner_boolean_cannot_set_scientific_outcome`, and `test_cli_full_cycle_requires_phase_token`. Run the targeted modules; expected FAIL.

- [ ] **Step 2 GREEN:** Wrap side effects with `EntryGuard.assert_side_effect`. Change `KBASE_WRITEBACK` default from enabled to disabled/fail-closed for automatic runs. Keep an explicit proposal-only warning path, but automatic execution and Commit fail closed when KB validation is unavailable. Replace direct final-status trust with an adapter call that requires evidence references.

- [ ] **Step 3 GREEN verification:** Run `python -m unittest tests.test_kbase_ag2_full_cycle tests.test_automation_resilience tests.test_discovery_handoff_bridge -v`. Existing dry-run and production-boundary tests must remain green.

- [ ] **Step 4 REFACTOR:** Keep legacy runner output readable as raw evidence, never as a trusted Learning Packet. Do not alter strategy parameters or production files.

- [ ] **Step 5 Evidence/rollback:** Capture changed-file list and test output. Rollback is a controlled revert of only the allowlisted control-plane seams after user authorization; no `git reset`/checkout.

## Task 6: P0/P1 gate report

**Files:** create `docs/superpowers/plans/2026-07-25-v34-p0-p1-gate-report.md`; no production changes.

- [ ] **Step 1:** Run all P0/P1 targeted tests and the full existing automation/bridge test modules.
- [ ] **Step 2:** Record inventory hash, policy-source status, protocol hash fixtures, failed tests, passing tests, bypass results, unresolved risks, and `gate_status=PASS|FAIL`.
- [ ] **Step 3:** Confirm no files outside the P0/P1 allowlist changed relative to the baseline manifest.
- [ ] **Step 4:** Only a PASS report permits the next exact command `START_IMPLEMENTATION phase=P2`; there is no automatic transition.

## Task exit criteria

P0/P1 is complete only when every entrypoint is inventoried, unauthorized side effects fail closed, protocol identity is deterministic, old Runner booleans cannot decide outcomes, all targeted and regression tests pass, and the gate report is PASS. No real research or Git mutation occurs in this plan.
