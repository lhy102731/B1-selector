# P0 Change Request 001 — Entry and Side-Effect Closure

```yaml
change_request_id: P0-CR-001
status: PENDING_USER_APPROVAL
phase: P0
supersedes_authorization: START_IMPLEMENTATION phase=P0
current_plan_version: V3.4.1-FINAL
current_plan_hash: a4dd6d2e7e7a02838dcf1d656974a2ae4f43f453860b3a520280ce29c1bae2c6
current_scope_hash: e7d481aa20036b66e035944e398bdd888a9266b9d6000b1ecd193c6c7194b7df
implementation_status: DONE_WITH_CONCERNS
```

## Reason

The authorized P0 inventory proved that the approved P0/P1 plan does not cover every executable entry or the deepest side-effect sinks. Continuing against the old allowlist would either leave bypasses open or violate the approved plan and scope hashes.

The change is mandatory for a truthful P0 gate. It does not authorize P1, a backtest, a research run, a Campaign, a data update, a cache rebuild, a KBase write, a Git mutation, or a Windows Task Scheduler/ACL mutation.

## Affected tasks

- Split the combined `P0/P1` gate into independent P0 and P1 gates. P0 must stop and wait for `START_IMPLEMENTATION phase=P1` after its own PASS report.
- Revise Task 2 so inventory covers all executable and import-active code surfaces, while excluding raw data, generated output, dependency, worktree, temporary, and archive trees by explicit rule rather than by an incomplete root list.
- Revise Task 3 from `PhaseGrant -> SideEffect` to `PhaseGrant -> TaskTicket -> SideEffect`, binding each dangerous action to an approved entry, actor, resource scope, plan/scope/policy hashes, and idempotency key.
- Remove public-hash bootstrap authorization. Plan/scope/policy hashes identify approved content; they are not credentials. A PASS gate satisfies only the next phase's prerequisite and must never mint the next phase's authorization.
- Revise Task 5 so guards are placed at the deepest shared execution/write sinks, not only at CLI wrappers.
- Add an independent canonical `gate_report.json` for P0. A Markdown report may accompany it but cannot be the authority.
- Leave Protocol Compiler, IdentityBundle, GenerationManifest, and execution-protocol conformance in P1.

## Evidence that triggered the request

The originally bounded scan found 39 root/`apps`/`tools` files plus three named import seams and one Windows scheduled task. Deeper inspection then found bypasses outside the approved modification list:

- `execute_plan()` accepts a caller-constructed arbitrary command.
- repair code can invoke an LLM, write patches, and run `git apply`.
- `ClaudePatchExecutor` can modify a production file before a later audit-path check fails.
- `RealBacktestExecutor.execute()` is a public subprocess and shared-output mutation sink.
- Registry, Snapshot, Handoff, KBase, and orchestration write paths can be called by import rather than through a protected CLI.
- `apps/web_roundtable.py` can start LLM threads and write discussions without using the shared orchestrator.
- `apps/web_server.py` can replace production strategy configuration through an unauthenticated endpoint.
- `run_select.bat` is referenced by the enabled Windows task `\\A股选股`; the task runs as Administrator's interactive token while the workspace code chain is modifiable by broader local groups.
- The current `PhaseAuthorizer` lets any caller choose a new SQLite path, supply public hashes, construct an `Actor`, and mint a phase token. It also lets automation mint P1 immediately after a P0 PASS and retry P0 after FAIL without a fresh user authorization.
- The current gate writer accepts an arbitrary report hash, does not validate canonical gate semantics/evidence, and performs part of its latest-attempt check outside the write transaction.
- Entry declaration comparison omits `declared_side_effects` and `declared_phase`, so declaration metadata can be substituted while retaining the same entry ID/path.

Inventory/quarantine is sufficient for legacy scientific artifacts only when their results are mechanically ineligible for Learning, Claim, Memory, Registry, KBase, and Promotion. Inventory labels alone are not sufficient for active web endpoints or deepest write/execute sinks.

## Files added or changed if approved

### Plan and control-plane state

- `docs/superpowers/plans/2026-07-25-v34-p0-revision-001.md` (new immutable P0 revision)
- `docs/superpowers/plans/2026-07-25-v34-plan-approval-manifest-p0r1.json` (new)
- `research_state/control_plane/p0/` (authorization receipt, inventory, task reports, test evidence, and gate report only)

### Existing P0 contract and guard files

- `research_automation/control_plane/__init__.py`
- `research_automation/control_plane/contracts.py`
- `research_automation/control_plane/entry_guard.py`
- `research_automation/safety.py`
- `research_automation/kb_gate.py`

### Deep shared side-effect sinks and entry adapters

- `research_automation/autonomous_runner.py`
- `research_automation/kbase_ag2_full_cycle.py`
- `research_automation/discovery_execution_bridge.py`
- `research_automation/handoff_runner_repair.py`
- `research_automation/automation_controller.py`
- `research_automation/experiment_runner.py`
- `research_automation/patch_executor.py`
- `research_automation/registry_updater.py`
- `research_automation/snapshot_updater.py`
- `research_automation/handoff_updater.py`
- `ag2_research/knowledge_bridge.py`
- `ag2_research/orchestrator.py`
- `run_research.py`
- `run_research_cycle.py`
- `research_automation/run_brick_sqnav_backlog.py`
- `research/brick_ag2_kbase_sqnav_autorun.py`
- `apps/web_server.py`
- `apps/web_roundtable.py`

The backlog/autorun files may be changed only to install or enforce an entry adapter; their research parameters, dates, roster, prompts, and validation design remain frozen.

### Tests

- `tests/test_control_plane_contracts.py`
- `tests/test_control_plane_entry_guard.py`
- `tests/test_control_plane_sink_guards.py` (new)
- `tests/test_automation_resilience.py`
- `tests/test_kbase_ag2_full_cycle.py`
- `tests/test_discovery_handoff_bridge.py`
- `tests/test_handoff_runner_repair.py`
- `tests/test_knowledge_bridge.py`
- `tests/test_ag2_orchestration_contracts.py`

No strategy, signal, model, parameter, production configuration, market-data file, cache, research result, Registry, Snapshot, Handoff, Memory, or KBase content file is in this allowlist.

## Side-effect vocabulary correction

The current seven-value enum cannot faithfully describe production-data mutation, production-config mutation, deletion, outbound notification, arbitrary subprocess execution, or externally exposed services. If approved, P0 may add closed enum members for these effects; unknown effects remain denied. This is a control-contract change only and does not grant any of those effects to P0.

## Trusted authorization source and threat model

The P0 revision will use the following bounded design:

1. The controller provisions an immutable authorization envelope into the one fixed control-plane database before implementation starts. The envelope binds one exact phase, full actor/invocation identity, plan/scope/policy hashes, an `authorization_ref`, and the hash of a one-time random bearer secret.
2. Runtime code can only claim a pre-existing envelope. It cannot create authority from raw hashes, create authority by choosing another database path, retry a failed phase, or authorize the next phase.
3. The database stores only bearer-secret hashes. A public frozen dataclass, copied row, or manifest cannot itself act as authority.
4. Each task ticket authorizes exactly one `entry_id + side_effect + resource_scope + idempotency_key`. Its effect must be allowed by the phase and declared by the frozen entry manifest.
5. Task tickets transition `ISSUED -> IN_PROGRESS -> SUCCEEDED|FAILED|IN_DOUBT`. A crash after effect start is `IN_DOUBT` and is never automatically replayed unless the sink is provably idempotent and reconciliation succeeds.
6. Phase gates are immutable, canonical, evidence-bound records tied to one phase grant. They cannot close while any task ticket is open or in doubt. FAIL and the next phase both require a newly provisioned user authorization.
7. The old `phase_tokens` schema remains read-only `LEGACY_UNTRUSTED`; old tokens are never upgraded into grants. Unknown future schema versions fail closed.

This is a process-integrity boundary against accidental or legacy-code self-authorization. It is not claimed to resist a hostile process already running as the same Windows Administrator and able to rewrite the repository/database. A separate broker under another Windows identity is the stronger design, but it is an external operational change and is not silently introduced in P0.

## Required RED tests added by the revision

- public hashes or an arbitrary new DB path cannot bootstrap a grant;
- a missing, reused, mismatched, or tampered authorization envelope is rejected;
- P0 PASS still requires a fresh P1 authorization, and P0 FAIL requires a fresh retry authorization;
- a task ticket binds entry, effect, resource, actor/invocation, authorization ref, and all approved hashes;
- P0 cannot issue research, KBase, holdout, Git, production-data/config, network, process, or service-exposure tickets;
- resource containment rejects `..`, symlink/junction escapes, Windows drive aliases, and case-normalization tricks;
- identical idempotent issuance returns the same ticket while the same key with different semantics fails;
- concurrent ticket claim has one winner; crash after claim becomes `IN_DOUBT` and never auto-replays;
- forged grants/tickets and declaration side-effect/phase substitution are rejected;
- arbitrary report hashes, open tickets, in-doubt tickets, and stale concurrent attempts cannot create or replace a PASS gate;
- legacy token migration preserves audit evidence but revokes authority; unknown schema versions fail closed.

## Scientific risk

Low from the change itself: P0 performs no scientific computation and cannot promote a result. High if rejected but P0 is nevertheless marked PASS: legacy artifacts and Runner booleans could become authoritative without an audited execution path.

## Operational risk

- Guarding deepest sinks can intentionally break legacy automation that does not carry a valid task ticket.
- Guard placement must occur before any directory creation, file write, subprocess launch, network call, thread start, KBase write, or Git command.
- The production daily selection chain must be classified separately from controlled research so P0 does not silently disable normal selection/update behavior.
- The Windows task/ACL finding is not included in this code change. Changing it could affect other local users and the Codex runtime and therefore requires separate explicit authorization.

## Estimated time and tokens

```yaml
estimated_time: 2-4 hours for P0 code, tests, inventory, reviews, and gate evidence
estimated_tokens: unknown
token_reason: No reliable provider-level token ledger is exposed to this task; zero must not be recorded.
```

## Alternatives considered

1. Keep the original allowlist and mark P0 PASS: rejected because known bypasses remain.
2. Guard only CLI wrappers: rejected because import callers reach the shared sinks directly.
3. Edit every historical research script: rejected as unnecessarily broad; inventory plus non-authoritative quarantine is sufficient when all authority/write sinks are guarded.
4. Disable all research and web files with OS ACLs: rejected from this request because it is an external-state change with broader operational impact.
5. Retain a combined P0/P1 gate: rejected because it contradicts single-phase authorization and automatic-advance prohibition.

## Rollback

- Revert only files in the approved task allowlist using an explicit reverse patch; never use `git reset --hard` or `git checkout --`.
- Preserve all user-owned pre-existing changes.
- Remove no existing control-plane database; failed attempts remain immutable audit evidence.
- A failed P0 gate leaves all legacy scientific outputs `legacy_unaudited`, `promotion_eligible=false`, and ineligible for the future Ledger.

## Approval boundary

Until this request and its resulting plan revision are explicitly approved:

- no additional production/control-plane code is changed;
- the current P0 authorization is treated as consumed by the first attempt;
- no new phase token is issued against the old plan/scope hashes;
- P0 remains `DONE_WITH_CONCERNS`, not PASS;
- no P1 work starts.

The Windows scheduler/ACL is a separate decision and is not authorized by approving this request.

To approve only this bounded change request, use:

```text
APPROVE_CHANGE_REQUEST id=P0-CR-001
```

Approval permits generation of the immutable P0 revision and its new plan/scope hashes. It does not itself restart implementation; the revision manifest will then name the exact fresh P0 authorization command.
