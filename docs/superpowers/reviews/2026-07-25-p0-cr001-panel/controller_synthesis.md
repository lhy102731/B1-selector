# P0-CR-001 Parallel Review — Controller Synthesis

## Outcome

All five requested profiles returned without provider failure. Four profiles used the Volcengine pool and excluded its DeepSeek profile; `deepseekv4` used the official DeepSeek API. No model or provider was silently substituted.

The panel supports the CR direction. One reviewer returned `PASS`; four returned `PASS_WITH_FIXES` or an equivalent conditional approval. MiniMax and DeepSeek reached the configured 6,000 completion-token ceiling, so only their captured recommendations are used and their responses are not represented as complete.

## Accepted corrections for the P0 revision

- Public plan/scope/policy hashes are identifiers, never credentials.
- The fixed authority chain is `pre-provisioned authorization envelope -> PhaseGrant -> one-effect TaskTicket -> SideEffectLease`.
- P0 PASS satisfies a prerequisite only. It cannot issue P1 authority; P0 FAIL cannot self-authorize a retry.
- The fixed database path, envelope semantics, bearer-secret hashing, schema version, and legacy-token revocation must be explicit.
- `declared_side_effects` and `declared_phase` are part of entry-manifest identity and must be compared.
- Gate creation and latest-attempt checks occur in one transaction; callers cannot submit an arbitrary PASS hash.
- `gate_report.json` has a closed canonical schema and a self-hash over every field except `self_hash`.
- All new side-effect enum members remain denied in P0.
- Production daily selection is inventoried as a separate non-research operational chain and must not be silently disabled by research guards.
- Guards execute before directory creation, file writes, network calls, threads, subprocesses, KBase operations, and Git commands.
- P0 uses the simple crash rule: once a side effect starts and completion cannot be proven in the same process, the ticket becomes `IN_DOUBT`; P0 never automatically replays or reconciles it. Rich reconciliation is deferred.

## Main-controller decisions where reviewers differed

### Windows scheduler and ACL

The enabled `\\A股选股` task and its Administrator/writable-code-chain finding remain a high-priority external operational risk. P0 must inventory it, record its XML/hash/ACL status, and prove it cannot create trusted research provenance. P0-CR-001 does not authorize modifying the task or ACL.

The code-side P0 gate is not made contingent on an unauthorized external mutation. Instead, the unresolved ACL risk blocks any claim that the overall machine is production-hardened and blocks later production promotion until a separate explicit operational decision is made.

### `IN_DOUBT` recovery

P0 implements no automatic reconciliation. This removes an unnecessary state-machine branch while preserving fail-closed behavior. A future phase may add sink-specific reconciliation only with explicit evidence rules and authorization.

## Newly confirmed allowlist omission

Repository inspection identified five existing files and four new test/control files required by the already-approved objective but absent from CR-001's explicit file list. Because the master prompt requires exact file allowlists, they are proposed in `P0-CR-001-A`; they are not silently added to the revision.

## Token record

Provider-reported usage was 15,377 prompt tokens, 27,776 completion tokens, and 43,153 total tokens. Internal controller/subagent token usage is unavailable and must remain `unknown`, not zero.
