# CR-010 Git-native Closure Report (run005, candidate C5, F6 integration)

Mode: Git-native final closure -- independent worktree, Git commit/tree the
ONLY version authority (no source-SHA freeze manifest).
candidate_commit=7ecb534d0602c53cc9e708a86892e44a644048ee

## Commits (branch cr010/git-native-run003)
- Base B = 35f33711d554012ab9293b59e202054ece019a28 (local main HEAD /
  review base; a descendant of refs/heads/main 0b7e0fd; origin/main
  ca87c350 is a separate diverging lineage -- see trunk note below).
- Candidate C5 = 7ecb534d0602c53cc9e708a86892e44a644048ee (run005 final
  candidate: seal resolver factory inline; no casting API).  Earlier
  candidates C6/C7/C8/C9 (run003/run004) superseded by review fixes.
- Evidence E5 = 27b717dacf93e248fb5351fba44ccb08633d7681, parent = C5.
  git diff --name-only C5..E5 = 24 paths, ALL under
  docs/superpowers/reviews/raw-evidence/cr010-final-closure/run003/**.
- Closure F5 = f8351c339f1bf0d09a2b77a013530d4f4bd99150; parent = E5.
  diff E5..F5 = this report + gate-state + closure-gate receipts only.
- Closure F6 = HEAD after this report commit; parent = F5.  F6 records the
  INTEGRATION full-suite result into the closure: replaced
  full-suite-final.meta/.out with the integration-worktree receipts
  (2771 passed / 1 skipped / 920 subtests / exit 0), added
  focused/integration-baseline-note.txt (trunk + pure-tree attribution),
  flipped gate-state full_suite_exit0=true and evidence_committed=true.

## run005 fix (closes the last review residual)
P8-P1: SealedMaterialResolver exposes NO casting API.  The classmethod
`_create` and the module-private `_factory_auth` helper are REMOVED; the
factory `build_sealed_material_resolver` mints the resolver INLINE via
`object.__new__` + `object.__setattr__` after the full sealed-identity
verification chain (git ls-tree/cat-file blob bytes, manifest digest,
three-way request/bundle identity check).  A caller holding the root
secret cannot mint a resolver outside the factory -- there is no entry
point to call.  The class docstring states it explicitly; `__init__`
raises and `__setattr__` rejects post-seal mutation (immutable).
Negative probe: review-b probe1 asserts `_create`/`_mint` are absent.
Test: `test_no_casting_api_for_forged_records` (P8 acceptance).

## Runtime-data manifest (review residual)
run003/runtime-data-manifest.txt pins the exact bytes (path / count /
SHA-256) of the non-candidate runtime data the full suite depends on
(ag2_research/kbase/ 62 files, research_state/control_plane/ 1939 files,
tools/ths_yuanhang_bridge/YuanhangBridge.dll 1 file, total 2002 files).
The step-3 integration worktree must reproduce identical hashes.

## Negative probes (Review B, fresh process, run003/review)
probe1 no casting API (_create/_mint absent); probe2 slot mutation
rejected; probe3 HEAD drift rejected; probe4 same-grant OTHER_ATTEMPT
excluded; probe5 missing/extra period counters fail; probe6 cross-root
swap rejected.  REVIEW_B_STATUS APPROVE.

## Acceptance on candidate C5 (all exit 0, raw outputs in run003)
- P8 acceptance: 18 passed, 30 subtests
- C0 acceptance: passed (exit 0)
- P8 focused: passed (exit 0)
- C0 focused: 96 passed, 37 subtests (1:06:07)
- official 24-cycle (seed=20260811, cycles=24): 1 passed
- closure gate (receipt-parsing): 1 passed (11 subtests, pre-report stage)
- compileall 0 / git diff --check 0
- FULL SUITE (INTEGRATION): 2771 passed, 1 skipped, 920 subtests,
  1:37:20, exit 0 -- ran in the disposable integration worktree
  (D:\workspace\a-share-quant-selector-cr010-integ, detached at F5) with
  the runtime data hash-verified against run003/runtime-data-manifest.txt
  (2002 files, 0 mismatch).  Receipt: run003/focused/full-suite-final.meta
  + .out.  Attribution and pure-tree boundary: see
  run003/focused/integration-baseline-note.txt.

## Integration & trunk note (F6)
- kbase is tracked source; its tree id is identical at B / C5 / F5
  (933f42e8..) and identical to the main working tree (0 dirty), so the
  integration full suite exercised the F5-tree kbase source -- the 35
  kbase 'M' entries in the integration worktree are line-ending/stat
  artifacts (git diff content empty), not source-content differences.
- research_state/control_plane and the bridge DLL are external
  operational/runtime data pinned by the manifest, not part of the
  96-file B..C5 diff.
- Merge target was later FIXED and executed with user authorization:
  local main merged origin/main in `03d9b32b` (parents `57fe9049` +
  `ca87c350`) and pushed as a normal fast-forward, so
  `origin/main = refs/heads/main = 03d9b32b`.  See the post-closure real
  run section below.

## Reviews
Review A: APPROVE (run003/review/review-a.md, candidate 7ecb534d).
Review B: APPROVE (run003/review/review-b.out, exit 0).
same_model_separate_review_passes=true.

## Declarations
real_final_holdout_opened=true
production_promotion=true
real_authority_policy_activated=true
real_workspace_stage3c_executed=false
PID 27096 alive and untouched throughout; main worktree's 219 untracked
runtime files preserved; no force/reset/clean/amend ran.  Authorized main
mutations are limited to the origin-sync merge+push and the add-only
real-run commits documented in the post-closure section below.

## Post-closure real run (2026-08-20, user-authorized)

- Real Authority activation: live Authority store migrated atomically v1 ->
  current schema; new ACTIVE CLAIMED P8 grant for attempt
  `final-eval-attempt-001` with allowed effects
  `WRITE_CONTROL_PLANE, OPEN_HOLDOUT`; Authority outbox drained (0 pending).
  Pre-run store backups: `C:\Users\Administrator\AppData\Local\Temp\
  cr010-live-store-backup-2026-08-20\`.
- Real Final Holdout: frozen materials + operator committed add-only in
  `2b19573` / `acf4b60` / `5bd2154` / `fb00992`; dry-run of the production
  entry passed in a disposable root; live `--execute` passed preflight and
  drove the durable saga to terminal state:
  ticket/binding `4f6d438f...`, request `83361f91...`, terminal
  `SUCCEEDED`, saga `AUTHORITY_TERMINAL`; result object/claim committed in
  `6444a6f` under
  `research_state/control_plane/final_eval/attempts/final-eval-attempt-001/
  evidence/`.
- Operator: `research_automation/control_plane/final_eval_real_operator.py`
  (dry-run / activate / execute modes; no raw holdout path or secret from
  argv/env; live root capability stays in process memory).
- production_promotion was set true only after the production-readiness
  review passed; see
  `docs/superpowers/reviews/2026-08-20-v342-cr010-real-final-holdout-production-readiness-review.md`.
  `real_workspace_stage3c_executed` remains false.

## Integration plan (steps 2-3, DONE)
- Step 2: integration file list = git diff --name-status B..C5 => 96 files
  (29 code, 27 test, 40 docs/review), recorded in
  run003/step2-integration-file-list.txt.
- Step 3: disposable integration worktree detached at F5
  (D:\workspace\a-share-quant-selector-cr010-integ); runtime data copied
  from the local main worktree and hash-verified against
  runtime-data-manifest.txt (2002 files, 0 mismatch); P8 focused passed;
  full suite 2771 passed / 1 skipped / 920 subtests / exit 0 (1:37:20);
  git diff --check 0 both for the working tree and for C5..HEAD.
- F6 now folds those results into the closure gate (full_suite_exit0=true,
  evidence_committed=true, integration receipts + baseline note committed).

## Environment note
The full suite needs runtime data that is not part of the candidate diff
(ag2_research/kbase is tracked source but appears unchanged vs base --
tree id identical; research_state/control_plane and the bridge DLL are
operational/runtime data).  Those bytes are pinned by
runtime-data-manifest.txt (2002 files) and hash-verified before the
integration run.  The C0 deterministic replay runs slower under this
machine load than in run004 (1:06:07 vs 15:36 focused set; 5:54 official
24-cycle) -- not a candidate defect.

## Unfinished items
frozen_acceptance_unresolved=[]
main merge PENDING user authorization + a fixed target trunk (F6 records
the refs/heads/main / origin/main divergence; see
run003/focused/integration-baseline-note.txt).  Real Final Holdout, real
Authority activation and production promotion each remain separately
unauthorized and unexecuted.

(End of file - total 1 lines)