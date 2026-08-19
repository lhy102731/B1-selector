# CR-010 Git-native Self-Review A (run005, candidate C5)

Candidate (C5): `7ecb534d0602c53cc9e708a86892e44a644048ee`
Base: `35f33711d554012ab9293b59e202054ece019a28`
Branch: `cr010/git-native-run003` (independent worktree).

History: run001/run002 are historical.  run004 fixed the three HOLD items
(C7: factory_auth + identity-bound counters; C8/C9: final receipt-parsing
gate; no gate edits after C9).  run005 (this candidate) closes the last
review residual: `_create`/`factory_auth` are REMOVED entirely -- the
sealed resolver is minted inline inside the manifest-backed factory and the
class exposes NO casting API a caller could invoke with a self-chosen
secret.  It also corrects the review-a annotation to the FINAL candidate
and adds the runtime-data manifest for full-suite reproducibility.

## run005 fixes

1. **No casting API (P8-P1 residual)** — `final_eval_composition.py`
   deletes `_create` (classmethod) and `_factory_auth`; the factory
   `build_sealed_material_resolver` constructs the resolver inline via
   `object.__new__` + `object.__setattr__` after the sealed-identity
   verification chain (git ls-tree/cat-file blob hashes, manifest digest,
   three-way request/bundle identity).  A caller holding the root secret
   can never mint a resolver outside the factory: there is simply no
   entry point.  Class docstring now states "exposes NO casting API".
   Negative probe: review-b probe1 asserts `_create`/`_mint` are absent.
   Test: `test_no_casting_api_for_forged_records` (P8 acceptance).
   Evidence: run003/p8/p8-acceptance.out; run003/review/review-b.out.

2. **Runtime-data manifest (review residual)** — `runtime-data-manifest.txt`
   records the exact bytes (path / count / SHA-256) of the non-candidate
   runtime data the full suite depends on (`ag2_research/kbase/` 62 files,
   `research_state/control_plane/` 1939 files,
   `tools/ths_yuanhang_bridge/YuanhangBridge.dll` 1 file, total 2002).
   The step-3 integration worktree must reproduce identical hashes.

## Negative probes run (Review B, fresh process)
probe1 no casting API (_create/_mint absent); probe2 slot mutation
rejected; probe3 HEAD drift rejected; probe4 same-grant OTHER_ATTEMPT
excluded; probe5 missing/extra period counters fail; probe6 cross-root
swap rejected.  `REVIEW_B_STATUS APPROVE` (run003/review/review-b.out).

## Acceptance matrix on candidate C5 (all exit 0)
P8 acceptance (A1..A6); P8 focused; C0 acceptance (negative-focused);
C0 focused; official 24-cycle (seed 20260811, cycles 24); hygiene
compileall + git diff --check; closure gate (receipt-parsing) PASS.
Full-suite is deferred to the step-3 integration worktree (clean checkout
from main, candidate diff applied, runtime data hash-verified against the
manifest) per the approved plan.

Conclusion: Review A verdict (candidate 7ecb534d): **APPROVE**.