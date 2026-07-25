# V3.4.1 Data Generation and Pinned DataView Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use Superpowers `subagent-driven-development` and `test-driven-development`. This plan never runs a real data update or backtest.

**Goal:** Turn the existing CSV/raw-parquet/indicator/signal cache chain into versioned, atomically published generations that a run can pin and verify without copying or rehashing the full 84GB data set.

**Architecture:** `GenerationPublisher` records a manifest under `research_state/control_plane/generations/`. A pinned run reads through `GenerationPin`, which verifies the manifest and touched artifacts. Existing CSV remains source of truth; raw parquet and indicator/signal caches remain derived. Daily updates stage and validate changed files before publication.

**Tech Stack:** Python standard library, existing pandas/Parquet utilities, existing daily checkpoint helpers, `unittest`.

---

## Files and responsibilities

### Create

- `research_automation/control_plane/generation.py` — staging/publish, CURRENT pointer, pin verification and artifact fingerprints imported from contracts.py.
- `tests/test_control_plane_generation.py` — atomic publish, pin, mutation, cache identity and crash tests.

### Modify

- `tools/update_today_em_client.py:23-35,46-61,64-180,502-556` — stage changed CSVs, validate the complete checkpoint, publish one generation, and retain the existing suspended/no-bar semantics.
- `utils/raw_parquet_cache.py:31-75` — accept an optional `GenerationPin` and reject stale/mutated artifacts.
- `build_indicators_cache.py` — write generation metadata into derived cache manifests; preserve `--raw-only`, `--research-cache`, and production/research separation.
- `backtest_optimized.py` only if the existing cache identity seam cannot consume a generation id; if modified, run the AGENTS-required isolated known-good verification backtest before the task gate.
- `tests/test_raw_parquet_cache.py`, `tests/test_signal_cache_identity.py`, `tests/test_daily_checkpoint_retention.py` — add regression assertions without changing existing expected behavior.

## Generation contract

`generation.py` imports the canonical ArtifactFingerprint and GenerationManifest types from contracts.py and exposes:

```python
class GenerationPublisher:
    def stage(self, payload: dict) -> Path: ...
    def publish(self, staged_manifest: Path) -> GenerationManifest: ...

class GenerationPin:
    def verify_manifest(self) -> None: ...
    def verify_artifact(self, relative_path: str, *, require_content_hash: bool = False) -> None: ...
    def data_snapshot_id(self) -> str: ...
```

`publish` writes a complete staged manifest, fsyncs it, atomically replaces a generation directory marker, then atomically updates `CURRENT.json`. It never edits an existing generation. The generation nonce is unique and `generation_id` is derived from the canonical manifest including nonce, hash_state and artifact metadata. Raw CSV source artifacts must be `CONTENT_HASHED` when first touched by a pinned run; derived/cache artifacts may start `LAZY` and are content-hashed before use. A changed size/mtime/content returns `GenerationMutatedError("GENERATION_MUTATED")` and records an access/audit event. A one-time, separately authorized baseline inventory may establish the initial manifest; it is not part of routine runs.

## Task 1: Generation manifest and atomic CURRENT

**Files:** create `generation.py`, `tests/test_control_plane_generation.py`.

- [ ] **Step 1 RED:** Add `test_publish_writes_complete_manifest`, `test_current_pointer_changes_only_after_valid_manifest`, `test_partial_manifest_is_not_readable`, `test_old_generation_is_create_only`, and `test_manifest_hash_is_stable`. Run:

  `python -m unittest tests.test_control_plane_generation -v`

  Expected: FAIL because no generation module exists.

- [ ] **Step 2 GREEN:** Implement `GenerationPublisher.stage/publish` and `GenerationPin` using the canonical types from contracts.py. Use `tempfile.NamedTemporaryFile` in the target directory, `flush`/`os.fsync`, and `os.replace`; reject path traversal, junction/symlink roots, malformed manifests, duplicate artifact paths, missing required fields, and a production publish root unless a consumed phase token explicitly authorizes it.

- [ ] **Step 3 GREEN verification:** Run the targeted tests with a temporary root. Kill/interrupt only a test subprocess during staging and verify `CURRENT.json` still points to the previous generation.

- [ ] **Step 4 REFACTOR:** Keep manifest generation bounded: never `rglob` raw data automatically; accept an explicit artifact list from the caller and record root metadata for unlisted files. Do not introduce an environment-variable bypass for the production-root guard.

- [ ] **Step 5 Evidence/rollback:** Save the fixture manifest and root hash. Rollback removes the temporary generation root only.

## Task 2: Daily update staging and publication

**Files:** `tools/update_today_em_client.py`, `tests/test_daily_checkpoint_retention.py`, `tests/test_control_plane_generation.py`.

- [ ] **Step 1 RED:** Add `test_invalid_daily_checkpoint_does_not_change_source_csv`, `test_valid_checkpoint_publishes_one_generation`, `test_suspended_no_today_bar_is_not_forward_filled`, and `test_second_publish_keeps_pinned_generation_readable`. Run the two targeted modules; expected FAIL for the new behavior.

- [ ] **Step 2 GREEN:** Refactor only the update transaction boundary: write changed CSVs to the date staging directory, retain the existing per-file backups and `validate_checkpoint_run`, and do not replace source CSVs until the complete report validates. On validation failure, leave source CSVs unchanged and write a failure report. On success, atomically replace staged CSVs, mark derived caches stale, and call `GenerationPublisher.publish`.

- [ ] **Step 3 GREEN verification:** Run the targeted modules. Expected: existing `no_today_bar` and checkpoint-retention tests remain PASS; failed validation leaves byte-identical source fixtures.

- [ ] **Step 4 REFACTOR:** Preserve GBK encoding, reverse chronological CSV order, current cache cleanup behavior, and `DAILY_CHECKPOINT_RETENTION`. Do not change fetcher scope or date parameters.

- [ ] **Step 5 Evidence/rollback:** Record the before/after fixture hashes and generation id. Rollback is restoring only the staged temporary root; never delete real backups.

## Task 3: Pin raw parquet and indicator/signal caches

**Files:** `utils/raw_parquet_cache.py`, `build_indicators_cache.py`, optionally `backtest_optimized.py`, related tests.

- [ ] **Step 1 RED:** Add `test_pinned_read_rejects_mutated_csv`, `test_raw_csv_requires_content_hash`, `test_pinned_raw_parquet_uses_generation_id`, `test_research_cache_generation_is_separate`, `test_unversioned_cache_is_invalid_for_pinned_run`, and `test_signal_identity_changes_when_generation_changes`. Expected FAIL.

- [ ] **Step 2 GREEN:** Add an optional `generation_pin` argument to `RawParquetCache` and cache-build helpers. Before returning a cached frame, call `verify_artifact`; raw CSV verification always requires content hash, and derived cache metadata includes `generation_id` and `data_snapshot_id`. Keep the existing mtime shortcut for unpinned legacy callers, but never use it for a V3.4 pinned run.

- [ ] **Step 3 GREEN verification:** Run `python -m unittest tests.test_raw_parquet_cache tests.test_signal_cache_identity -v`. If `backtest_optimized.py` changes, run the isolated AGENTS known-good verification backtest with its exact approved parameters and record command, exit code, and output/cache paths.

- [ ] **Step 4 REFACTOR:** Do not overwrite `data/indicators_cache` from research mode; preserve explicit `research_indicators_cache` behavior. Keep all path validation centralized.

- [ ] **Step 5 Evidence/rollback:** Save cache identity fixtures and generation manifest hash. Rollback only the optional generation arguments, not the existing cache format.

## Task 4: Suspended-stock and NAV semantics

**Files:** `generation.py`, `utils/raw_parquet_cache.py`, `tests/test_daily_checkpoint_retention.py`, `tests/test_market_data_semantics.py`.

- [ ] **Step 1 RED:** Add `test_suspended_day_has_no_signal_features`, `test_suspended_day_never_uses_previous_price_for_entry_or_exit`, and `test_stale_nav_valuation_has_explicit_missing_reason`. Expected FAIL.

- [ ] **Step 2 GREEN:** Represent a missing bar as `bar_status=SUSPENDED`, `missing_reason=SUSPENDED`, with null feature/entry/exit fields. Permit only a separate `STALE_VALUATION` portfolio-mark field; it cannot enter signal, model, or trade execution input.

- [ ] **Step 3 GREEN verification:** Run the focused market-data tests and existing daily checkpoint tests.

- [ ] **Step 4 REFACTOR:** Reuse existing `no_today_bar` records; do not introduce forward-fill in normalization or indicator calculation.

- [ ] **Step 5 Evidence/rollback:** Record the fixture rows and assertions. Rollback only the new metadata fields in temporary test fixtures.

## Task 5: P2 gate report

**Files:** create `docs/superpowers/plans/2026-07-25-v34-p2-gate-report.md`.

- [ ] **Step 1:** Run all generation/cache/checkpoint tests and the full existing raw-cache test set.
- [ ] **Step 2:** Verify `CURRENT` atomicity, old-generation mutation failure, raw-source content verification, no routine full-data rehash, suspended-stock semantics, and production/research cache separation.
- [ ] **Step 3:** Compare filesystem manifest to the P2 allowlist and record any external concurrent change.
- [ ] **Step 4:** Write `gate_status=PASS|FAIL`; only PASS permits P3.

## Task exit criteria

P2 is complete only when a pinned run cannot observe an in-place mutation, daily failure cannot partially publish source data, derived caches carry generation identity, and suspended bars cannot leak into signals or entry/exit features.
