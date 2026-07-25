# V3.4.2 P2: Immutable Data Generation and Cache Identity

## Objective

Keep a research run on immutable data without copying the approximately 84GB production data tree, while allowing daily update staging and preserving existing CSV/cache semantics.

## Immutable release seam

Extract a reusable `ImmutableReleaseStore` from existing KBase semantic/catalog publication behavior: stage, manifest validation, content hashes, current/previous pointer, atomic promote, recovery, and rollback. KBase and market-data generations use separate adapters; neither receives a second publication implementation.

A GenerationManifest binds CSV cutoff, trading calendar, point-in-time universe, adjustment scheme, missing-data policy, and cache manifests. CSV remains source of truth; raw parquet is a cleaned ascending cache; indicator/signal caches remain derived and production/research namespaces stay isolated.

## Read lease and daily publication

A Campaign holds a shared read lease on one generation. Daily update may download, stage, and validate while a lease exists, but cannot replace live CSV or move CURRENT. It records `PUBLISH_PENDING`.

The current cycle may finish. No next cycle starts while publication is pending. After release and publication, continuation requires an explicit campaign revision/new campaign binding; generation never switches silently. Lease/crash handling uses fencing, not file age.

## Cache manifest

Unify the cache identity interface, not cache file formats. Bind generation, source identity, feature contract, calendar, universe, adjustment, and research/production namespace. A pinned trusted path validates touched artifacts only; routine full-tree scans and 84GB rehashes are forbidden. Legacy unpinned mtime paths may remain but cannot produce trusted evidence.

## Missing-bar semantics

Use `PRESENT`, `NO_BAR_CONFIRMED`, `UNKNOWN_NO_BAR`, and `FETCH_FAILED`. Only source-confirmed absence may be labelled suspended. No-bar and fetch-failed rows cannot generate signal, entry, exit, or model features. Portfolio valuation may separately carry `STALE_VALUATION`, which cannot feed a model.

Preserve GBK source files, reverse chronological CSV order, ascending parquet order, checkpoint retention, existing `no_today_bar` legality, and daily production/research cache separation.

## Gate

Test staging crashes, reader/writer lease competition, pending publish, uncontrolled byte mutation, cache identity, research isolation, suspension/no-bar/fetch failure, disk full, CURRENT recovery, and legacy release hash compatibility. If backtest/signal/parameter code must change, stop and obtain approval for the exact verification command before running any backtest.

