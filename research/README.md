# Research script layout

Versioned and historical research runners live here instead of the repository
root.  Run commands from the repository root unless a script says otherwise.
The moved runners use the repository's `AGENTS.md` marker to locate this root;
keep that marker with any portable checkout of the research tree.

## B1

- `b1/benchmarks/`: signal generation and benchmark comparisons.
- `b1/phase11_13/`: completed Phase 11–13 isolation, ablation, recovery, and
  generator reproduction runners. Their historical snapshot and quarantine
  status are documented under `archive/research/b1/phase11_13/README.md`.
  Compute runners are locked against default execution and require a new,
  explicitly named output directory. Do not run them to validate file layout.

## Brick

- `brick/legacy/v1/`: retired V1 backtest and analysis.
- `brick/legacy/v3/`: retired V3 factor/ranker/backtest tools.
- `brick/legacy/ml/`: historical ML training and walk-forward runners.
- `brick/v2/`: V2 L1, V2A, V2C, V2R, V2S, and WF3 experiments.
- `brick/v4_pipeline/`: the V4 orchestrator and its complete phase chain.
- `brick/v5/`: V5 profile, clustering, regime, ranker, and comparison stages.
- `brick/v5e/`: V5E entry-feature, L1, scoring, and account experiments.
- `brick/_paths.py`: shared project/input/output path contract.

The flat `research/brick_*.py` files are newer AG2/KBase protocol and factor
experiments.  They remain in place because active research-state evidence and
runner registrations refer to those exact paths; regrouping them requires a
separate runner/Registry migration.

Generated research data belongs under `artifacts/research/`.  Production CSV
and cache inputs under `data/` remain read-only to these historical runners.
Deleted raw research panels must be regenerated under
`artifacts/research/legacy_panels/`; runners must not recreate the old root
aliases or `data/v5*` directories.

The full old-to-new map and validation record is in
`archive/manifests/script_layout_migration_20260724.md`.
