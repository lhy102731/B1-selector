# AG2 KBase progressive-disclosure layer

This package makes KBase source material discoverable without turning KBase into a factor or hypothesis generator.

## Authority boundary

- KBase adapters, catalog, tools, intake, and Source Librarian are source-only.
- `source_brief` rejects hypothesis, mechanism, factor, proxy, formula, parameter, V5 mapping, and research queue fields.
- Alpha Hunter and Factor Engineer may create project-side inference only after the Source Librarian handoff. They cannot directly browse broad KBase sources in the `kbase_discovery` workflow.
- Existing strategy logic, parameters, backtests, and project hard constraints are outside this package.

## Published files

The catalog is published under:

```text
D:\KBase\wiki\outputs\manifests\ag2-kbase\
  current\
  previous\
  candidate\
  archive\
  intake\
```

`current` changes only after schema and coverage validation. Failed candidates remain isolated. `previous` is the rollback release.

## Operations

Build and publish an incremental catalog:

```powershell
python -m ag2_research.kbase.catalog_builder --vault D:\KBase
```

Run the fixed and frozen-holdout query suites:

```powershell
python -m ag2_research.kbase.query_regression --engine catalog --output data/ag2_kbase/query-regression-catalog.json
python -m ag2_research.kbase.query_regression --engine catalog --cases ag2_research/kbase/query_holdout.yaml --output data/ag2_kbase/query-holdout-catalog.json
```

## Selected local semantic models

Production semantic search uses only the bakeoff-selected pair:

- embedding: `bge-m3`
- reranker: `bge-reranker-v2-m3`

The layer remains a search fallback after maps and deterministic metadata. It
requires a lexical anchor, preserves exact source/date/title hits, and returns
the original lexical order whenever the GPU worker, index, model, or timeout
gate fails. It does not expose an HTTP endpoint. The long-running CUDA worker
uses an offline local filesystem queue inside a `--network none` container.

The production merge is deliberately conservative: navigation commands remain
pure lexical, ordinary searches keep the first two lexical candidates fixed,
and comparison searches keep the first two candidates from every comparison
branch. BGE may supplement and rerank the remaining candidates. Reranker logits
are compared only within one query; there is no cross-query score threshold.

Provision the exact reviewed model bytes, create the catalog-bound candidate,
run both candidate-target regression suites, and publish only after approval:

```powershell
python -m ag2_research.kbase.semantic_index --vault D:\KBase provision-models --source-candidate <approved-candidate> --evaluation <expansion20> --apply
python -m ag2_research.kbase.semantic_index --vault D:\KBase bootstrap --source-candidate <approved-candidate> --evaluation <expansion20> --apply
python -m ag2_research.kbase.semantic_index --vault D:\KBase regress --candidate <semantic-candidate> --apply
python -m ag2_research.kbase.semantic_index --vault D:\KBase publish --candidate <semantic-candidate> --apply
python -m ag2_research.kbase.semantic_index --vault D:\KBase install-service --apply
```

`install-service` refuses to replace an existing container. A validated prior
semantic release can be inspected and rolled back separately:

```powershell
python -m ag2_research.kbase.semantic_index --vault D:\KBase rollback
python -m ag2_research.kbase.semantic_index --vault D:\KBase rollback --apply
```

Omit `--apply` for the dry-run form of every mutating command. Model files are
published through verified NTFS hard links, so the selected pair remains stable
without duplicating approximately 4.27 GiB of weights. The evaluated dense
matrix is bridged into a self-contained semantic release and remains hash-bound
to the active catalog; no evaluation or external judging is rerun.

Register a new local source and publish it:

```powershell
python -m ag2_research.kbase.ingest C:\path\to\source.pdf --vault D:\KBase --publish
```

Register a directory as one batch (recursive by default) and publish once:

```powershell
python -m ag2_research.kbase.ingest C:\path\to\incoming --vault D:\KBase --extensions txt,md,pdf,mp4 --publish
```

Preview without writing anything, or scan only the directory's top level:

```powershell
python -m ag2_research.kbase.ingest C:\path\to\incoming --vault D:\KBase --dry-run
python -m ag2_research.kbase.ingest C:\path\to\incoming --vault D:\KBase --no-recursive --publish
```

Batch intake isolates failures per file, reuses complete content-addressed intake
objects on rerun, and writes a JSON batch summary under
`wiki/outputs/manifests/ag2-kbase/intake-batches/`. A batch publishes the catalog
at most once, after all selected files have been attempted.

Intake is local-only by default. It preserves the original by hash once, writes generated packets under `wiki/outputs/source-packets/intake/`, and reports discoverability separately from OCR/ASR/visual completeness. The external-upload flag records consent for that invocation but the current deterministic intake never uploads content.

Generate the metadata-only maintenance report:

```powershell
python -m ag2_research.kbase.maintenance --vault D:\KBase --output data/ag2_kbase/maintenance-report.json
```

Preview or resume the candidate-only content re-extraction queue:

```powershell
python -m ag2_research.kbase.reextraction_runner D:\KBase\wiki\outputs\candidates\ag2-kbase\content-layer-repair\content-reextraction-queue.json --vault D:\KBase --dry-run
python -m ag2_research.kbase.reextraction_runner D:\KBase\wiki\outputs\candidates\ag2-kbase\content-layer-repair\content-reextraction-queue.json --vault D:\KBase
```

The runner checkpoints every item, resumes interrupted work, isolates failures,
and writes an append-only audit log plus anchored extraction artifacts. It uses
only local extractors and never edits raw files, packets, or the catalog.
Extracted text is deliberately not labelled as an accepted statement or
evidence; OCR/ASR-unavailable items remain blocked instead of being guessed.
Every output passes the shared content quality gate. Anchored text remains
non-publishable pending distillation, and a plugin-produced statement/evidence
candidate is publication-eligible only when the gate returns `accept`;
`review` and `reject` remain non-publishable.

Rollback is an explicit Python operation:

```python
from pathlib import Path
from ag2_research.kbase.catalog_builder import rollback_catalog

print(rollback_catalog(Path(r"D:\KBase")))
```

## AG2 workflow

The default CLI path is source-first:

```powershell
python run_research.py discover --strategy brick --topic "优化当前选股系统"
```

It runs:

```text
Project State Compiler -> Research Gap Request -> Source Librarian
-> multi-LLM roundtable -> Alpha Hunter -> Falsification Officer
-> Factor Engineer
```

The compiler binds Snapshot, Handoff, Registry, project KB, recent research
artifacts, production/research script hashes, and the live semantic release
bundle. Discovery stops before model use when catalog, semantic index, gate, or
runtime identity disagree. `kbase_roundtable_discovery` remains available only
through `--roundtable-first` as a legacy compatibility path.

The older single-pass `kbase_discovery` runs:

```text
Source Librarian -> Alpha Hunter -> Falsification Officer -> Factor Engineer
```

The orchestrator rejects malformed briefs, unknown source IDs, stale catalog versions, untraceable evidence references, missing KBase handoffs, and independent work that falsely claims source support.

Do not run this workflow merely to test connectivity. It is intended for a real AG2 research gap after source organization is accepted.

The post-release shadow suite is frozen at
`ag2_research/kbase/query_shadow_20260723.yaml`. It is independent of the fixed
and original holdout suites and must not be edited to accommodate later ranking
changes; add a newly dated suite instead.

## Maintenance rules

- New source packets are picked up incrementally; unchanged catalog entries are reused.
- Keep the 30-case regression suite stable. Add new cases rather than rewriting targets to hide failures.
- Treat `query_holdout.yaml` as frozen evidence against regression-set overfitting.
- Usage logs store query hashes and IDs, never source text or query text.
- Regression and holdout runners disable production telemetry; synthetic runs must not drive content-deepening priority.
- High usage means “worth reviewing,” not “correct.”
- Never repair legacy raw packets in place. Compatibility adapters and candidate repair remain separate.

## Progressive coverage, not disk-file coverage

Coverage means that every published source packet can be reached from a Wiki
map, source family, or date entry and then disclosed through summary,
statements, evidence, and raw layers. It does not mean eagerly indexing every
file in the raw vault. Run `python -m ag2_research.kbase.maintenance --vault
<path>` to obtain the per-packet coverage report and its P0/P1/P2 gap queue.
The report separates catalog declarations from verified coverage: packet JSON
must be readable, statement/evidence fields must contain usable content, and
the raw path must resolve to an existing file. Map reachability follows the
full parent/family ancestor chain. Dated packets are reachable through dates
shown by `kbase_overview` or an explicit `date:<date>` browse node.
