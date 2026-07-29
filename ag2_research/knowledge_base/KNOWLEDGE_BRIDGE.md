# Knowledge Bridge v1

Knowledge Bridge keeps the broad Obsidian vault and the strategy closure separate while allowing audited evidence to flow between them.

## Authority Order

1. `ag2_research/knowledge_base/<subject>/hard_constraints.yaml`
2. validated claims under `D:\KBase\wiki\claims\`
3. source, concept, visual, and query notes opened explicitly for research

External evidence can support a proposal. It cannot override a project hard constraint or authorize a strategy change by itself.

## Claim Consumption Contract

A KBase note is visible through the bridge only when all fields pass:

```yaml
type: claim
status: reviewed
claim_id: b1-example-001
project_subjects: [b1_v3]
validation_status: validated
evidence_level: project_validated
sources: [wiki/sources/example.md]
information_available_at: signal bar close
lookahead_review: passed
execution_review: passed
project_kb_version: 1.0.0
confidence: high
```

Draft claims, source notes, concepts, and project outputs are excluded.

## APIs

```python
from ag2_research.knowledge_bridge import (
    build_combined_research_context,
    load_validated_claims,
    write_experiment_output,
)

claims = load_validated_claims("b1_v3")
context = build_combined_research_context("b1_v3", query="volume contraction")
```

AG2 agents can call `kb_validated_claims`. The autonomous runner injects the combined context automatically.

## Experiment Writeback

Every autonomous experiment can append an idempotent evidence record under:

```text
D:\KBase\wiki\outputs\projects\<subject>\Cycle <cycle_id>.md
```

Writeback records use:

```yaml
validation_status: unreviewed
promotion_status: output_only
```

They include hypothesis, scope, metrics, baseline, artifact paths, referenced claim IDs, project KB fingerprint, and an explicit promotion block. Automation never writes to `wiki/claims/`.

Set `KBASE_PATH` to use another vault. Set `KBASE_WRITEBACK=0` to disable autonomous writeback for a run.

## Promotion Path

```text
source/visual evidence
  -> testable hypothesis
  -> project experiment output
  -> independent audit
  -> validated claim
  -> optional project KB version update
```

Updating the project KB remains a separate, reviewed action with a manifest version bump.
