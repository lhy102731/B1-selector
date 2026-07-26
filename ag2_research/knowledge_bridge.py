r"""Bridge validated KBase evidence with the project's strategy knowledge base.

The two stores have intentionally different authority:

* ``D:\KBase`` contains broad source material, source representations,
  visual evidence, and project outputs.
* ``ag2_research.knowledge_base`` contains versioned, machine-enforced
  strategy closures.

Only explicitly validated KBase claims may enter the automatic project-context
path. Agents may separately browse source material for inspiration, but must
label their own mechanisms and hypotheses as project-side inference. Experiment
writeback always lands in ``wiki/outputs`` and never promotes itself to a claim.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

import yaml

from research_automation.control_plane.contracts import SideEffect
from research_automation.control_plane.sink_guard import (
    AuthorizedPathMutation,
    ExecutionInvocation,
)
from research_automation.control_plane.stores import AuthorityReader, TaskExecutionLease


DEFAULT_VAULT = Path(os.environ.get("KBASE_PATH", r"D:\KBase"))
PROJECT_ROOT = Path(__file__).resolve().parent.parent
VALIDATED_STATUSES = {"validated", "verified", "accepted", "promoted"}
REVIEWED_NOTE_STATUSES = {"reviewed", "validated", "accepted", "promoted"}
PASSED_REVIEWS = {"passed", "not_applicable", "not applicable", "n/a"}


@dataclass(frozen=True)
class ValidatedClaim:
    claim_id: str
    title: str
    subject: str
    path: str
    claim: str
    limits: str
    sources: list[str]
    confidence: str
    evidence_level: str
    information_available_at: str
    project_kb_version: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_subject(subject: str) -> str:
    value = str(subject or "").strip().lower()
    return "b1_v3" if value == "b1" else value


def _vault(vault_path: str | Path | None) -> Path:
    return Path(vault_path).resolve() if vault_path else DEFAULT_VAULT.resolve()


def _parse_note(path: Path) -> tuple[dict[str, Any], str]:
    text = path.read_text(encoding="utf-8")
    match = re.match(r"---\s*\r?\n(.*?)\r?\n---\s*\r?\n", text, flags=re.S)
    if not match:
        return {}, text
    data = yaml.safe_load(match.group(1)) or {}
    return data if isinstance(data, dict) else {}, text[match.end():]


def _section(body: str, heading: str) -> str:
    match = re.search(
        rf"(?ims)^##\s+{re.escape(heading)}\s*$\s*(.*?)(?=^##\s+|\Z)",
        body,
    )
    return match.group(1).strip() if match else ""


def _title(body: str, fallback: str) -> str:
    match = re.search(r"(?m)^#\s+(.+?)\s*$", body)
    return match.group(1).strip() if match else fallback


def _as_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if value is None:
        return []
    text = str(value).strip()
    return [text] if text else []


def _subject_list(frontmatter: dict[str, Any]) -> list[str]:
    value = (
        frontmatter.get("project_subjects")
        or frontmatter.get("project_subject")
        or frontmatter.get("subject")
    )
    return [normalize_subject(item) for item in _as_list(value)]


def _review_passed(value: Any) -> bool:
    return str(value or "").strip().lower() in PASSED_REVIEWS


def _is_validated_claim(frontmatter: dict[str, Any], subject: str) -> bool:
    if str(frontmatter.get("type", "")).strip().lower() != "claim":
        return False
    if str(frontmatter.get("status", "")).strip().lower() not in REVIEWED_NOTE_STATUSES:
        return False
    if str(frontmatter.get("validation_status", "")).strip().lower() not in VALIDATED_STATUSES:
        return False
    if normalize_subject(subject) not in _subject_list(frontmatter):
        return False
    if not _as_list(frontmatter.get("sources")):
        return False
    available_at = str(frontmatter.get("information_available_at", "")).strip()
    if not available_at:
        return False
    if not _review_passed(frontmatter.get("lookahead_review")):
        return False
    if not _review_passed(frontmatter.get("execution_review")):
        return False
    return str(frontmatter.get("confidence", "low")).strip().lower() in {"medium", "high"}


def load_validated_claims(
    subject: str,
    *,
    vault_path: str | Path | None = None,
) -> list[ValidatedClaim]:
    """Load claims that pass the explicit project-consumption contract."""
    normalized = normalize_subject(subject)
    root = _vault(vault_path)
    claims_dir = root / "wiki" / "claims"
    if not claims_dir.is_dir():
        return []
    claims: list[ValidatedClaim] = []
    for path in sorted(claims_dir.rglob("*.md")):
        try:
            frontmatter, body = _parse_note(path)
        except (OSError, UnicodeDecodeError, yaml.YAMLError):
            continue
        if not _is_validated_claim(frontmatter, normalized):
            continue
        claim_text = _section(body, "Claim")
        if not claim_text:
            continue
        relative = str(path.relative_to(root)).replace("\\", "/")
        claims.append(
            ValidatedClaim(
                claim_id=str(frontmatter.get("claim_id") or path.stem),
                title=_title(body, path.stem),
                subject=normalized,
                path=relative,
                claim=claim_text,
                limits=_section(body, "Limits / Failure Cases"),
                sources=_as_list(frontmatter.get("sources")),
                confidence=str(frontmatter.get("confidence", "medium")),
                evidence_level=str(frontmatter.get("evidence_level", "validated")),
                information_available_at=str(frontmatter.get("information_available_at", "")),
                project_kb_version=str(frontmatter.get("project_kb_version", "")),
            )
        )
    return claims


def _query_score(claim: ValidatedClaim, query: str) -> int:
    tokens = {token.lower() for token in re.findall(r"[\w\u4e00-\u9fff]+", query) if len(token) > 1}
    if not tokens:
        return 0
    haystack = f"{claim.title} {claim.claim} {claim.limits}".lower()
    return sum(haystack.count(token) for token in tokens)


def build_validated_claim_context(
    subject: str,
    *,
    query: str = "",
    max_chars: int = 6000,
    vault_path: str | Path | None = None,
) -> str:
    """Build deterministic external-evidence context from validated claims only."""
    normalized = normalize_subject(subject)
    claims = load_validated_claims(normalized, vault_path=vault_path)
    if query:
        claims = sorted(claims, key=lambda item: (-_query_score(item, query), item.claim_id))
    lines = [
        "\n================================================================\n",
        f"  EXTERNAL VALIDATED EVIDENCE: {normalized}\n",
        "================================================================\n",
        "Authority: evidence only. Project hard constraints always take precedence.\n",
    ]
    if not claims:
        lines.append(
            "No KBase claim currently passes the validation contract. Source notes, "
            "concept pages, and hypotheses MUST NOT be treated as strategy rules.\n"
        )
        return "".join(lines)
    for claim in claims:
        block = [
            f"\n### [{claim.claim_id}] {claim.title}\n",
            f"- path: {claim.path}\n",
            f"- confidence: {claim.confidence}; evidence: {claim.evidence_level}\n",
            f"- information_available_at: {claim.information_available_at}\n",
            f"- claim: {claim.claim}\n",
        ]
        if claim.limits:
            block.append(f"- limits: {claim.limits}\n")
        candidate = "".join(lines + block)
        if len(candidate) > max_chars:
            break
        lines.extend(block)
    return "".join(lines)


def build_combined_research_context(
    subject: str,
    *,
    query: str = "",
    project_mode: str = "brief",
    vault_path: str | Path | None = None,
) -> str:
    """Combine machine-enforced project closure with validated external evidence."""
    normalized = normalize_subject(subject)
    project_context = ""
    try:
        from .knowledge_base import build_context, list_subjects

        if normalized in list_subjects():
            project_context = build_context(normalized, mode=project_mode)
    except Exception:
        project_context = ""
    external_context = build_validated_claim_context(
        normalized,
        query=query,
        vault_path=vault_path,
    )
    priority = (
        "KNOWLEDGE PRIORITY ORDER\n"
        "1. Machine-enforced project hard constraints.\n"
        "2. Validated external claims as supporting evidence.\n"
        "3. Unvalidated source notes only through explicit book tools, never as rules.\n\n"
    )
    return priority + project_context + external_context


def _safe_component(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value)).strip("._")
    return cleaned[:100] or "unknown"


def _json_block(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, default=str)


def _metrics_rows(metrics: dict[str, Any], delta: dict[str, Any]) -> list[str]:
    keys = sorted(set(metrics) | set(delta))
    if not keys:
        return ["| none |  |  |"]
    return [f"| `{key}` | {metrics.get(key, '')} | {delta.get(key, '')} |" for key in keys]


def _project_kb_fingerprint(subject: str) -> str:
    try:
        from .knowledge_base import load

        return load(normalize_subject(subject)).fingerprint()
    except Exception:
        return "unregistered"


def _ensure_output_index(root: Path, subject: str, cycle_path: Path) -> Path:
    projects = root / "wiki" / "outputs" / "projects"
    projects.mkdir(parents=True, exist_ok=True)
    index = projects / "index.md"
    if not index.exists():
        index.write_text(
            "---\n"
            "type: project_output_index\n"
            "status: maintained\n"
            "sources: [\"project experiment writeback\"]\n"
            "confidence: high\n"
            "---\n\n"
            "# Project Research Outputs\n\n"
            "Experiment records are evidence objects. They are not durable claims until reviewed.\n\n"
            "## Cycles\n\n",
            encoding="utf-8",
        )
    relative = str(cycle_path.relative_to(root)).replace("\\", "/")
    link = f"- [[{relative[:-3]}|{subject} / {cycle_path.stem}]]"
    text = index.read_text(encoding="utf-8")
    if link not in text:
        index.write_text(text.rstrip() + "\n" + link + "\n", encoding="utf-8")
    return index


def _authorize_kbase_write(
    *,
    lease: TaskExecutionLease | None,
    invocation: ExecutionInvocation | None,
    authority_reader: AuthorityReader | None,
    repository_root: str | Path,
    paths: tuple[Path, ...],
) -> None:
    """Require one immutable KBase-write permit before any filesystem effect."""
    AuthorizedPathMutation(
        authority_reader=authority_reader or AuthorityReader(),
        repository_root=repository_root,
    ).authorize(
        lease,
        invocation,
        operation="KBASE_WRITE",
        effect=SideEffect.WRITE_KBASE,
        module="ag2_research.knowledge_bridge",
        callable_name="write_experiment_output",
        paths=paths,
    )


def write_experiment_output(
    *,
    subject: str,
    cycle_id: str,
    round_n: int,
    entry: dict[str, Any],
    baseline: dict[str, Any] | None = None,
    artifact_paths: Iterable[str | Path] = (),
    vault_path: str | Path | None = None,
    project_root: str | Path | None = None,
    lease: TaskExecutionLease | None = None,
    invocation: ExecutionInvocation | None = None,
    execution_lease: TaskExecutionLease | None = None,
    execution_invocation: ExecutionInvocation | None = None,
    authority_reader: AuthorityReader | None = None,
    repository_root: str | Path | None = None,
) -> Path:
    """Idempotently append one experiment record to a cycle output note."""
    normalized = normalize_subject(subject)
    root = _vault(vault_path)
    cycle_name = _safe_component(cycle_id)
    projects_dir = root / "wiki" / "outputs" / "projects"
    out_dir = projects_dir / normalized
    path = out_dir / f"Cycle {cycle_name}.md"
    index_path = projects_dir / "index.md"
    _authorize_kbase_write(
        lease=lease if lease is not None else execution_lease,
        invocation=invocation if invocation is not None else execution_invocation,
        authority_reader=authority_reader,
        repository_root=repository_root or PROJECT_ROOT,
        paths=(projects_dir, out_dir, path, index_path),
    )
    if not (root / "wiki").is_dir():
        raise FileNotFoundError(f"KBase wiki not found: {root / 'wiki'}")
    out_dir.mkdir(parents=True, exist_ok=True)
    experiment_id = str(entry.get("experiment_id") or "unknown")
    marker = f"<!-- experiment:{_safe_component(experiment_id)} -->"
    artifacts = [str(Path(item)) for item in artifact_paths if str(item).strip()]
    project_path = Path(project_root).resolve() if project_root else PROJECT_ROOT

    if not path.exists():
        frontmatter = {
            "type": "project_output",
            "status": "generated",
            "project": "a-share-quant-selector",
            "project_subject": normalized,
            "cycle_id": str(cycle_id),
            "validation_status": "unreviewed",
            "promotion_status": "output_only",
            "project_kb_version": _project_kb_fingerprint(normalized),
            "created": dt.datetime.now().strftime("%Y-%m-%d"),
            "sources": artifacts or [str(project_path / "research_state" / normalized)],
            "confidence": "medium",
        }
        header = (
            "---\n"
            + yaml.safe_dump(frontmatter, sort_keys=False, allow_unicode=True).strip()
            + "\n---\n\n"
            + f"# {normalized} Research Cycle {cycle_id}\n\n"
            + "> Automated experiment evidence. Review is required before claim promotion.\n\n"
        )
        path.write_text(header, encoding="utf-8")

    existing = path.read_text(encoding="utf-8")
    if marker in existing:
        return path

    metrics = entry.get("metrics") or {}
    delta = entry.get("delta_vs_baseline") or {}
    evidence_claim_ids = _as_list(
        entry.get("evidence_claim_ids") or entry.get("knowledge_claims")
    )
    section = [
        marker,
        f"\n## Experiment {experiment_id}\n",
        f"- Round: `{round_n}`",
        f"- Experiment status: `{entry.get('_experiment_status', 'unknown')}`",
        f"- Promotion status: `{entry.get('promotion_status', 'unknown')}`",
        f"- Recorded at: `{dt.datetime.now(dt.timezone.utc).isoformat()}`",
        f"- Project KB: `{_project_kb_fingerprint(normalized)}`",
        "",
        "### Hypothesis",
        "",
        str(entry.get("hypothesis") or "Not recorded."),
        "",
        "### Scope",
        "",
        "```json",
        _json_block({"params": entry.get("params"), "code_change": entry.get("code_change")}),
        "```",
        "",
        "### Metrics",
        "",
        "| Metric | Actual | Delta vs baseline |",
        "| --- | ---: | ---: |",
        *_metrics_rows(metrics, delta),
        "",
        "### Baseline",
        "",
        "```json",
        _json_block(baseline or {}),
        "```",
        "",
        "### Evidence Chain",
        "",
    ]
    if artifacts:
        section.extend(f"- `{item}`" for item in artifacts)
    else:
        section.append("- No artifact path recorded; review required.")
    section.extend(["", "### External Claims Referenced", ""])
    if evidence_claim_ids:
        section.extend(f"- `{claim_id}`" for claim_id in evidence_claim_ids)
    else:
        section.append("- None recorded. Do not infer that external claims were consulted.")
    section.extend(
        [
            "",
            "### Promotion Decision",
            "",
            "`output_only` - this record cannot update strategy constraints or claims without an independent audit.",
            "",
        ]
    )
    path.write_text(existing.rstrip() + "\n\n" + "\n".join(section), encoding="utf-8")
    _ensure_output_index(root, normalized, path)
    return path
