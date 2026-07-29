"""Candidate-only five-model distillation for locally extracted KBase text."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import random
import re
import tempfile
import time
from typing import Any

from openai import OpenAI

from ag2_research.config import ResearchConfig
from .content_quality import validate_extraction_candidate


PROFILES = {
    "extractor": "glm51",
    "organizer": "doubao",
    "temporal_auditor": "deepseekv4",
    "skeptical_auditor": "minimax_hs",
    "synthesizer": "kimi_hs",
}
FALLBACK_PROFILES = {
    "extractor": ["doubao", "deepseekv4"],
}
FORBIDDEN_MODEL_RE = re.compile(r"(?:^|[-_/])(gpt|openai)(?:$|[-_/])", re.I)
EXTERNAL_UPLOAD_ENV = "KBASE_ALLOW_EXTERNAL_UPLOAD"

PROMPTS = {
    "extractor": """你是保守的原文陈述抽取员。只依据给定的带锚点原文，提取作者明确表达的市场、交易、投资、复盘、资金、情绪、博弈、风险控制、研究方法等陈述。不要提前判断，也不要在这一层判断它是否“直接对 A 股因子有意义”；是否能启发因子价值留给后续AG2。数量由信息密度决定，零条完全合法。禁止把摘要、标题、广告、乱码、常识补全或模型推测当成陈述。每条必须保留逐字证据和原锚点。只返回JSON：{\"items\":[{\"statement\":\"\",\"evidence_quote\":\"\",\"evidence_anchor\":\"\",\"confidence\":0.0}],\"quality_notes\":[]}""",
    "organizer": """你是投资资料整理员。检查原文和抽取结果，合并语义重复项，保留作者主观表达和可追溯证据，不得推导因子、策略规则或客观事实。乱码无法可靠恢复时删除。不得因为内容不是 A 股专属就删除明确的投资/交易陈述。只返回JSON：{\"items\":[{\"statement\":\"\",\"evidence_quote\":\"\",\"evidence_anchor\":\"\",\"confidence\":0.0}],\"removed\":[]}""",
    "temporal_auditor": """你是时序与证据溯源审计员。逐项核对陈述是否被给定原文逐字支持、证据和锚点是否匹配，并区分当时观点、事后回顾、混合、无法确定；通用书籍/方法论允许标记not_applicable。不得用外部知识补全，也不得因为非 A 股直接相关而否决有原文证据的投资/交易陈述。只返回JSON：{\"verdict\":\"pass|revise|reject\",\"temporal_status\":\"contemporaneous|hindsight|mixed|unclear|not_applicable\",\"valid_items\":[],\"issues\":[]}""",
    "skeptical_auditor": """你是反证与质量审计员。删除广告、标题、套话、幸存者偏差式断言、无法辨认的乱码、摘要冒充陈述以及证据不足项。低置信度必须明确标记。不得因为内容不是 A 股专属就否决有原文证据的投资/交易陈述。只返回JSON：{\"verdict\":\"pass|revise|reject\",\"valid_items\":[],\"issues\":[]}""",
    "synthesizer": """你是最终候选合成员。依据原文及四份报告，输出可由确定性门禁验证的候选；不能静默补充证据。陈述数量由可靠内容自然决定，零条合法。claim 必须是明确陈述，evidence_quote 必须逐字来自原文，evidence_anchor 必须来自对应段落。不要输出会覆盖来源包的摘要。只返回JSON：{\"record\":{\"claims\":[{\"claim\":\"\",\"evidence_quote\":\"\",\"evidence_anchor\":\"\",\"confidence\":0.0}]},\"confidence\":0.0,\"limits\":[]}""",
}


def _require_external_upload_authorization() -> None:
    if os.environ.get(EXTERNAL_UPLOAD_ENV) != "1":
        raise RuntimeError(
            f"{EXTERNAL_UPLOAD_ENV}=1 is required before sending KBase source text to external models"
        )


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name, suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


@contextmanager
def _run_lock(run_dir: Path):
    lock = run_dir / ".distillation.lock"
    try:
        fd = os.open(str(lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise RuntimeError(f"distillation run already in progress or lock not cleared: {lock}") from exc
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps({"pid": os.getpid(), "started_at": datetime.now(timezone.utc).isoformat()},
                                    ensure_ascii=False))
        yield
    finally:
        try:
            lock.unlink()
        except FileNotFoundError:
            pass


def _parse_json(text: str) -> dict[str, Any]:
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.I)
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        start, end = text.find("{"), text.rfind("}")
        if start < 0 or end <= start:
            raise ValueError("model response contains no JSON object")
        value = json.loads(text[start:end + 1])
    if not isinstance(value, dict):
        raise ValueError("model response is not a JSON object")
    return value


class VolcanoPanel:
    def __init__(self, config: ResearchConfig | None = None) -> None:
        _require_external_upload_authorization()
        self.config = config or ResearchConfig()
        configured_profiles = set(PROFILES.values())
        for fallbacks in FALLBACK_PROFILES.values():
            configured_profiles.update(fallbacks)
        for profile in configured_profiles:
            entry = self.config.get_llm_config(profile)["config_list"][0]
            if FORBIDDEN_MODEL_RE.search(str(entry.get("model", ""))):
                raise RuntimeError(f"forbidden GPT/OpenAI model in profile {profile}")
            if "volces.com" not in str(entry.get("base_url", "")):
                raise RuntimeError(f"profile {profile} is not routed through Volcengine")

    def call(self, role: str, payload: dict[str, Any], retries: int = 3) -> tuple[dict[str, Any], dict[str, Any]]:
        profiles = [PROFILES[role], *FALLBACK_PROFILES.get(role, [])]
        last: Exception | None = None
        for profile_index, profile in enumerate(profiles):
            cfg = self.config.get_llm_config(profile)
            entry = cfg["config_list"][0]
            timeout = min(float(cfg.get("timeout", 180)), 90)
            client = OpenAI(api_key=entry["api_key"], base_url=entry["base_url"], timeout=timeout)
            attempts_for_profile = 1 if profile_index == 0 and len(profiles) > 1 else retries + 1
            for attempt in range(attempts_for_profile):
                started = time.time()
                try:
                    request: dict[str, Any] = {
                        "model": entry["model"],
                        "temperature": min(float(cfg.get("temperature", .2)), .2),
                        "max_tokens": 6000,
                        "response_format": {"type": "json_object"},
                        "messages": [
                            {"role": "system", "content": PROMPTS[role]},
                            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                        ],
                    }
                    if entry.get("extra_body"):
                        request["extra_body"] = entry["extra_body"]
                    response = client.chat.completions.create(**request)
                    content = response.choices[0].message.content or ""
                    parsed = _parse_json(content)
                    usage = response.usage
                    return parsed, {
                        "role": role,
                        "profile": profile,
                        "model": entry["model"],
                        "attempt": attempt + 1,
                        "elapsed_seconds": round(time.time() - started, 2),
                        "prompt_tokens": getattr(usage, "prompt_tokens", None),
                        "completion_tokens": getattr(usage, "completion_tokens", None),
                        "fallback_for": PROFILES[role] if profile_index else None,
                    }
                except Exception as exc:
                    last = exc
                    if attempt + 1 < attempts_for_profile:
                        time.sleep(min(30, (2 ** attempt) + random.random()))
        raise RuntimeError(f"{role}/{PROFILES[role]} failed after fallbacks {profiles}: {last}")


def _source_payload(
    artifact: dict[str, Any],
    max_chars: int = 120000,
    max_chars_per_chunk: int = 24000,
) -> dict[str, Any]:
    segments = artifact.get("segments", [])
    selected: list[dict[str, str]] = []
    chunks: list[dict[str, Any]] = []
    current: list[dict[str, str]] = []
    current_chars = 0
    used = 0

    def flush_chunk() -> None:
        nonlocal current, current_chars
        if current:
            chunks.append({
                "chunk_index": len(chunks),
                "chars": current_chars,
                "segments": current,
            })
            current = []
            current_chars = 0

    for segment in segments:
        text = str(segment.get("text", "")).strip()
        anchor = str(segment.get("anchor", "")).strip()
        if not text or not anchor:
            continue
        cost = len(text) + len(anchor)
        if selected and used + cost > max_chars:
            break
        item = {"anchor": anchor, "text": text}
        if current and current_chars + cost > max_chars_per_chunk:
            flush_chunk()
        selected.append(item)
        current.append(item)
        current_chars += cost
        used += cost
    flush_chunk()
    eligible_count = len([
        s for s in segments
        if str(s.get("text", "")).strip() and str(s.get("anchor", "")).strip()
    ])
    return {
        "source_id": artifact["source_id"],
        "raw_path": artifact["raw_path"],
        "segments": selected,
        "chunks": chunks,
        "truncated": len(selected) < eligible_count,
        "selection_policy": {
            "max_chars": max_chars,
            "max_chars_per_chunk": max_chars_per_chunk,
            "candidate_only": True,
        },
        "distillation_scope": "extract explicit market/trading/investment statements first; AG2 decides factor relevance later",
    }


def _verify_quotes(candidate: dict[str, Any], source: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    by_anchor = {s["anchor"]: s["text"] for s in source["segments"]}
    for index, item in enumerate(candidate.get("record", {}).get("claims", [])):
        anchor = str(item.get("evidence_anchor", ""))
        quote = str(item.get("evidence_quote", ""))
        if "," in anchor or "，" in anchor:
            errors.append(f"claims[{index}] anchor must be a single extracted source anchor")
        elif anchor not in by_anchor:
            errors.append(f"claims[{index}] anchor not found in extracted source")
        elif quote not in by_anchor[anchor]:
            errors.append(f"claims[{index}] quote not found at declared anchor")
    return errors


RELEVANCE_ONLY_RE = re.compile(r"(非A股|不是A股|A股.*不相关|直接相关|适用A股|因子|选股)", re.I)
BLOCKING_RE = re.compile(r"(证据|锚点|原文|逐字|乱码|广告|标题|摘要|低置信|无法辨认|不支持|hallucinat|fabricat)", re.I)


def _auditor_failures(reports: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    for role in ("temporal_auditor", "skeptical_auditor"):
        report = reports.get(role, {})
        verdict = str(report.get("verdict", "")).lower()
        blocking = report.get("blocking_issues", [])
        if isinstance(blocking, str):
            blocking = [blocking]
        blocking_text = " ".join(str(item) for item in blocking)
        issues = report.get("issues", [])
        if isinstance(issues, str):
            issues = [issues]
        issue_text = " ".join(str(item) for item in issues)

        if blocking:
            failures.append(f"{role} blocking issues: {blocking_text[:500]}")
        elif verdict not in {"", "pass"}:
            if issue_text and not BLOCKING_RE.search(issue_text):
                continue
            failures.append(f"{role} verdict is {verdict or 'missing'}, not pass")
    return failures


def _final_gate(candidate: dict[str, Any], source: dict[str, Any], reports: dict[str, Any]) -> dict[str, Any]:
    """Combine deterministic evidence checks with fail-closed auditor verdicts."""
    errors = _verify_quotes(candidate, source)
    gate = validate_extraction_candidate(candidate)
    errors.extend(gate.get("errors", []))
    auditor_failures = _auditor_failures(reports)
    if errors:
        return {"decision": "reject", "errors": errors + auditor_failures,
                "warnings": gate.get("warnings", [])}
    if auditor_failures:
        return {"decision": "review", "errors": [],
                "warnings": auditor_failures + gate.get("warnings", [])}
    return gate


def process_artifact(artifact_path: Path, run_dir: Path, panel: VolcanoPanel) -> dict[str, Any]:
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    source = _source_payload(artifact)
    source_id = source["source_id"]
    checkpoint = run_dir / "checkpoints" / f"{source_id}.json"
    state = json.loads(checkpoint.read_text(encoding="utf-8")) if checkpoint.exists() else {
        "source_id": source_id,
        "raw_path": source["raw_path"],
        "artifact_path": str(artifact_path),
        "status": "in_flight",
        "reports": {},
        "usage": [],
        "errors": {},
    }
    _atomic_json(checkpoint, state)
    context: dict[str, Any] = {"source": source}
    for role in ("extractor", "organizer", "temporal_auditor", "skeptical_auditor", "synthesizer"):
        if role in state["reports"]:
            context[role] = state["reports"][role]
            continue
        try:
            report, meta = panel.call(role, context)
            state["reports"][role] = report
            state["usage"].append(meta)
            context[role] = report
            _atomic_json(checkpoint, state)
        except Exception as exc:
            state["status"] = "failed"
            state["errors"][role] = str(exc)[:2000]
            _atomic_json(checkpoint, state)
            return state
    candidate = dict(state["reports"]["synthesizer"])
    candidate["source_id"] = source_id
    candidate["raw_path"] = source["raw_path"]
    candidate.setdefault("record", {}).pop("summary", None)
    gate = _final_gate(candidate, source, state["reports"])
    state["quality_gate"] = {**gate, "publication_eligible": gate["decision"] == "accept"}
    claims = candidate.get("record", {}).get("claims", [])
    if isinstance(claims, list) and not claims:
        state["status"] = "no_usable_content"
        state["quality_gate"]["publication_eligible"] = False
        state["quality_gate"]["content_outcome"] = "valid_zero_statements"
    else:
        state["status"] = "accepted" if gate["decision"] == "accept" else gate["decision"]
    target = run_dir / ("accepted" if gate["decision"] == "accept" else "quarantine") / f"{source_id}.candidate.json"
    _atomic_json(target, {
        "candidate_schema_version": 1,
        "candidate": candidate,
        "quality_gate": state["quality_gate"],
        "policy": {
            "candidate_only": True,
            "raw_modified": False,
            "packet_modified": False,
            "catalog_modified": False,
            "external_models_called": True,
            "models": [PROFILES[x] for x in PROFILES],
            "fallback_profiles": FALLBACK_PROFILES,
        },
    })
    state["candidate_path"] = str(target)
    state["completed_at"] = datetime.now(timezone.utc).isoformat()
    _atomic_json(checkpoint, state)
    return state


def _is_terminal(status: str) -> bool:
    return status in {"accepted", "review", "reject", "no_usable_content"}


def run(*, extraction_run: str | Path, output_dir: str | Path | None = None,
        limit: int | None = None, artifact: str | Path | None = None,
        retry_failed: bool = False) -> dict[str, Any]:
    source_root = Path(extraction_run).resolve()
    if artifact:
        artifacts = [Path(artifact).resolve()]
    else:
        artifacts = sorted((source_root / "artifacts").glob("*.extracted.json"))
    run_dir = Path(output_dir).resolve() if output_dir else source_root / "distillation-five-model-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    with _run_lock(run_dir):
        panel = VolcanoPanel()
        results: list[dict[str, Any]] = []
        for artifact_path in artifacts:
            checkpoint = run_dir / "checkpoints" / f"{artifact_path.name.split('.')[0]}.json"
            if checkpoint.exists():
                status = json.loads(checkpoint.read_text(encoding="utf-8")).get("status")
                if _is_terminal(str(status)) or (status == "failed" and not retry_failed):
                    continue
            results.append(process_artifact(artifact_path, run_dir, panel))
            if limit is not None and len(results) >= limit:
                break
        counts: dict[str, int] = {}
        for path in (run_dir / "checkpoints").glob("*.json"):
            status = json.loads(path.read_text(encoding="utf-8")).get("status", "unknown")
            counts[status] = counts.get(status, 0) + 1
        summary = {
            "run_schema_version": 1,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source_artifacts": len(artifacts),
            "attempted_this_run": len(results),
            "counts": counts,
            "profiles": PROFILES,
            "fallback_profiles": FALLBACK_PROFILES,
            "policy": {"candidate_only": True, "gpt_forbidden": True, "accepted_only_publishable": True},
            "run_dir": str(run_dir),
        }
        _atomic_json(run_dir / "summary.json", summary)
        return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Distill anchored KBase text with five Volcengine models")
    parser.add_argument("extraction_run")
    parser.add_argument("--output-dir")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--artifact")
    parser.add_argument("--retry-failed", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(
        extraction_run=args.extraction_run,
        output_dir=args.output_dir,
        limit=args.limit,
        artifact=args.artifact,
        retry_failed=args.retry_failed,
    ), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
