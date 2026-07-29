"""Five-model Volcengine pipeline for dated market-review event cards.

This module is deliberately separate from strategy and backtest code. It reads
immutable KBase source packets and writes output-only research artifacts.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from openai import OpenAI

from .config import ResearchConfig


PROJECT_ROOT = Path(__file__).resolve().parent.parent
KBASE = Path(r"D:\KBase")
FAMILY_PAGE = KBASE / "wiki" / "sources" / "families" / "北京炒家.md"
PACKET_DIR = (
    KBASE
    / "raw"
    / "imports"
    / "baidu-new-folder-20260628"
    / "distillation"
    / "source-packets"
)
OUTPUT_ROOT = KBASE / "wiki" / "outputs" / "event-cards" / "北京炒家"

PANEL = {
    "temporal_auditor": "deepseekv4",
    "source_extractor": "glm51",
    "context_organizer": "doubao",
    "skeptical_auditor": "minimax_hs",
}
SYNTHESIZER_PROFILE = "kimi_hs"


ROLE_PROMPTS = {
    "temporal_auditor": """You are a temporal provenance auditor. Analyze one dated post.
Return JSON only. Identify publication/event date, information-available time,
whether statements are contemporaneous or hindsight, and any lookahead risk.
Never infer facts not present in the supplied packet. Schema:
{"event_date":"YYYY-MM-DD or null","information_available_at":"...","post_type":"post_close_review|intraday_note|retrospective|mixed|unclear","contemporaneous_observations":[],"hindsight_elements":[],"lookahead_risks":[],"confidence":"low|medium|high","evidence":[]}.""",
    "source_extractor": """You are a conservative source extractor. Return JSON only.
Extract every materially distinct statement the author directly makes about
that day's market, sectors, stocks, trades, position/risk and next-session
expectation. Let evidence density determine the natural count: zero is valid,
and long high-quality sources may contain many items. Do not split one idea
into near-duplicates or fill a quota. Preserve uncertainty and evidence anchors.
Do not add market facts from memory. Schema:
{"market_observations":[],"sector_observations":[],"stock_observations":[],"author_actions":[],"position_risk":[],"next_session_expectations":[],"failure_conditions":[],"evidence":[],"missing_context":[]}.""",
    "context_organizer": """You organize one A-share author's dated source material.
Return JSON only. Preserve the author's own vocabulary and arrange only what the
source explicitly says about emotion, market condition, breadth/liquidity,
leadership, sectors and trading context. Do not infer an objective regime, propose
a factor, create a hypothesis, or judge usefulness for a selector. Zero items is
valid. Schema: {"author_emotion_terms":[],"author_market_terms":[],"breadth_liquidity_statements":[],"leadership_statements":[],"sector_context":[],"trading_context":[],"ambiguities":[],"confidence":"low|medium|high","evidence":[]}.""",
    "skeptical_auditor": """You are a skeptical evidence auditor. Return JSON only.
Find attribution problems, marketing language, survivorship/hindsight bias,
missing context, claims that cannot be quantified, near-duplicate candidate
ideas, and facts requiring chart or market-data reconstruction. Judge content
quality rather than length. Schema: {"attribution_issues":[],"bias_risks":[],"unsupported_inferences_to_avoid":[],"near_duplicate_ideas":[],"needs_market_reconstruction":[],"needs_visual_review":[],"usable_as_is":[],"confidence":"low|medium|high"}.""",
}

SYNTHESIS_PROMPT = """You are the final historian for a five-model source event-card pipeline.
Using the source packet plus four role reports, produce one conservative JSON
event card for progressive-disclosure browsing. Do not repair missing evidence
silently. Preserve subjective observations as the author's statements rather
than objective facts. Do not generate hypotheses, factors, observable proxies,
V5 mappings, research queues, selector recommendations, or strategy rules.

Quantity policy:
- There is no minimum and no maximum number of source statements.
- Zero items in a section is correct when the source contains no supported content.
- Long high-information sources may contain many distinct items.
- Never split one idea to increase count and merge semantic duplicates.

Required schema:
{
  "schema_version": 3,
  "event_id": "beijing-chaogu-YYYY-MM-DD-SHA8",
  "author": "北京炒家",
  "event_date": "YYYY-MM-DD",
  "information_available_at": "after_close|intraday|unclear",
  "source_path": "...",
  "source_packet": "...",
  "post_type": "...",
  "source_says": {"market":[],"sectors":[],"stocks":[],"actions":[],"position_risk":[],"next_session_expectations":[]},
  "author_language": {"emotion_terms":[],"market_terms":[],"leadership_terms":[],"liquidity_breadth_terms":[]},
  "chronology": {"prior_context":[],"current_session":[],"next_session_statements":[]},
  "evidence": [],
  "limits": [],
  "lookahead_review": "passed|blocked|needs_review",
  "visual_review": "not_required|pending|required",
  "confidence": "low|medium|high",
  "promotion_status": "output_only",
  "validation_status": "unreviewed"
}
Return JSON only."""


@dataclass(frozen=True)
class SourceItem:
    sha256: str
    event_date: str
    packet_path: Path
    payload: dict[str, Any]


def _extract_date(text: str) -> str | None:
    match = re.search(r"(?<!\d)(20\d{2})[-_/年.](\d{1,2})[-_/月.](\d{1,2})", text)
    if not match:
        return None
    try:
        return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
    except ValueError:
        return None


def load_family_items() -> list[SourceItem]:
    page = FAMILY_PAGE.read_text(encoding="utf-8")
    marker = "## 全部家族成员"
    if marker not in page:
        raise RuntimeError(f"Member marker missing in {FAMILY_PAGE}")
    member_text = page.split(marker, 1)[1]
    shas = sorted(set(re.findall(r"`([0-9a-f]{64})`", member_text)))
    items: list[SourceItem] = []
    for sha in shas:
        packet_path = PACKET_DIR / f"{sha}.json"
        if not packet_path.exists():
            continue
        packet = json.loads(packet_path.read_text(encoding="utf-8"))
        blob = " ".join(
            str(x or "")
            for x in (
                packet.get("original_path"),
                packet.get("record", {}).get("canonical_title"),
                packet.get("record", {}).get("family_key"),
            )
        )
        event_date = _extract_date(blob)
        if event_date:
            items.append(SourceItem(sha, event_date, packet_path, packet))
    return sorted(items, key=lambda x: (x.event_date, x.sha256))


def stratified_sample(items: list[SourceItem], limit: int) -> list[SourceItem]:
    if limit <= 0 or limit >= len(items):
        return items
    if limit == 1:
        return [items[len(items) // 2]]
    indices = {round(i * (len(items) - 1) / (limit - 1)) for i in range(limit)}
    return [items[i] for i in sorted(indices)]


def source_view(item: SourceItem) -> dict[str, Any]:
    record = item.payload.get("record", {})
    allowed = (
        "canonical_title",
        "title_basis",
        "source_role",
        "primary_people",
        "topics",
        "summary",
        "methods",
        "claims",
        "risks",
        "contradictions",
        "visual_gaps",
        "advertising",
        "reliability",
        "reliability_reasons",
        "family_key",
        "review_flags",
    )
    return {
        "sha256": item.sha256,
        "event_date_from_path": item.event_date,
        "original_path": item.payload.get("original_path"),
        "source_packet": str(item.packet_path),
        "record": {key: record.get(key) for key in allowed if key in record},
    }


def _message_text(message: Any) -> str:
    content = getattr(message, "content", None)
    if content:
        return content
    reasoning = getattr(message, "reasoning_content", None)
    if reasoning:
        return reasoning
    if hasattr(message, "model_dump"):
        dumped = message.model_dump()
        return dumped.get("content") or dumped.get("reasoning_content") or ""
    return ""


def _parse_json(text: str) -> dict[str, Any]:
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.I)
    text = re.sub(r"\s*```$", "", text)
    try:
        value = json.loads(text)
        if isinstance(value, dict):
            return value
    except json.JSONDecodeError:
        pass
    start, end = text.find("{"), text.rfind("}")
    if start >= 0 and end > start:
        value = json.loads(text[start : end + 1])
        if isinstance(value, dict):
            return value
    raise ValueError("Model response did not contain a JSON object")


class PanelClient:
    def __init__(self, config: ResearchConfig) -> None:
        self.config = config

    def call(
        self,
        profile: str,
        system: str,
        payload: dict[str, Any],
        retries: int = 2,
        max_tokens: int = 4000,
        json_mode: bool = False,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        llm = self.config.get_llm_config(profile)
        entry = llm["config_list"][0]
        client = OpenAI(api_key=entry["api_key"], base_url=entry["base_url"], timeout=llm.get("timeout", 180))
        last_error: Exception | None = None
        for attempt in range(retries + 1):
            started = time.time()
            try:
                request: dict[str, Any] = {
                    "model": entry["model"],
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                    ],
                    "temperature": llm.get("temperature", 0.2),
                    "max_tokens": max_tokens,
                }
                if json_mode:
                    request["response_format"] = {"type": "json_object"}
                response = client.chat.completions.create(
                    **request,
                )
                text = _message_text(response.choices[0].message)
                parsed = _parse_json(text)
                usage = getattr(response, "usage", None)
                meta = {
                    "profile": profile,
                    "model": entry["model"],
                    "elapsed_seconds": round(time.time() - started, 2),
                    "prompt_tokens": getattr(usage, "prompt_tokens", None),
                    "completion_tokens": getattr(usage, "completion_tokens", None),
                    "attempt": attempt + 1,
                }
                return parsed, meta
            except Exception as exc:  # provider errors vary by OpenAI-compatible gateway
                last_error = exc
                if attempt < retries:
                    time.sleep(2**attempt)
        raise RuntimeError(f"{profile} failed after {retries + 1} attempts: {last_error}")


def process_item(
    item: SourceItem,
    run_dir: Path,
    panel: PanelClient,
    retry_errors: bool = False,
) -> dict[str, Any]:
    checkpoint = run_dir / "checkpoints" / f"{item.event_date}_{item.sha256[:8]}.json"
    if checkpoint.exists():
        existing = json.loads(checkpoint.read_text(encoding="utf-8"))
        if not retry_errors or "synthesizer" not in existing.get("role_errors", {}):
            return existing
        source = existing["source"]
        role_reports = existing.get("role_reports", {})
        errors = {k: v for k, v in existing.get("role_errors", {}).items() if k != "synthesizer"}
        usage = existing.get("usage", [])
        synthesis_input = {"source": source, "role_reports": role_reports, "role_errors": errors}
        card, meta = panel.call(
            SYNTHESIZER_PROFILE,
            SYNTHESIS_PROMPT,
            synthesis_input,
            max_tokens=8000,
            json_mode=True,
        )
        usage.append(meta)
        card["event_id"] = f"beijing-chaogu-{item.event_date}-{item.sha256[:8]}"
        card["author"] = "北京炒家"
        card["event_date"] = item.event_date
        card["source_path"] = source.get("original_path")
        card["source_packet"] = str(item.packet_path)
        card["promotion_status"] = "output_only"
        card.setdefault("validation_status", "unreviewed")
        existing.update({"role_errors": errors, "event_card": card, "usage": usage})
        checkpoint.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
        return existing
    source = source_view(item)
    role_reports: dict[str, Any] = {}
    usage: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(panel.call, profile, ROLE_PROMPTS[role], source): role
            for role, profile in PANEL.items()
        }
        for future in as_completed(futures):
            role = futures[future]
            try:
                report, meta = future.result()
                role_reports[role] = report
                usage.append(meta)
            except Exception as exc:
                errors[role] = str(exc)
    synthesis_input = {"source": source, "role_reports": role_reports, "role_errors": errors}
    try:
        card, meta = panel.call(
            SYNTHESIZER_PROFILE,
            SYNTHESIS_PROMPT,
            synthesis_input,
            max_tokens=8000,
            json_mode=True,
        )
        usage.append(meta)
    except Exception as exc:
        card = {
            "schema_version": 3,
            "event_id": f"beijing-chaogu-{item.event_date}-{item.sha256[:8]}",
            "author": "北京炒家",
            "event_date": item.event_date,
            "source_path": source.get("original_path"),
            "source_packet": str(item.packet_path),
            "promotion_status": "output_only",
            "validation_status": "blocked",
            "confidence": "low",
            "limits": [f"synthesis_failed: {exc}"],
        }
        errors["synthesizer"] = str(exc)
    card["event_id"] = f"beijing-chaogu-{item.event_date}-{item.sha256[:8]}"
    card["author"] = "北京炒家"
    card["event_date"] = item.event_date
    card["source_path"] = source.get("original_path")
    card["source_packet"] = str(item.packet_path)
    card["promotion_status"] = "output_only"
    card.setdefault("validation_status", "unreviewed")
    result = {
        "source": source,
        "role_reports": role_reports,
        "role_errors": errors,
        "event_card": card,
        "usage": usage,
    }
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def build_summary(run_dir: Path, results: list[dict[str, Any]], selected: list[SourceItem]) -> dict[str, Any]:
    cards = [x["event_card"] for x in results]
    calls = [meta for x in results for meta in x.get("usage", [])]
    profile_counts: dict[str, int] = {}
    for meta in calls:
        profile_counts[meta["profile"]] = profile_counts.get(meta["profile"], 0) + 1
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "pilot_completed",
        "source_family": "北京炒家",
        "selection": "chronologically_stratified",
        "selected": len(selected),
        "completed": len(results),
        "event_date_min": min(x.event_date for x in selected),
        "event_date_max": max(x.event_date for x in selected),
        "cards_with_errors": sum(bool(x.get("role_errors")) for x in results),
        "profile_call_counts": profile_counts,
        "models": sorted({meta.get("model") for meta in calls if meta.get("model")}),
        "promotion_status": "output_only",
        "validation_status": "unreviewed",
    }
    (run_dir / "event_cards.json").write_text(json.dumps(cards, ensure_ascii=False, indent=2), encoding="utf-8")
    (run_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Build dated market-review event cards with five Volcengine models")
    parser.add_argument("--limit", type=int, default=20, help="Pilot size; 0 means all dated family members")
    parser.add_argument("--run-id", default="source-event-cards-v1", help="Stable directory name; reruns resume checkpoints")
    parser.add_argument("--retry-errors", action="store_true", help="Retry only failed synthesis checkpoints")
    args = parser.parse_args()

    config = ResearchConfig()
    required = set(PANEL.values()) | {SYNTHESIZER_PROFILE}
    missing = [name for name in required if name not in config.profiles]
    if missing:
        raise RuntimeError(f"Missing LLM profiles: {missing}")

    items = load_family_items()
    selected = stratified_sample(items, args.limit)
    if not selected:
        raise RuntimeError("No dated source packets found")
    run_dir = OUTPUT_ROOT / args.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    panel = PanelClient(config)
    results: list[dict[str, Any]] = []
    for index, item in enumerate(selected, start=1):
        print(f"[{index}/{len(selected)}] {item.event_date} {item.sha256[:8]}", flush=True)
        results.append(process_item(item, run_dir, panel, retry_errors=args.retry_errors))
    summary = build_summary(run_dir, results, selected)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
