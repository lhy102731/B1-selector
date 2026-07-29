"""Run the fixed P0 query set against the legacy book_search baseline."""
from __future__ import annotations

import argparse
import functools
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any

import yaml

from ag2_research.tools import book_search
from ag2_research.kbase.tools import kbase_search, kbase_trace


DEFAULT_CASES = Path(__file__).with_name("query_regression.yaml")


def _telemetry_disabled(function):
    """Keep synthetic regression traffic out of production usage telemetry."""
    @functools.wraps(function)
    def wrapped(*args, **kwargs):
        previous = os.environ.get("KBASE_TELEMETRY_DISABLED")
        os.environ["KBASE_TELEMETRY_DISABLED"] = "1"
        try:
            return function(*args, **kwargs)
        finally:
            if previous is None:
                os.environ.pop("KBASE_TELEMETRY_DISABLED", None)
            else:
                os.environ["KBASE_TELEMETRY_DISABLED"] = previous
    return wrapped


def load_query_suite(path: str | Path = DEFAULT_CASES) -> dict[str, Any]:
    suite = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(suite, dict) or not isinstance(suite.get("cases"), list):
        raise ValueError("query regression file must contain a cases list")
    return suite


def _positive_hit(expected: dict[str, Any], result_text: str) -> tuple[bool, int]:
    source_ids = [str(item).lower() for item in expected.get("source_ids", [])]
    matched_sources = sum(source_id in result_text for source_id in source_ids)
    tokens = source_ids + [str(item).lower() for item in expected.get("family_ids", [])]
    tokens += [str(item).lower() for item in expected.get("title_contains", [])]
    minimum = int(expected.get("minimum_distinct_sources", 1))
    if minimum > 1:
        return matched_sources >= minimum, matched_sources
    return any(token and token in result_text for token in tokens), matched_sources


def evaluate_case(case: dict[str, Any], payload: dict[str, Any], forbidden_scopes: list[str]) -> dict[str, Any]:
    results = payload.get("results", []) if isinstance(payload, dict) else []
    text = json.dumps(results, ensure_ascii=False).lower()
    expected = case["expected"]
    pollution = []
    top_paths = []
    for item in results:
        paths = item.get("paths") if isinstance(item.get("paths"), dict) else {}
        relative_paths = [str(item.get("relative_path", ""))] if item.get("relative_path") else []
        relative_paths.extend(str(value) for value in paths.values())
        relative_paths = [value.replace("\\", "/") for value in relative_paths if value]
        top_paths.extend(relative_paths[:1])
        for relative in relative_paths:
            if any(scope.lower() in relative.lower() for scope in forbidden_scopes):
                pollution.append(relative)

    if expected.get("no_result"):
        hit = not results
        matched_sources = 0
    elif expected.get("requires_refinement"):
        hit = bool(payload.get("requires_refinement"))
        matched_sources = 0
    elif expected.get("exclude_outputs"):
        hit = not pollution
        matched_sources = 0
    else:
        hit, matched_sources = _positive_hit(expected, text)

    return {
        "id": case["id"],
        "intent": case["intent"],
        "hit": hit,
        "result_count": len(results),
        "matched_expected_sources": matched_sources,
        "pollution_paths": pollution,
        "top_paths": top_paths,
        "error": payload.get("error") if isinstance(payload, dict) else "invalid payload",
    }


def run_legacy_baseline(
    suite: dict[str, Any], *, vault_path: str | None = None
) -> dict[str, Any]:
    forbidden = [str(item) for item in suite.get("forbidden_scopes", [])]
    outcomes = []
    for case in suite["cases"]:
        raw = book_search(
            case["query"],
            max_results=int(suite.get("max_results", 5)),
            scope="all",
            vault_path=vault_path,
        )
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"error": "book_search returned invalid JSON", "results": []}
        outcome = evaluate_case(case, payload, forbidden)
        result_ids = [str(item.get("source_id")) for item in payload.get("results", []) if item.get("source_id")]
        target_ids = [str(item) for item in case["expected"].get("source_ids", [])]
        traced_ids = [source_id for source_id in result_ids if source_id in target_ids]
        if case["intent"] != "negative" and outcome["hit"]:
            if not traced_ids and result_ids:
                traced_ids = result_ids[:1]
            trace_results = []
            for source_id in traced_ids:
                try:
                    trace_payload = json.loads(kbase_trace(source_id, vault_path=vault_path))
                    trace_results.append(
                        not trace_payload.get("error")
                        and bool(trace_payload.get("paths"))
                        and (
                            not case.get("evidence_layer_required")
                            or "evidence" in trace_payload.get("available_layers", [])
                        )
                    )
                except json.JSONDecodeError:
                    trace_results.append(False)
            outcome["trace_success"] = bool(trace_results) and all(trace_results)
        else:
            outcome["trace_success"] = None
        families = {
            str(item.get("family_id") or item.get("source_id"))
            for item in payload.get("results", [])
            if item.get("source_id")
        }
        outcome["source_diversity"] = len(families) / max(1, len(payload.get("results", [])))
        outcomes.append(outcome)

    positives = [item for item in outcomes if item["intent"] != "negative"]
    result_slots = sum(item["result_count"] for item in outcomes)
    polluted_slots = sum(len(item["pollution_paths"]) for item in outcomes)
    by_intent = {}
    for intent in sorted({item["intent"] for item in outcomes}):
        group = [item for item in outcomes if item["intent"] == intent]
        by_intent[intent] = sum(item["hit"] for item in group) / len(group)

    return {
        "baseline": "legacy_book_search",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "query_count": len(outcomes),
        "metrics": {
            "known_item_recall_at_5": (
                sum(item["hit"] for item in positives) / len(positives) if positives else 0.0
            ),
            "output_pollution": polluted_slots / result_slots if result_slots else 0.0,
            "error_rate": sum(bool(item["error"]) for item in outcomes) / len(outcomes),
            "trace_success": (
                sum(bool(item["trace_success"]) for item in positives) / len(positives) if positives else 0.0
            ),
            "source_diversity_at_5": (
                sum(item["source_diversity"] for item in positives) / len(positives) if positives else 0.0
            ),
            "by_intent": by_intent,
        },
        "cases": outcomes,
    }


@_telemetry_disabled
def run_catalog_regression(
    suite: dict[str, Any], *, vault_path: str | None = None
) -> dict[str, Any]:
    forbidden = [str(item) for item in suite.get("forbidden_scopes", [])]
    outcomes = []
    for case in suite["cases"]:
        raw = kbase_search(
            case["query"],
            max_results=int(suite.get("max_results", 5)),
            scope="sources",
            vault_path=vault_path,
        )
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"error": "kbase_search returned invalid JSON", "results": []}
        outcome = evaluate_case(case, payload, forbidden)
        result_ids = [str(item.get("source_id")) for item in payload.get("results", []) if item.get("source_id")]
        target_ids = [str(item) for item in case["expected"].get("source_ids", [])]
        traced_ids = [source_id for source_id in result_ids if source_id in target_ids]
        if case["intent"] != "negative" and outcome["hit"]:
            if not traced_ids and result_ids:
                traced_ids = result_ids[:1]
            trace_results = []
            for source_id in traced_ids:
                try:
                    trace_payload = json.loads(kbase_trace(source_id, vault_path=vault_path))
                    trace_results.append(
                        not trace_payload.get("error")
                        and bool(trace_payload.get("paths"))
                        and (
                            not case.get("evidence_layer_required")
                            or "evidence" in trace_payload.get("available_layers", [])
                        )
                    )
                except json.JSONDecodeError:
                    trace_results.append(False)
            outcome["trace_success"] = bool(trace_results) and all(trace_results)
        else:
            outcome["trace_success"] = None
        families = {
            str(item.get("family_id") or item.get("source_id"))
            for item in payload.get("results", [])
            if item.get("source_id")
        }
        outcome["source_diversity"] = len(families) / max(1, len(payload.get("results", [])))
        outcomes.append(outcome)

    positives = [item for item in outcomes if item["intent"] != "negative"]
    result_slots = sum(item["result_count"] for item in outcomes)
    polluted_slots = sum(len(item["pollution_paths"]) for item in outcomes)
    by_intent = {}
    for intent in sorted({item["intent"] for item in outcomes}):
        group = [item for item in outcomes if item["intent"] == intent]
        by_intent[intent] = sum(item["hit"] for item in group) / len(group)
    return {
        "baseline": "catalog_kbase_search",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "query_count": len(outcomes),
        "metrics": {
            "known_item_recall_at_5": (
                sum(item["hit"] for item in positives) / len(positives) if positives else 0.0
            ),
            "output_pollution": polluted_slots / result_slots if result_slots else 0.0,
            "error_rate": sum(bool(item["error"]) for item in outcomes) / len(outcomes),
            "trace_success": (
                sum(bool(item["trace_success"]) for item in positives) / len(positives) if positives else 0.0
            ),
            "source_diversity_at_5": (
                sum(item["source_diversity"] for item in positives) / len(positives) if positives else 0.0
            ),
            "by_intent": by_intent,
        },
        "cases": outcomes,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", default=str(DEFAULT_CASES))
    parser.add_argument("--vault-path")
    parser.add_argument("--output")
    parser.add_argument("--engine", choices=("legacy", "catalog"), default="legacy")
    args = parser.parse_args()
    suite = load_query_suite(args.cases)
    previous_telemetry = os.environ.get("KBASE_TELEMETRY_DISABLED")
    if args.engine == "catalog":
        os.environ["KBASE_TELEMETRY_DISABLED"] = "1"
    try:
        report = (
            run_catalog_regression(suite, vault_path=args.vault_path)
            if args.engine == "catalog"
            else run_legacy_baseline(suite, vault_path=args.vault_path)
        )
    finally:
        if args.engine == "catalog":
            if previous_telemetry is None:
                os.environ.pop("KBASE_TELEMETRY_DISABLED", None)
            else:
                os.environ["KBASE_TELEMETRY_DISABLED"] = previous_telemetry
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
    else:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        print(rendered)


if __name__ == "__main__":
    main()
