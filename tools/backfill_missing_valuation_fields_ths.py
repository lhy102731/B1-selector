"""Backfill missing PE(TTM), PB and PS(TTM) values from THS Wencai.

Queries are exact code/date market-ratio fields and never forward-fill.  The
repository's ``pcf`` column is intentionally excluded: BaoStock ``pcfNcfTTM``
uses net cash flow, while the available THS Wencai PCF is explicitly based on
operating cash flow and is not the same data contract.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import shutil
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backfill_missing_amount_ths import batched, chinese_date, query_with_retry
from tools.backfill_valuation_fields import (
    _atomic_csv,
    _atomic_json,
    _date_keys,
    _read_csv,
    _update_manifest,
    code_path,
    stock_files,
    valid_valuation,
)
from tools.backfill_missing_bars_ths import invalidate_caches


FIELD_TERMS = {
    "pe_dynamic": "市盈率ttm",
    "pb": "市净率",
    "ps": "市销率ttm",
}
FIELD_PREFIXES = {
    "pe_dynamic": "市盈率(pe,ttm)[",
    "pb": "市净率(pb)[",
    "ps": "市销率(ps,ttm)[",
}


def identify_targets(
    data_dir: Path,
    fields: tuple[str, ...],
) -> set[tuple[str, str, str]]:
    targets: set[tuple[str, str, str]] = set()
    for path in stock_files(data_dir):
        frame = _read_csv(path)
        if frame.empty or "date" not in frame.columns:
            continue
        dates = _date_keys(frame["date"])
        for field in fields:
            values = frame.get(field, pd.Series(pd.NA, index=frame.index))
            missing = dates.notna() & ~valid_valuation(values, field)
            targets.update(
                (path.stem, str(day), field) for day in dates.loc[missing].unique()
            )
    return targets


def parse_wencai_valuations(
    frame: pd.DataFrame,
    *,
    allowed: set[tuple[str, str, str]],
) -> dict[tuple[str, str, str], float]:
    if frame is None or frame.empty:
        return {}
    code_column = next(
        (column for column in ("股票代码", "代码", "证券代码") if column in frame.columns),
        None,
    )
    if code_column is None:
        return {}
    mapped: dict[tuple[str, str], Any] = {}
    for column in frame.columns:
        text = str(column)
        for field, prefix in FIELD_PREFIXES.items():
            if not text.startswith(prefix) or not text.endswith("]"):
                continue
            compact = text[text.rfind("[") + 1 : -1]
            parsed = pd.to_datetime(compact, format="%Y%m%d", errors="coerce")
            if pd.notna(parsed):
                mapped[(field, pd.Timestamp(parsed).strftime("%Y-%m-%d"))] = column
    output: dict[tuple[str, str, str], float] = {}
    for _, row in frame.iterrows():
        code = str(row[code_column]).strip()[:6].zfill(6)
        if not code.isdigit():
            continue
        for (field, day), column in mapped.items():
            key = (code, day, field)
            if key not in allowed:
                continue
            value = pd.to_numeric(pd.Series([row[column]]), errors="coerce").iloc[0]
            if (
                pd.notna(value)
                and math.isfinite(float(value))
                and float(value) != 0
                and abs(float(value)) < 1_000_000
            ):
                output[key] = float(value)
    return output


def query_plan(
    targets: set[tuple[str, str, str]],
    *,
    wide_date_min_codes: int,
    token_batch_size: int,
) -> dict[str, Any]:
    by_date: dict[str, set[str]] = defaultdict(set)
    for code, day, _ in targets:
        by_date[day].add(code)
    wide_dates = {
        day for day, codes in by_date.items() if len(codes) >= wide_date_min_codes
    }
    wide_covered = {key for key in targets if key[1] in wide_dates}
    remaining = targets - wide_covered
    by_code: dict[str, int] = defaultdict(int)
    for code, _, _ in remaining:
        by_code[code] += 1
    code_queries = sum(math.ceil(count / token_batch_size) for count in by_code.values())
    all_by_code: dict[str, int] = defaultdict(int)
    for code, _, _ in targets:
        all_by_code[code] += 1
    exact_queries = sum(
        math.ceil(count / token_batch_size) for count in all_by_code.values()
    )
    return {
        "target_values": len(targets),
        "target_code_dates": len({(code, day) for code, day, _ in targets}),
        "target_codes": len({code for code, _, _ in targets}),
        "target_dates": len(by_date),
        "wide_dates": len(wide_dates),
        "wide_target_values": len(wide_covered),
        "initial_code_queries": code_queries,
        "initial_total_queries": len(wide_dates) + code_queries,
        "exact_code_queries": exact_queries,
    }


def build_exact_tasks(
    targets: set[tuple[str, str, str]],
    *,
    token_batch_size: int,
) -> list[tuple[str, tuple[tuple[str, str], ...]]]:
    by_code: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for code, day, field in targets:
        by_code[code].append((day, field))
    tasks: list[tuple[str, tuple[tuple[str, str], ...]]] = []
    for code in sorted(by_code):
        tokens = sorted(set(by_code[code]))
        for group in batched(tokens, token_batch_size):
            tasks.append((code, tuple(group)))
    return tasks


def fetch_exact_tasks(
    tasks: list[tuple[str, tuple[tuple[str, str], ...]]],
    *,
    min_interval: float,
) -> tuple[dict[tuple[str, str, str], float], dict[str, Any]]:
    from thsdk import THS

    logging.disable(logging.CRITICAL)
    client = THS()
    connected = client.connect()
    if not connected.success:
        raise RuntimeError(f"THSDK connect failed: {connected.error}")
    values: dict[tuple[str, str, str], float] = {}
    failed_queries: list[dict[str, str]] = []
    last_query_at = 0.0
    try:
        for index, (code, group) in enumerate(tasks, 1):
            wait = min_interval - (time.monotonic() - last_query_at)
            if wait > 0:
                time.sleep(wait)
            expected = {(code, day, field) for day, field in group}
            terms = " ".join(
                f"{chinese_date(day)}{FIELD_TERMS[field]}" for day, field in group
            )
            condition = f"{code} {terms}"
            try:
                frame = query_with_retry(client, condition)
                values.update(parse_wencai_valuations(frame, allowed=expected))
            except Exception as exc:
                failed_queries.append(
                    {"condition": condition[:500], "error": f"{type(exc).__name__}: {exc}"}
                )
            last_query_at = time.monotonic()
            if index == 1 or index % 25 == 0:
                print(
                    f"valuation chunk queries={index}/{len(tasks)} values={len(values)} "
                    f"failed={len(failed_queries)}",
                    flush=True,
                )
    finally:
        client.disconnect()
        logging.disable(logging.NOTSET)
    return values, {
        "queries": len(tasks),
        "returned_values": len(values),
        "failed_queries": failed_queries,
    }


def write_targets(targets: set[tuple[str, str, str]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(sorted(targets), columns=["code", "date", "field"])
    frame.to_csv(path, index=False, encoding="utf-8-sig")


def read_targets(path: Path) -> set[tuple[str, str, str]]:
    frame = pd.read_csv(path, encoding="utf-8-sig", dtype={"code": str, "date": str})
    return {
        (str(row.code).zfill(6), str(row.date), str(row.field))
        for row in frame.itertuples(index=False)
    }


def fetch_values(
    targets: set[tuple[str, str, str]],
    fields: tuple[str, ...],
    *,
    wide_date_min_codes: int,
    token_batch_size: int,
    min_interval: float,
) -> tuple[dict[tuple[str, str, str], float], dict[str, Any]]:
    from thsdk import THS

    by_date: dict[str, set[str]] = defaultdict(set)
    triples_by_date: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    for code, day, _ in targets:
        by_date[day].add(code)
    for key in targets:
        triples_by_date[key[1]].add(key)
    wide_dates = sorted(
        day for day, codes in by_date.items() if len(codes) >= wide_date_min_codes
    )
    logging.disable(logging.CRITICAL)
    client = THS()
    connected = client.connect()
    if not connected.success:
        raise RuntimeError(f"THSDK connect failed: {connected.error}")
    values: dict[tuple[str, str, str], float] = {}
    failed_queries: list[dict[str, str]] = []
    last_query_at = 0.0
    queries = 0
    wide_queries = 0
    code_queries = 0

    def run_query(condition: str, expected: set[tuple[str, str, str]], kind: str) -> None:
        nonlocal last_query_at, queries, wide_queries, code_queries
        wait = min_interval - (time.monotonic() - last_query_at)
        if wait > 0:
            time.sleep(wait)
        try:
            frame = query_with_retry(client, condition)
            values.update(parse_wencai_valuations(frame, allowed=expected))
        except Exception as exc:
            failed_queries.append(
                {"condition": condition[:500], "error": f"{type(exc).__name__}: {exc}"}
            )
        last_query_at = time.monotonic()
        queries += 1
        wide_queries += int(kind == "wide")
        code_queries += int(kind == "code")
        if queries == 1 or queries % 25 == 0:
            print(
                f"valuation queries={queries} values={len(values)} failed={len(failed_queries)}",
                flush=True,
            )

    try:
        for day in wide_dates:
            expected = triples_by_date[day]
            terms = " ".join(FIELD_TERMS[field] for field in fields)
            run_query(f"{chinese_date(day)} A股 {terms}", expected, "wide")

        # Retry every value not returned by a wide query as an exact-code
        # request.  This preserves delisted symbols and turns a broad-query
        # omission into an explicit exact-source result.
        pending = sorted(targets - set(values))
        by_code: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for code, day, field in pending:
            by_code[code].append((day, field))
        for code in sorted(by_code):
            tokens = sorted(set(by_code[code]))
            for group in batched(tokens, token_batch_size):
                expected = {(code, day, field) for day, field in group}
                terms = " ".join(
                    f"{chinese_date(day)}{FIELD_TERMS[field]}" for day, field in group
                )
                run_query(f"{code} {terms}", expected, "code")
    finally:
        client.disconnect()
        logging.disable(logging.NOTSET)
    return values, {
        **query_plan(
            targets,
            wide_date_min_codes=wide_date_min_codes,
            token_batch_size=token_batch_size,
        ),
        "queries": queries,
        "wide_queries": wide_queries,
        "code_queries": code_queries,
        "returned_values": len(values),
        "failed_queries": failed_queries,
    }


def write_results(
    values: dict[tuple[str, str, str], float],
    path: Path,
    fields: tuple[str, ...] = tuple(FIELD_TERMS),
) -> None:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for (code, day, field), value in values.items():
        rows.setdefault((code, day), {"code": code, "date": day})[field] = value
    frame = pd.DataFrame(
        [rows[key] for key in sorted(rows)],
        columns=["code", "date", *fields],
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8-sig",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            frame.to_csv(handle, index=False)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def read_results(path: Path, fields: tuple[str, ...]) -> dict[tuple[str, str, str], float]:
    frame = pd.read_csv(path, encoding="utf-8-sig", dtype={"code": str, "date": str})
    frame["code"] = frame["code"].astype(str).str.zfill(6)
    output: dict[tuple[str, str, str], float] = {}
    for row in frame.itertuples(index=False):
        for field in fields:
            value = pd.to_numeric(pd.Series([getattr(row, field, None)]), errors="coerce").iloc[0]
            if pd.notna(value) and math.isfinite(float(value)) and float(value) != 0:
                output[(str(row.code), str(row.date), field)] = float(value)
    return output


def apply_values(
    data_dir: Path,
    legacy_dir: Path,
    values: dict[tuple[str, str, str], float],
    fields: tuple[str, ...],
    *,
    backup_dir: Path,
    apply: bool,
    original_targets: set[tuple[str, str, str]] | None = None,
) -> dict[str, Any]:
    by_code: dict[str, dict[tuple[str, str], float]] = defaultdict(dict)
    for (code, day, field), value in values.items():
        by_code[code][(day, field)] = value
    counts: defaultdict[str, int] = defaultdict(int)
    changed_files = 0
    for code in sorted(by_code):
        path = code_path(data_dir, code)
        frame = _read_csv(path)
        dates = _date_keys(frame["date"])
        legacy = _read_csv(code_path(legacy_dir, code))
        legacy_indexed = None
        if not legacy.empty and "date" in legacy.columns:
            legacy_indexed = legacy.assign(_date=_date_keys(legacy["date"]))
            legacy_indexed = legacy_indexed.dropna(subset=["_date"]).drop_duplicates(
                "_date", keep="last"
            ).set_index("_date")
        changed = False
        for (day, field), value in by_code[code].items():
            mask = dates.eq(day)
            if not mask.any():
                counts["target_date_missing"] += 1
                continue
            current_valid = valid_valuation(frame.loc[mask, field], field).iloc[0]
            if current_valid:
                replace_legacy = False
                if (
                    original_targets is not None
                    and (code, day, field) in original_targets
                    and legacy_indexed is not None
                    and day in legacy_indexed.index
                    and field in legacy_indexed
                ):
                    old = pd.to_numeric(
                        pd.Series([legacy_indexed.at[day, field]]), errors="coerce"
                    )
                    current_value = pd.to_numeric(
                        frame.loc[mask, field], errors="coerce"
                    ).iloc[0]
                    replace_legacy = valid_valuation(old, field).iloc[0] and bool(
                        np.isclose(
                            float(current_value),
                            float(old.iloc[0]),
                            rtol=0.0,
                            atol=1e-10,
                        )
                    )
                if not replace_legacy:
                    counts["already_valid"] += 1
                    continue
                counts[f"replaced_legacy_{field}"] += 1
            frame.loc[mask, field] = float(value)
            counts[f"filled_{field}"] += 1
            changed = True
            if legacy_indexed is not None and day in legacy_indexed.index and field in legacy_indexed:
                old = pd.to_numeric(pd.Series([legacy_indexed.at[day, field]]), errors="coerce")
                if valid_valuation(old, field).iloc[0]:
                    counts[f"legacy_compared_{field}"] += 1
                    relative = abs(float(value) / float(old.iloc[0]) - 1.0)
                    counts[f"legacy_within_1pct_{field}"] += int(relative <= 0.01)
                    counts[f"legacy_within_5pct_{field}"] += int(relative <= 0.05)
        if changed:
            changed_files += 1
            if apply:
                relative = path.relative_to(data_dir)
                backup_path = backup_dir / relative
                backup_path.parent.mkdir(parents=True, exist_ok=True)
                if not backup_path.exists():
                    shutil.copy2(path, backup_path)
                _atomic_csv(frame, path)
                invalidate_caches(data_dir, code)
    return {"changed_files": changed_files, "counts": dict(counts)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--legacy-dir", default="data_pre_ths_backup_20260727_110350")
    parser.add_argument("--fields", default="pe_dynamic,pb,ps")
    parser.add_argument("--wide-date-min-codes", type=int, default=100)
    parser.add_argument("--token-batch-size", type=int, default=40)
    parser.add_argument("--min-interval", type=float, default=0.25)
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument(
        "--targets-file",
        default="artifacts/maintenance/all_data_gaps/ths_wencai_missing_valuation_targets.csv",
    )
    parser.add_argument(
        "--chunk-dir",
        default="artifacts/maintenance/all_data_gaps/valuation_chunks",
    )
    parser.add_argument("--task-chunk-size", type=int, default=400)
    parser.add_argument("--task-chunk-index", type=int, default=None)
    parser.add_argument("--merge-chunks", action="store_true")
    parser.add_argument("--reuse-results", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--results",
        default="artifacts/maintenance/all_data_gaps/ths_wencai_missing_valuations.csv",
    )
    parser.add_argument(
        "--report",
        default="artifacts/maintenance/all_data_gaps/ths_wencai_missing_valuations_report.json",
    )
    parser.add_argument(
        "--backup-dir",
        default="artifacts/maintenance/all_data_gaps/valuation_ths_backup",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    fields = tuple(dict.fromkeys(value.strip() for value in args.fields.split(",") if value.strip()))
    unknown = set(fields) - set(FIELD_TERMS)
    if unknown:
        raise ValueError(
            f"unsupported or semantically incompatible THS valuation fields: {sorted(unknown)}"
        )
    if args.token_batch_size > 40:
        raise ValueError("Wencai valuation token batches above 40 are not validated")
    data_dir = Path(args.data_dir).resolve()
    legacy_dir = Path(args.legacy_dir).resolve()
    results_path = (ROOT / args.results).resolve()
    report_path = (ROOT / args.report).resolve()
    backup_dir = (ROOT / args.backup_dir).resolve()
    targets_path = (ROOT / args.targets_file).resolve()
    chunk_dir = (ROOT / args.chunk_dir).resolve()
    started = time.time()
    if (args.task_chunk_index is not None or args.merge_chunks) and targets_path.exists():
        targets = read_targets(targets_path)
    else:
        targets = identify_targets(data_dir, fields)
        write_targets(targets, targets_path)
    plan = query_plan(
        targets,
        wide_date_min_codes=args.wide_date_min_codes,
        token_batch_size=args.token_batch_size,
    )
    if args.plan_only:
        report = {
            "status": "PLAN_ONLY",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "fields": fields,
            "plan": plan,
            "pcf_exclusion": "repository pcfNcfTTM != THS operating-cash-flow PCF",
        }
        _atomic_json(report, report_path)
        print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
        return 0
    if args.task_chunk_index is not None:
        tasks = build_exact_tasks(targets, token_batch_size=args.token_batch_size)
        total_chunks = math.ceil(len(tasks) / args.task_chunk_size)
        if args.task_chunk_index < 0 or args.task_chunk_index >= total_chunks:
            raise ValueError(
                f"task chunk index {args.task_chunk_index} outside 0..{total_chunks - 1}"
            )
        start = args.task_chunk_index * args.task_chunk_size
        selected = tasks[start : start + args.task_chunk_size]
        values, fetch = fetch_exact_tasks(selected, min_interval=args.min_interval)
        chunk_dir.mkdir(parents=True, exist_ok=True)
        chunk_path = chunk_dir / f"chunk_{args.task_chunk_index:04d}.csv"
        write_results(values, chunk_path, fields)
        chunk_report = {
            "status": "COMPLETED" if not fetch["failed_queries"] else "PARTIAL",
            "chunk_index": args.task_chunk_index,
            "total_chunks": total_chunks,
            "task_start": start,
            "task_stop": start + len(selected),
            "chunk_path": str(chunk_path),
            "fetch": fetch,
            "elapsed_seconds": round(time.time() - started, 3),
        }
        _atomic_json(
            chunk_report,
            chunk_dir / f"chunk_{args.task_chunk_index:04d}.json",
        )
        print(json.dumps(chunk_report, ensure_ascii=False, indent=2), flush=True)
        return 0 if not fetch["failed_queries"] else 2
    if args.merge_chunks:
        values: dict[tuple[str, str, str], float] = {}
        chunk_paths = sorted(chunk_dir.glob("chunk_*.csv"))
        for chunk_path in chunk_paths:
            values.update(read_results(chunk_path, fields))
        expected_chunks = math.ceil(
            len(build_exact_tasks(targets, token_batch_size=args.token_batch_size))
            / args.task_chunk_size
        )
        if len(chunk_paths) != expected_chunks:
            raise RuntimeError(
                f"valuation chunks incomplete: expected {expected_chunks}, found {len(chunk_paths)}"
            )
        write_results(values, results_path, fields)
        fetch = {
            **plan,
            "chunk_files": len(chunk_paths),
            "returned_values": len(values),
            "failed_queries": [],
        }
    elif args.reuse_results:
        values = read_results(results_path, fields)
        fetch = {**plan, "reused_results": True, "returned_values": len(values), "failed_queries": []}
    else:
        values, fetch = fetch_values(
            targets,
            fields,
            wide_date_min_codes=args.wide_date_min_codes,
            token_batch_size=args.token_batch_size,
            min_interval=args.min_interval,
        )
        write_results(values, results_path, fields)
    applied = apply_values(
        data_dir,
        legacy_dir,
        values,
        fields,
        backup_dir=backup_dir,
        apply=args.apply,
        original_targets=targets,
    )
    report = {
        "status": "COMPLETED" if not fetch.get("failed_queries") else "PARTIAL",
        "applied": bool(args.apply),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "THSDK wencai_nlp exact-date market ratios",
        "fields": fields,
        "data_dir": str(data_dir),
        "legacy_dir": str(legacy_dir),
        "results": str(results_path),
        "backup_dir": str(backup_dir),
        "fetch": fetch,
        "apply_stats": applied,
        "pcf_exclusion": "repository pcfNcfTTM != THS operating-cash-flow PCF",
        "elapsed_seconds": round(time.time() - started, 3),
    }
    _atomic_json(report, report_path)
    if args.apply and not fetch.get("failed_queries"):
        _update_manifest(
            data_dir,
            "historical_missing_valuations_ths_wencai",
            {
                "source": "THSDK wencai_nlp exact-date market ratios",
                "fields": list(fields),
                "filled": applied["counts"],
                "pcf_exclusion": report["pcf_exclusion"],
                "report": str(report_path),
            },
        )
    print(f"report={report_path}", flush=True)
    print(f"fetch={fetch} apply={applied}", flush=True)
    return 0 if not fetch.get("failed_queries") else 2


if __name__ == "__main__":
    raise SystemExit(main())
