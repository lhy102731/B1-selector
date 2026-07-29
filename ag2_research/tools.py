"""AG2-compatible tools that wrap the existing codebase APIs.

Each tool is a Python function with type hints and docstrings.
AG2's agent framework auto-converts these into function-calling tools.
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Annotated, Any

# Ensure project root is on path for clean imports
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


_CODE_ALLOWED_DIRS = {
    "ag2_research", "config", "docs", "l2", "research",
    "research_automation", "research_state", "strategy", "tests", "tools", "utils", "web",
}
_READABLE_EXTENSIONS = {".py", ".md", ".yaml", ".yml", ".txt", ".bat", ".csv"}
_SENSITIVE_NAMES = {
    ".env", "credential", "credentials", "credentials.json", "secret", "secrets", "secrets.json",
    "api_keys", "api_keys.json", "api_keys.yaml", "api_keys.yml",
    "token", "tokens", "id_rsa", "id_ed25519",
}
_SENSITIVE_PREFIXES = ("credential_", "credentials_", "secret_", "secrets_", "api_key_", "api_keys_", "token_", "tokens_")
_PRODUCTION_INDICATORS_CACHE = "indicators_cache"
_RESEARCH_INDICATORS_CACHE = "research_indicators_cache"
_INDICATOR_CACHE_POLICY = (
    "AG2 research tools default to data/research_indicators_cache. "
    "data/indicators_cache is production/reference only unless explicitly requested."
)


def _safe_indicator_cache_name(cache_name: str | None) -> str:
    value = str(cache_name or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("indicator cache name may contain only letters, numbers, '_' and '-'")
    return value


def _indicator_cache_status(cache_name: str) -> dict[str, Any]:
    safe_name = _safe_indicator_cache_name(cache_name)
    path = _PROJECT_ROOT / "data" / safe_name
    return {
        "name": safe_name,
        "path": str(path),
        "exists": path.exists(),
        "files": len(list(path.glob("*.parquet"))) if path.exists() else 0,
    }


def _is_within(path: Path, root: Path) -> bool:
    """Compare resolved paths by components, not vulnerable string prefixes."""
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _has_hidden_or_sensitive_part(path: Path, root: Path) -> bool:
    relative = path.relative_to(root)
    for part in relative.parts:
        lowered = part.lower()
        stem = Path(lowered).stem
        if (part.startswith(".") or lowered in _SENSITIVE_NAMES or lowered.startswith(".env.")
                or stem.startswith(_SENSITIVE_PREFIXES)):
            return True
    return False


def _safe_code_path(relative_path: str, *, require_file: bool = True) -> Path:
    root = _PROJECT_ROOT.resolve()
    path = (root / relative_path).resolve()
    if not _is_within(path, root):
        raise ValueError("Path traversal denied")
    if _has_hidden_or_sensitive_part(path, root):
        raise ValueError("Hidden or sensitive paths are not readable")

    relative = path.relative_to(root)
    if len(relative.parts) > 1 and relative.parts[0] not in _CODE_ALLOWED_DIRS:
        raise ValueError("Path is outside the allowed source directories")
    if require_file and path.suffix.lower() not in _READABLE_EXTENSIONS:
        raise ValueError("File extension is not allowed")
    return path


# ============================================================
# Tool: get_strategy_config
# ============================================================
def get_strategy_config(
    strategy_name: Annotated[str | None, "Strategy name to query, e.g. 'UnifiedB1Strategy'. If None, lists all."] = None,
) -> str:
    """Read strategy parameters and configuration from the project YAML config.

    Returns strategy name, its parameters, and any pattern-matching config.
    """
    import yaml

    config_path = _PROJECT_ROOT / "config" / "strategy_params.yaml"
    if not config_path.exists():
        return json.dumps({"error": f"Config not found: {config_path}"}, ensure_ascii=False)

    with open(config_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if strategy_name:
        normalized = str(strategy_name).strip().lower()
        if normalized in {"brick", "brick_v2", "brick-v2"}:
            brick_cfg = {
                "subject": "brick",
                "status": "research_only",
                "production_script": {
                    "path": "backtest_brick_v2.py",
                    "policy": "do not modify without explicit user authorization",
                    "exists": (_PROJECT_ROOT / "backtest_brick_v2.py").exists(),
                },
                "research_runner": {
                    "path": "research/brick_v2_rebuilt_dual_metrics.py",
                    "exists": (_PROJECT_ROOT / "research" / "brick_v2_rebuilt_dual_metrics.py").exists(),
                },
                "baseline_paths": {
                    "frozen_top3_top5_no_timing": (
                        "research_state/brick/baselines/"
                        "sq_nav_notiming_top3_top5_20260709"
                    ),
                    "cost_fixed_topn": (
                        "research_state/brick/"
                        "v2_dual_metrics_costfix_top2_top3_top5_20260710"
                    ),
                    "industry_cap_costfix": (
                        "research_state/brick/"
                        "industry_cap_effect_costfix_20260710"
                    ),
                    "failure_attribution": (
                        "research_state/brick/v2_failure_attribution_20260710.md"
                    ),
                    "factor_library": (
                        "research_state/brick/factor_library/"
                        "brick_effective_factor_library_20260709.md"
                    ),
                },
                "validation_rules": {
                    "split_key": "entry_date",
                    "purge_rule": "train labels must satisfy exit_date < test_start",
                    "folds": [
                        "2020-2022 train -> 2023 validation -> 2024 test",
                        "2021-2023 train -> 2024 validation -> 2025 test",
                        "2022-2024 train -> 2025 validation -> 2026 test",
                    ],
                    "market_timing": "disabled for primary results",
                },
                "pre_0925_boundary": {
                    "allowed": [
                        "signal-day and earlier daily fields",
                        "entry_date open against signal-day close/yellow/MA5 references",
                    ],
                    "forbidden_model_inputs": [
                        "return_pct",
                        "exit_date",
                        "exit_price",
                        "hold_days",
                        "entry_date high/low/close",
                        "T+1 close-derived indicators",
                    ],
                },
                "structured_kb_subject": "brick",
            }
            return json.dumps({"brick": brick_cfg}, ensure_ascii=False, indent=2)
        section = cfg.get(strategy_name, {})
        if not section:
            return json.dumps({"error": f"Strategy '{strategy_name}' not found", "available": list(cfg.keys())}, ensure_ascii=False)
        return json.dumps({strategy_name: section}, ensure_ascii=False, indent=2)

    return json.dumps({"strategies": list(cfg.keys()) + ["brick"]}, ensure_ascii=False, indent=2)


# ============================================================
# Tool: run_backtest
# ============================================================
def run_backtest(
    start_date: Annotated[str, "Start date YYYY-MM-DD"],
    end_date: Annotated[str, "End date YYYY-MM-DD"],
    max_stocks: Annotated[int, "Max stocks per day"] = 10,
    min_similarity: Annotated[int, "Minimum B1 similarity threshold (0-100)"] = 60,
    sample_size: Annotated[int | None, "Random sample size for selection, None = all"] = None,
    j_threshold: Annotated[float | None, "J-value threshold override. None = use YAML default"] = None,
    volume_shrink_ratio: Annotated[float | None, "Volume shrink ratio override. None = use YAML default"] = None,
    indicators_cache_name: Annotated[str, "Indicator cache folder under data/. Defaults to research_indicators_cache for AG2 research isolation. Use indicators_cache only for explicit production reproduction."] = _RESEARCH_INDICATORS_CACHE,
) -> str:
    """Run a backtest with the optimized backtester and return summary metrics.

    WARNING: This can take 5-30 minutes depending on date range. Use for final validation,
    not quick checks. Returns total_return_pct, max_drawdown_pct, win_rate, sharpe_approx,
    trade_count, and annualized return.
    """
    from backtest_optimized import OptimizedBacktester

    try:
        safe_cache_name = _safe_indicator_cache_name(indicators_cache_name)
    except ValueError as e:
        return json.dumps({
            "error": str(e),
            "indicator_cache_policy": _INDICATOR_CACHE_POLICY,
        }, ensure_ascii=False)

    bt = OptimizedBacktester(
        data_dir=str(_PROJECT_ROOT / "data"),
        use_cache=True,
        indicators_cache_name=safe_cache_name,
    )
    bt.max_stocks_per_day = max_stocks
    bt.min_similarity = min_similarity
    if j_threshold is not None:
        bt.strategy.set_param("j_threshold", j_threshold)
    if volume_shrink_ratio is not None:
        bt.strategy.set_param("volume_shrink_ratio", volume_shrink_ratio)

    bt.run(
        start_date=start_date,
        end_date=end_date,
        sample_size=sample_size,
        n_workers=8,
    )

    if hasattr(bt, "last_summary") and isinstance(bt.last_summary, dict):
        result = dict(bt.last_summary)
    elif hasattr(bt, "last_summary"):
        result = {"status": "completed", "summary": bt.last_summary}
    else:
        result = {"status": "completed", "note": "Check console output for full results"}
    result["indicator_cache"] = _indicator_cache_status(safe_cache_name)
    result["indicator_cache_policy"] = _INDICATOR_CACHE_POLICY
    return json.dumps(result, ensure_ascii=False, indent=2)


# ============================================================
# Tool: list_research_docs
# ============================================================
def list_research_docs(
    query: Annotated[str | None, "Optional filter keyword, e.g. 'V2', 'brick', 'WF3'"] = None,
) -> str:
    """List research documents in the docs/ directory.

    Returns filenames. Use read_research_doc to read a specific file.
    """
    docs_dir = _PROJECT_ROOT / "docs"
    if not docs_dir.exists():
        return json.dumps({"error": "docs/ directory not found"}, ensure_ascii=False)

    files = sorted(p.name for p in docs_dir.glob("*.md"))
    if query:
        query_lower = query.lower()
        files = [f for f in files if query_lower in f.lower()]

    return json.dumps({"count": len(files), "files": files}, ensure_ascii=False, indent=2)


# ============================================================
# Tool: read_research_doc
# ============================================================
def read_research_doc(
    filename: Annotated[str, "Document filename, e.g. 'b1_v3_results.md'"],
    max_lines: Annotated[int, "Max lines to return (default 99999)"] = 99999,
) -> str:
    """Read a research document from the docs/ directory.

    Returns the first max_lines of the document. Use list_research_docs to find filenames.
    """
    docs_dir = (_PROJECT_ROOT / "docs").resolve()
    filepath = (docs_dir / filename).resolve()
    if (not _is_within(filepath, docs_dir) or filepath.parent != docs_dir
            or _has_hidden_or_sensitive_part(filepath, _PROJECT_ROOT.resolve())
            or filepath.suffix.lower() != ".md"):
        return json.dumps({"error": "Only Markdown files directly inside docs/ may be read"}, ensure_ascii=False)
    if not filepath.exists():
        return json.dumps({"error": f"File not found: {filename}", "hint": "Use list_research_docs to see available files"}, ensure_ascii=False)

    try:
        with open(filepath, encoding="utf-8") as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        with open(filepath, encoding="gbk") as f:
            lines = f.readlines()

    take = len(lines) if max_lines <= 0 else min(len(lines), max_lines)
    content = "".join(lines[:take])
    truncated = take < len(lines)
    result = {"filename": filename, "lines_returned": take, "total_lines": len(lines), "truncated": truncated, "content": content}
    return json.dumps(result, ensure_ascii=False)


# ============================================================
# Tool: get_market_calendar
# ============================================================
def get_market_calendar(
    start_date: Annotated[str, "Start date YYYY-MM-DD"],
    end_date: Annotated[str, "End date YYYY-MM-DD, defaults to today"] = "2026-12-31",
) -> str:
    """Get A-share trading calendar for a date range.

    Returns total trading days, first 5 and last 5 dates for inspection.
    """
    from utils.akshare_fetcher import AKShareFetcher

    fetcher = AKShareFetcher(data_dir=str(_PROJECT_ROOT / "data"))
    try:
        import akshare as ak
        calendar_df = ak.tool_trade_date_hist_sina()
        calendar_df = calendar_df[(calendar_df["trade_date"] >= start_date) & (calendar_df["trade_date"] <= end_date)]
        dates = sorted(calendar_df["trade_date"].tolist())
        return json.dumps({
            "total_trading_days": len(dates),
            "first_5": dates[:5],
            "last_5": dates[-5:],
        }, ensure_ascii=False, indent=2)
    except Exception:
        return json.dumps({"error": "Failed to fetch calendar, akshare may need updating"}, ensure_ascii=False)


# ============================================================
# Tool: check_market_timing
# ============================================================
def check_market_timing(
    date: Annotated[str, "Date to check YYYY-MM-DD"],
) -> str:
    """Check if a given date is in a bullish (可开仓) or bearish (不开仓) regime.

    Uses the active-cap-based MarketTiming state machine.
    """
    from utils.market_timing import MarketTiming

    mt = MarketTiming()
    mt_path = _PROJECT_ROOT / "data" / "market" / "active_cap.csv"
    if not mt_path.exists():
        return json.dumps({"error": "Market timing data not available. Run fund_flow_collector first.", "path": str(mt_path)}, ensure_ascii=False)

    mt.load(str(mt_path))
    bullish = mt.is_bullish(date)
    can_open = mt.can_open(date)
    return json.dumps({"date": date, "is_bullish": bullish, "can_open": can_open}, ensure_ascii=False)


# ============================================================
# Tool: list_available_data
# ============================================================
def list_available_data() -> str:
    """List all available data: stock count, market data, cache status.

    Returns counts of stocks by exchange prefix, research/production indicator
    cache status, signal cache status.
    """
    from utils.csv_manager import CSVManager

    cm = CSVManager(str(_PROJECT_ROOT / "data"))
    stocks = cm.list_all_stocks()

    # Count by prefix
    counts: dict[str, int] = {}
    for code in stocks:
        prefix = code[:2]
        counts[prefix] = counts.get(prefix, 0) + 1

    # Cache status
    research_indicator_cache = _indicator_cache_status(_RESEARCH_INDICATORS_CACHE)
    production_indicator_cache = _indicator_cache_status(_PRODUCTION_INDICATORS_CACHE)
    signal_cache = _PROJECT_ROOT / "data" / "signal_cache"
    market_dir = _PROJECT_ROOT / "data" / "market"

    return json.dumps({
        "total_stocks": len(stocks),
        "by_prefix": counts,
        "indicator_cache_policy": _INDICATOR_CACHE_POLICY,
        "default_indicator_cache": _RESEARCH_INDICATORS_CACHE,
        "research_indicator_cache": research_indicator_cache,
        "production_indicator_cache": production_indicator_cache,
        "indicator_cache_exists": research_indicator_cache["exists"],
        "indicator_cache_files": research_indicator_cache["files"],
        "signal_cache_exists": signal_cache.exists(),
        "signal_cache_files": len(list(signal_cache.glob("*.pkl"))) if signal_cache.exists() else 0,
        "market_data_exists": market_dir.exists(),
    }, ensure_ascii=False, indent=2)


# ============================================================
# Tool: list_code
# ============================================================
def list_code(
    subdir: Annotated[str | None, "Subdirectory to list, e.g. 'strategy', 'utils'. None = project root"] = None,
    pattern: Annotated[str, "File pattern: '*.py' (default), '*.md', or '*' for all"] = "*.py",
) -> str:
    """List source and documentation files in the project. Use to discover what exists.

    Returns file paths relative to project root. Excludes data/, __pycache__, .git/.
    """
    try:
        base = _PROJECT_ROOT.resolve() if subdir is None else _safe_code_path(subdir, require_file=False)
    except (ValueError, OSError) as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)
    if base != _PROJECT_ROOT.resolve():
        relative = base.relative_to(_PROJECT_ROOT.resolve())
        if not relative.parts or relative.parts[0] not in _CODE_ALLOWED_DIRS:
            return json.dumps({"error": "Directory is outside the allowed source directories"}, ensure_ascii=False)
    if not base.exists():
        return json.dumps({"error": f"Directory not found: {subdir}"}, ensure_ascii=False)

    files = []
    for f in sorted(base.rglob(pattern)):
        if not f.is_file():
            continue
        try:
            safe_file = _safe_code_path(str(f.relative_to(_PROJECT_ROOT)))
        except (ValueError, OSError):
            continue
        files.append(str(safe_file.relative_to(_PROJECT_ROOT.resolve())))

    return json.dumps({"directory": subdir or ".", "pattern": pattern, "files": files}, ensure_ascii=False, indent=2)


# ============================================================
# Tool: read_code
# ============================================================
def read_code(
    filepath: Annotated[str, "Path relative to project root, e.g. 'strategy/unified_b1_strategy.py' or 'CLAUDE.md'"],
    max_lines: Annotated[int, "Max lines to return (default 99999)"] = 99999,
    offset: Annotated[int, "Zero-based line offset to start reading from"] = 0,
) -> str:
    """Read any source or documentation file from the project. Use list_code first to find files.

    Supports .py, .md, .yaml, .txt, .bat, .csv (first 50 lines for CSV).
    Returns the file content from offset up to max_lines. Large files are truncated.
    """
    try:
        full_path = _safe_code_path(filepath)
    except (ValueError, OSError) as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)
    if not full_path.exists():
        return json.dumps({"error": f"File not found: {filepath}"}, ensure_ascii=False)

    # CSV files: limit lines and try gbk encoding
    if filepath.endswith(".csv"):
        max_lines = min(max_lines, 50)

    try:
        content = full_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            content = full_path.read_text(encoding="gbk")
        except (UnicodeDecodeError, PermissionError) as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)
    except PermissionError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    lines = content.split("\n")
    try:
        start = max(0, int(offset))
    except (TypeError, ValueError):
        start = 0
    remaining = max(0, len(lines) - start)
    take = remaining if max_lines <= 0 else min(remaining, max_lines)
    end = start + take
    truncated = end < len(lines)
    result = {
        "filepath": filepath,
        "offset": start,
        "lines_returned": take,
        "total_lines": len(lines),
        "truncated": truncated,
        "content": "\n".join(lines[start:end]),
    }
    return json.dumps(result, ensure_ascii=False)


# ============================================================
# Tools: local Obsidian/MinerU book vault
# ============================================================
_DEFAULT_KBASE_PATH = Path(os.environ.get("KBASE_PATH", r"D:\KBase"))


def _book_search_terms(query: str) -> list[str]:
    text = query.lower()
    terms = re.findall(r"[a-z0-9_]{2,}|[\u4e00-\u9fff]{2,}", text)
    # Chinese text often has no spaces; add short overlapping phrases so exact
    # concepts like "量价" or "趋势" can still match.
    chinese = "".join(re.findall(r"[\u4e00-\u9fff]+", text))
    if len(chinese) >= 2:
        terms.extend(chinese[i:i + 2] for i in range(len(chinese) - 1))
    if len(chinese) >= 3:
        terms.extend(chinese[i:i + 3] for i in range(len(chinese) - 2))
    if not terms:
        terms.extend(t for t in re.split(r"\s+", text.strip()) if len(t) >= 2)
    seen: set[str] = set()
    return [t for t in terms if not (t in seen or seen.add(t))]


def _iter_kbase_markdown(vault_path: Path, scope: str):
    candidates: list[Path] = []
    if scope in {"wiki", "all"}:
        candidates.append(vault_path / "wiki")
    if scope in {"raw", "all"}:
        candidates.append(vault_path / "raw" / "books")

    for base in candidates:
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.md")):
            if any(part.startswith(".") for part in path.parts):
                continue
            yield path


def _safe_kbase_path(root: Path, relative_path: str) -> Path:
    clean = relative_path.replace("/", "\\").lstrip("\\")
    path = (root / clean).resolve()
    root_resolved = root.resolve()
    if not str(path).lower().startswith(str(root_resolved).lower()):
        raise ValueError("path must stay inside the book vault")
    return path


def _read_text_fallback(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="gbk", errors="ignore")


def _chunk_markdown(content: str, max_chars: int = 1800):
    lines = content.splitlines()
    chunk: list[str] = []
    start_line = 1
    size = 0
    for idx, line in enumerate(lines, start=1):
        if not chunk:
            start_line = idx
        chunk.append(line)
        size += len(line) + 1
        if size >= max_chars and (not line.strip() or line.startswith("#")):
            yield start_line, "\n".join(chunk).strip()
            chunk = []
            size = 0
    if chunk:
        yield start_line, "\n".join(chunk).strip()


def book_index(
    max_chars: Annotated[int, "Maximum characters from wiki/index.md to return."] = 12000,
    vault_path: Annotated[str | None, "Obsidian vault path. Defaults to KBASE_PATH or D:\\KBase."] = None,
) -> str:
    """Open the local book vault's progressive-disclosure entry point.

    Agents should call this first. It returns the vault rules, the content map,
    and a compact file inventory so the next step can be a targeted book_open.
    """
    from ag2_research.kbase.tools import kbase_overview as _overview

    return _overview(vault_path=vault_path, top_n=max(5, min(max_chars // 500, 50)))


def book_open(
    relative_path: Annotated[str, "Path inside the vault, e.g. 'wiki/concepts/Volume Price Analysis.md'."],
    start_line: Annotated[int, "1-based line number to start reading from."] = 1,
    max_lines: Annotated[int, "Maximum lines to return."] = 160,
    vault_path: Annotated[str | None, "Obsidian vault path. Defaults to KBASE_PATH or D:\\KBase."] = None,
) -> str:
    """Open one selected knowledge-base file after book_index reveals it."""
    from ag2_research.kbase.tools import kbase_open as _open

    # Compatibility wrapper: line pagination becomes a bounded character page.
    cursor = max(0, start_line - 1) * 80
    return _open(
        path=relative_path,
        layer="raw",
        cursor=cursor,
        max_chars=max(1600, min(max_lines, 600) * 120),
        vault_path=vault_path,
    )


def book_search(
    query: Annotated[str, "Search question or keywords, e.g. '量价背离 如何处理假突破'."],
    max_results: Annotated[int, "Maximum result chunks to return."] = 5,
    scope: Annotated[str, "Search scope: wiki | raw | all. wiki is concise, raw searches full books."] = "all",
    vault_path: Annotated[str | None, "Obsidian vault path. Defaults to KBASE_PATH or D:\\KBase."] = None,
) -> str:
    """Search the local processed book vault before making research claims.

    This is a local RAG-style reader, not model training. It returns cited
    passages from D:\\KBase. Prefer book_index -> book_open for Karpathy-style
    progressive disclosure; use this when the index is insufficient.
    """
    from ag2_research.kbase.tools import kbase_search as _search

    if scope.lower().strip() not in {"wiki", "raw", "all"}:
        return json.dumps({"error": "scope must be one of: wiki, raw, all"}, ensure_ascii=False)
    return _search(
        query,
        scope="sources",
        max_results=max_results,
        vault_path=vault_path,
    )


def kbase_overview(
    top_n: Annotated[int, "Maximum top facets to return."] = 20,
    vault_path: Annotated[str | None, "KBase vault path."] = None,
) -> str:
    from ag2_research.kbase.tools import kbase_overview as impl
    return impl(vault_path=vault_path, top_n=top_n)


def kbase_browse(
    node_id: Annotated[str, "root, family id, source id, or date:YYYY-MM-DD"] = "root",
    relation: Annotated[
        str,
        "For root: children, maps, or families. For a source: related or parents.",
    ] = "children",
    cursor: Annotated[int, "Result offset."] = 0,
    page_size: Annotated[int, "Page size, maximum 100."] = 20,
    vault_path: Annotated[str | None, "KBase vault path."] = None,
) -> str:
    from ag2_research.kbase.tools import kbase_browse as impl
    return impl(node_id, relation=relation, cursor=cursor, page_size=page_size, vault_path=vault_path)


def kbase_search(
    query: Annotated[str, "Source question or keywords."],
    people: Annotated[str | None, "Optional person filter."] = None,
    family_id: Annotated[str | None, "Optional source-family filter."] = None,
    topics: Annotated[str | None, "Optional topic filter."] = None,
    source_type: Annotated[str | None, "Optional source-type filter."] = None,
    date_from: Annotated[str | None, "Optional inclusive start date."] = None,
    date_to: Annotated[str | None, "Optional inclusive end date."] = None,
    voice_role: Annotated[str | None, "Optional source voice filter."] = None,
    review_status: Annotated[str | None, "Optional review status filter."] = None,
    max_results: Annotated[int, "Maximum results."] = 5,
    cursor: Annotated[int, "Result offset."] = 0,
    vault_path: Annotated[str | None, "KBase vault path."] = None,
) -> str:
    from ag2_research.kbase.tools import kbase_search as impl
    return impl(
        query, people=people, family_id=family_id, topics=topics,
        source_type=source_type, date_from=date_from, date_to=date_to,
        voice_role=voice_role, review_status=review_status,
        max_results=max_results, cursor=cursor, vault_path=vault_path,
    )


def kbase_open(
    source_id: Annotated[str, "Catalog source id."],
    layer: Annotated[str, "summary, statements, evidence, raw, or visual"] = "summary",
    cursor: Annotated[int, "Character offset."] = 0,
    max_chars: Annotated[int, "Maximum characters returned."] = 8000,
    vault_path: Annotated[str | None, "KBase vault path."] = None,
) -> str:
    from ag2_research.kbase.tools import kbase_open as impl
    return impl(source_id, layer=layer, cursor=cursor, max_chars=max_chars, vault_path=vault_path)


def kbase_trace(
    source_id: Annotated[str, "Catalog source id."],
    vault_path: Annotated[str | None, "KBase vault path."] = None,
) -> str:
    from ag2_research.kbase.tools import kbase_trace as impl
    return impl(source_id, vault_path=vault_path)


# ============================================================
# Tool: kb_validated_claims  (project-safe external evidence)
# ============================================================
def kb_validated_claims(
    subject: Annotated[str, "Project knowledge subject, e.g. 'b1_v3'."],
    query: Annotated[str, "Optional terms used to rank validated claims."] = "",
    max_results: Annotated[int, "Maximum validated claims to return."] = 10,
    vault_path: Annotated[str | None, "KBase vault path. Defaults to KBASE_PATH or D:\\KBase."] = None,
) -> str:
    """Return only external claims that pass the Knowledge Bridge contract.

    Source notes, concepts, draft claims, and unreviewed project outputs are
    deliberately excluded. Project hard constraints remain authoritative.
    """
    from ag2_research.knowledge_bridge import load_validated_claims, normalize_subject

    claims = load_validated_claims(subject, vault_path=vault_path)
    if query:
        terms = [term.lower() for term in re.findall(r"[\w\u4e00-\u9fff]+", query) if len(term) > 1]

        def score(item) -> int:
            text = f"{item.title} {item.claim} {item.limits}".lower()
            return sum(text.count(term) for term in terms)

        claims = sorted(claims, key=lambda item: (-score(item), item.claim_id))
    claims = claims[:max(1, min(int(max_results), 50))]
    return json.dumps({
        "subject": normalize_subject(subject),
        "authority": "supporting_evidence_only",
        "project_hard_constraints_take_precedence": True,
        "count": len(claims),
        "claims": [item.to_dict() for item in claims],
    }, ensure_ascii=False, indent=2)


# ============================================================
# Tool: kb_lookup  (read structured knowledge base)
# ============================================================
def kb_lookup(
    subject: Annotated[str, "Strategy subject, e.g. 'b1_v3'."],
    section: Annotated[str, "One of: brief | architecture | geometry | graph | "
                            "alpha_generators | exit_alphas | concentrators | "
                            "dead_components | redundant | interactions | lessons | "
                            "frozen_baseline | acceptance_bar | hard_constraints | "
                            "headline | manifest | list"],
    query: Annotated[str | None, "Optional substring filter on item IDs / names."] = None,
) -> str:
    """Look up authoritative B1 V3 / strategy knowledge before proposing changes.

    Returns JSON. Cite the section + kb_version when you use the result.
    Use this BEFORE recommending parameter changes, removing filters, or
    proposing 'new alpha' — most of those questions are already settled in
    the closure.
    """
    from ag2_research.knowledge_base import load, list_subjects

    # Fast-path: no KB registered for this subject. Return a short hint so the
    # LLM stops asking and proceeds without consulting a KB. Common case for
    # non-B1 strategies that haven't been audited yet.
    if subject not in list_subjects():
        return json.dumps({
            "info": f"no knowledge base registered for subject '{subject}'",
            "available_subjects": list_subjects(),
            "guidance": "proceed without KB consultation; no rules to enforce",
        }, ensure_ascii=False)

    try:
        kb = load(subject)
    except FileNotFoundError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    s = section.lower().strip()

    if s == "list":
        return json.dumps({
            "subject": kb.subject,
            "kb_version": kb.kb_version,
            "as_of_phase": kb.as_of_phase,
            "sections_available": [
                "brief", "architecture", "geometry", "graph",
                "alpha_generators", "exit_alphas", "concentrators",
                "dead_components", "redundant", "interactions", "lessons",
                "frozen_baseline", "acceptance_bar", "hard_constraints",
                "headline", "manifest",
            ] + [
                key for key in kb.list_artifacts()
                if key not in {
                    "brief", "architecture", "geometry", "graph",
                    "alpha_generators", "exit_generators", "concentrators",
                    "dead_components", "redundant", "interactions", "lessons",
                    "hard_constraints", "full_verdict",
                }
            ],
            "artifacts_on_disk": kb.list_artifacts(),
        }, ensure_ascii=False, indent=2)

    payload: Any
    if s == "brief":
        payload = kb.read_text("brief")
    elif s == "architecture":
        payload = kb.read_text("architecture")
    elif s == "geometry":
        payload = kb.read_text("geometry")
    elif s == "graph":
        payload = kb.read_text("graph")
    elif s == "alpha_generators":
        payload = kb.alpha_generators()
    elif s == "exit_alphas":
        payload = kb.exit_alphas()
    elif s == "concentrators":
        payload = kb.concentrators()
    elif s == "dead_components":
        payload = kb.dead_components()
    elif s == "redundant":
        payload = kb.read_json("redundant")
    elif s == "interactions":
        payload = kb.interaction_summary()
    elif s == "lessons":
        payload = kb.lessons()
    elif s == "frozen_baseline":
        payload = kb.frozen_baseline
    elif s == "acceptance_bar":
        payload = kb.acceptance_bar
    elif s == "headline":
        payload = kb.headline
    elif s == "hard_constraints":
        payload = kb.hard_constraints
    elif s == "manifest":
        payload = kb.manifest
    elif s in kb.artifacts:
        artifact_path = kb.artifacts[s]
        if artifact_path.suffix.lower() == ".json":
            payload = kb.read_json(s)
        else:
            payload = kb.read_text(s)
    else:
        return json.dumps({
            "error": f"Unknown section '{section}'. Call kb_lookup(subject, 'list') for available sections.",
        }, ensure_ascii=False)

    # Optional substring filter for list payloads
    if query and isinstance(payload, list):
        q = query.lower()
        payload = [
            item for item in payload
            if isinstance(item, dict)
            and any(q in str(v).lower() for v in item.values())
        ]

    if isinstance(payload, str):
        return json.dumps({
            "subject": kb.subject,
            "kb_version": kb.kb_version,
            "section": s,
            "content": payload,
        }, ensure_ascii=False)
    return json.dumps({
        "subject": kb.subject,
        "kb_version": kb.kb_version,
        "section": s,
        "data": payload,
    }, ensure_ascii=False, indent=2)


# ============================================================
# Tool: kb_validate_proposal  (hard-constraint gate)
# ============================================================
def kb_validate_proposal(
    subject: Annotated[str, "Strategy subject, e.g. 'b1_v3'."],
    proposal_json: Annotated[str, "JSON string of the proposal. See ag2_research.knowledge_base.proposal_validator for schema."],
) -> str:
    """Check a research proposal against the strategy's hard constraints.

    ALWAYS call this BEFORE submitting a proposal. The same validator is
    enforced at the patch-executor gate; if you skip this check you will
    waste a research cycle.

    Returns JSON with verdict in {"allow", "reject", "needs_evidence"},
    plus violations / warnings / reasons / kb_version.
    """
    from ag2_research.knowledge_base import validate_proposal, list_subjects

    try:
        proposal = json.loads(proposal_json)
    except json.JSONDecodeError as e:
        return json.dumps({
            "verdict": "reject",
            "reasons": [f"proposal_json is not valid JSON: {e}"],
        }, ensure_ascii=False)

    # Fast-path: no KB for this subject -> auto-allow with a warning
    if subject not in list_subjects():
        return json.dumps({
            "verdict": "allow",
            "violations": [],
            "warnings": ["KB_SUBJECT_NOT_REGISTERED"],
            "needs_evidence": [],
            "reasons": [f"no knowledge base registered for subject '{subject}'; proceeding without validation"],
            "available_subjects": list_subjects(),
            "kb_version": None,
            "subject": subject,
        }, ensure_ascii=False)

    verdict = validate_proposal(subject, proposal)
    return json.dumps(verdict, ensure_ascii=False, indent=2)


# ============================================================
# Tool registry
# ============================================================
TOOL_REGISTRY: dict[str, Any] = {
    "get_strategy_config": get_strategy_config,
    "run_backtest": run_backtest,
    "list_research_docs": list_research_docs,
    "read_research_doc": read_research_doc,
    "get_market_calendar": get_market_calendar,
    "check_market_timing": check_market_timing,
    "list_available_data": list_available_data,
    "list_code": list_code,
    "read_code": read_code,
    "book_index": book_index,
    "book_open": book_open,
    "book_search": book_search,
    "kbase_overview": kbase_overview,
    "kbase_browse": kbase_browse,
    "kbase_search": kbase_search,
    "kbase_open": kbase_open,
    "kbase_trace": kbase_trace,
    "kb_validated_claims": kb_validated_claims,
    "kb_lookup": kb_lookup,
    "kb_validate_proposal": kb_validate_proposal,
}


def get_tools_for_agent(tool_names: list[str]) -> list:
    """Return the tool function objects for a given list of tool names."""
    return [TOOL_REGISTRY[name] for name in tool_names if name in TOOL_REGISTRY]
