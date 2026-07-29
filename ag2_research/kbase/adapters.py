"""Read-only adapters from heterogeneous KBase objects to catalog entries."""
from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

import yaml

from .schemas import validate_catalog_entry


DATE_PATTERNS = (
    re.compile(r"(?<!\d)(20\d{2})[-./年](\d{1,2})[-./月](\d{1,2})(?:日)?(?!\d)"),
    re.compile(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)"),
)
RELIABILITY = {"low", "medium", "high", "unverified"}
REVIEW_STATUS = {"source_only", "review_required", "reviewed", "blocked"}
LAYER_NAMES = {"summary", "statements", "evidence", "raw", "visual"}
STATEMENT_FIELDS = ("methods", "claims", "risks", "contradictions", "definitions", "examples")
_GENERIC_STATEMENT_KEYS = ("text", "claim", "description", "content")


def statement_text(item: dict[str, Any], field: str | None = None) -> str:
    """Read an explicit legacy statement; never infer one from summary/quotes."""
    keys = list(_GENERIC_STATEMENT_KEYS)
    if field:
        singular = field[:-1] if field.endswith("s") else field
        keys.extend((singular, f"{singular}_text", f"{singular}_content",
                     f"{singular}_description", f"{singular}_desc"))
    for key in keys:
        value = item.get(key)
        if isinstance(value, (str, int, float)) and str(value).strip():
            return str(value).strip()
    return ""


def normalize_statement_item(item: dict[str, Any], field: str) -> dict[str, Any]:
    """Add canonical ``text`` only when an explicit statement alias exists."""
    result = dict(item)
    text = statement_text(item, field)
    if text and not str(result.get("text") or "").strip():
        result["text"] = text
    return result


def _relative(path: Path, vault: Path) -> str:
    return str(path.resolve().relative_to(vault.resolve())).replace("\\", "/")


def _fingerprint_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        value = [value]
    result: list[str] = []
    for item in value:
        if isinstance(item, dict):
            text = item.get("text") or item.get("name") or item.get("title")
        else:
            text = item
        text = str(text or "").strip()
        if text and text not in result:
            result.append(text)
    return result


def _extract_date(*values: Any) -> str | None:
    text = " ".join(str(value or "") for value in values)
    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            year, month, day = (int(part) for part in match.groups())
            if 1 <= month <= 12 and 1 <= day <= 31:
                return f"{year:04d}-{month:02d}-{day:02d}"
    return None


def _review_status(record: dict[str, Any], warnings: list[str]) -> str:
    flags = _strings(record.get("review_flags"))
    if flags or warnings:
        return "review_required"
    return "source_only"


def discover_source_packets(vault: Path) -> list[Path]:
    """Return canonical packet files, excluding checkpoints and repair backups."""
    patterns = (
        "raw/imports/*/distillation/source-packets/*.json",
        "wiki/outputs/source-packets/intake/*.json",
    )
    paths = {path.resolve() for pattern in patterns for path in vault.glob(pattern)}
    return sorted(paths, key=lambda path: str(path).lower())


def load_raw_path_index(vault: Path) -> dict[str, str]:
    """Map source hashes to immutable raw objects using import manifests."""
    result: dict[str, str] = {}
    imports = vault / "raw" / "imports"
    if not imports.is_dir():
        return result
    for import_root in sorted(path for path in imports.iterdir() if path.is_dir()):
        manifest = import_root / "manifest.csv"
        if manifest.is_file():
            try:
                with manifest.open(encoding="utf-8-sig", newline="") as handle:
                    for row in csv.DictReader(handle):
                        sha = str(row.get("sha256") or "").lower()
                        raw_path = str(row.get("kbase_object") or "").replace("\\", "/")
                        if len(sha) == 64 and raw_path:
                            result[sha] = raw_path
            except (OSError, UnicodeError, csv.Error):
                pass
        inventory = import_root / "inventory.json"
        if inventory.is_file():
            try:
                data = json.loads(inventory.read_text(encoding="utf-8"))
                for item in data.get("items", []):
                    sha = str(item.get("sha256") or "").lower()
                    raw_path = str(item.get("raw_path") or "").replace("\\", "/")
                    if len(sha) == 64 and raw_path:
                        result[sha] = raw_path
            except (OSError, UnicodeError, json.JSONDecodeError, AttributeError):
                pass
    return result


def _family_preference(family: dict[str, Any]) -> tuple[int, int, str]:
    method = str(family.get("method") or "")
    role = str(family.get("role") or "")
    return (
        0 if method == "primary_person" and role == "primary" else
        1 if method == "exact_family_key" and role == "primary" else
        2 if role == "primary" else 3,
        -int(family.get("members") or 0),
        str(family.get("family_id") or ""),
    )


def load_family_index(vault: Path) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    families: dict[str, dict[str, Any]] = {}
    root = vault / "wiki" / "outputs" / "source-families" / "manifest"
    if not root.is_dir():
        return {}, {}
    for path in sorted(root.rglob("source-family-manifest.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        for family in data.get("families", []):
            family_id = str(family.get("family_id") or "").strip()
            if not family_id:
                continue
            current = families.get(family_id)
            if current is None or int(family.get("members") or 0) > int(current.get("members") or 0):
                families[family_id] = family
            for sha in family.get("member_shas", []):
                sha = str(sha).lower()
                if family_id not in {item.get("family_id") for item in by_source[sha]}:
                    by_source[sha].append(family)
    for values in by_source.values():
        values.sort(key=_family_preference)
    return dict(by_source), families


def _blocked_packet_entry(path: Path, vault: Path, data: bytes, message: str) -> dict[str, Any]:
    stem = path.stem.lower()
    source_id = stem if re.fullmatch(r"[0-9a-f]{64}", stem) else "packet:" + _fingerprint_bytes(data)
    entry = {
        "catalog_schema_version": 1,
        "source_id": source_id,
        "object_type": "source_packet",
        "title": path.stem,
        "aliases": [],
        "people": [],
        "family_id": None,
        "voice_role": "unknown",
        "source_type": "unknown",
        "date_start": None,
        "date_end": None,
        "topics": [],
        "summary": "",
        "reliability": "unverified",
        "review_status": "blocked",
        "available_layers": [],
        "warnings": [message],
        "parent_ids": [],
        "paths": {"packet": _relative(path, vault)},
        "content_fingerprint": _fingerprint_bytes(data),
        "source_schema_version": None,
    }
    validate_catalog_entry(entry)
    return entry


def adapt_source_packet(
    path: Path,
    vault: Path,
    *,
    raw_paths: dict[str, str] | None = None,
    family_membership: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Read a V1/V2/partially malformed packet without modifying it."""
    raw_paths = raw_paths or {}
    family_membership = family_membership or {}
    try:
        data = path.read_bytes()
    except OSError as error:
        data = b""
        return _blocked_packet_entry(path, vault, data, f"packet_read_error:{error}")
    try:
        document = json.loads(data.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        return _blocked_packet_entry(path, vault, data, f"packet_parse_error:{error}")
    if not isinstance(document, dict):
        return _blocked_packet_entry(path, vault, data, "packet_root_not_object")

    warnings: list[str] = []
    version = document.get("schema_version")
    if version not in {1, 2}:
        warnings.append(f"unsupported_source_schema:{version}")
    record = document.get("record")
    if not isinstance(record, dict):
        record = {}
        warnings.append("missing_record")
    sha = str(document.get("sha256") or path.stem).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", sha):
        sha = path.stem.lower() if re.fullmatch(r"[0-9a-f]{64}", path.stem.lower()) else "packet:" + _fingerprint_bytes(data)
        warnings.append("invalid_source_sha")

    title = str(record.get("canonical_title") or path.stem).strip()
    if not record.get("canonical_title"):
        warnings.append("missing_canonical_title")
    summary = str(record.get("summary") or "").strip()
    if not summary:
        warnings.append("missing_summary")
    aliases = _strings(record.get("aliases"))
    people = _strings(record.get("primary_people"))
    topics = _strings(record.get("topics"))
    source_type = str(record.get("source_type") or document.get("kind") or "unknown").strip()
    voice_role = str(record.get("source_role") or "unknown").strip()
    reliability = str(record.get("reliability") or "unverified").lower()
    if reliability not in RELIABILITY:
        reliability = "unverified"
        warnings.append("invalid_reliability")

    evidence_items: list[tuple[str, dict[str, Any]]] = []
    for key in STATEMENT_FIELDS:
        values = record.get(key)
        if isinstance(values, list):
            evidence_items.extend((key, item) for item in values if isinstance(item, dict))
        elif values is not None:
            warnings.append(f"invalid_record_field:{key}")
    layers: set[str] = set()
    if summary:
        layers.add("summary")
    if any(statement_text(item, field) for field, item in evidence_items):
        layers.add("statements")
    if any(statement_text(item, field) and (item.get("evidence_anchor") or item.get("evidence_quote"))
           for field, item in evidence_items):
        layers.add("evidence")

    paths = {"packet": _relative(path, vault)}
    raw_path = raw_paths.get(sha)
    original_path = str(document.get("original_path") or "").replace("\\", "/")
    if not raw_path and original_path.lower().startswith("raw/"):
        raw_path = original_path
    if raw_path:
        paths["raw"] = raw_path
        layers.add("raw")
    else:
        warnings.append("raw_path_unresolved")
    extraction_layers = _strings(document.get("extraction_layers"))
    if any("visual" in layer or "image" in layer for layer in extraction_layers):
        layers.add("visual")

    family_values = family_membership.get(sha, [])
    parent_ids = [str(item.get("family_id")) for item in family_values if item.get("family_id")]
    family_id = parent_ids[0] if parent_ids else None
    date = _extract_date(original_path, title, aliases)
    warnings.extend(_strings(record.get("review_flags")))
    warnings = list(dict.fromkeys(warnings))

    entry = {
        "catalog_schema_version": 1,
        "source_id": sha,
        "object_type": "source_packet",
        "title": title,
        "aliases": aliases,
        "people": people,
        "family_id": family_id,
        "voice_role": voice_role,
        "source_type": source_type,
        "date_start": date,
        "date_end": date,
        "topics": topics,
        "summary": summary,
        "reliability": reliability,
        "review_status": "blocked" if not record else _review_status(record, warnings),
        "available_layers": sorted(layers & LAYER_NAMES),
        "warnings": warnings,
        "parent_ids": parent_ids,
        "paths": paths,
        "content_fingerprint": _fingerprint_bytes(data),
        "source_schema_version": version,
    }
    validate_catalog_entry(entry)
    return entry


def family_catalog_entries(families: dict[str, dict[str, Any]]) -> Iterable[dict[str, Any]]:
    for family_id, family in sorted(families.items()):
        members = sorted(str(item).lower() for item in family.get("member_shas", []))
        fingerprint = _fingerprint_bytes((family_id + "\n" + "\n".join(members)).encode("utf-8"))
        entry = {
            "catalog_schema_version": 1,
            "source_id": family_id,
            "object_type": "family",
            "title": str(family.get("name") or family_id),
            "aliases": [],
            "people": [str(family.get("name"))] if family.get("method") == "primary_person" else [],
            "family_id": family_id,
            "voice_role": str(family.get("role") or "unknown"),
            "source_type": "source_family",
            "date_start": None,
            "date_end": None,
            "topics": [],
            "summary": f"来源家族，共 {int(family.get('members') or len(members))} 个成员。",
            "reliability": "unverified",
            "review_status": "source_only",
            "available_layers": ["summary"],
            "warnings": [],
            "parent_ids": [],
            "paths": {"manifest": "wiki/outputs/source-families/manifest"},
            "content_fingerprint": fingerprint,
            "source_schema_version": 1,
        }
        validate_catalog_entry(entry)
        yield entry


def _parse_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    match = re.match(r"---\s*\r?\n(.*?)\r?\n---\s*\r?\n", text, flags=re.S)
    if not match:
        return {}, text
    try:
        data = yaml.safe_load(match.group(1)) or {}
    except yaml.YAMLError:
        return {}, text
    return (data if isinstance(data, dict) else {}), text[match.end():]


def adapt_wiki_page(path: Path, vault: Path, object_type: str) -> dict[str, Any]:
    data = path.read_bytes()
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        text = data.decode("gbk", errors="replace")
    frontmatter, body = _parse_frontmatter(text)
    title_match = re.search(r"(?m)^#\s+(.+?)\s*$", body)
    title = str(frontmatter.get("title") or (title_match.group(1) if title_match else path.stem)).strip()
    paragraphs = [item.strip() for item in re.split(r"\n\s*\n", body) if item.strip() and not item.lstrip().startswith("#")]
    summary = re.sub(r"\s+", " ", paragraphs[0])[:1200] if paragraphs else ""
    source_id = str(frontmatter.get("source_id") or frontmatter.get("id") or "wiki:" + _fingerprint_bytes(_relative(path, vault).encode("utf-8")))
    status = str(frontmatter.get("status") or "source_only").lower()
    if status not in REVIEW_STATUS:
        status = "source_only"
    reliability = str(frontmatter.get("reliability") or frontmatter.get("confidence") or "unverified").lower()
    if reliability not in RELIABILITY:
        reliability = "unverified"
    entry = {
        "catalog_schema_version": 1,
        "source_id": source_id,
        "object_type": object_type,
        "title": title,
        "aliases": _strings(frontmatter.get("aliases")),
        "people": _strings(frontmatter.get("people") or frontmatter.get("author")),
        "family_id": str(frontmatter.get("family_id")) if frontmatter.get("family_id") else None,
        "voice_role": str(frontmatter.get("voice_role") or "unknown"),
        "source_type": str(frontmatter.get("source_type") or object_type),
        "date_start": _extract_date(frontmatter.get("date_start"), frontmatter.get("date"), title),
        "date_end": _extract_date(frontmatter.get("date_end"), frontmatter.get("date"), title),
        "topics": _strings(frontmatter.get("topics") or frontmatter.get("tags")),
        "summary": summary,
        "reliability": reliability,
        "review_status": status,
        "available_layers": ["summary", "raw"] if summary else ["raw"],
        "warnings": [] if frontmatter else ["missing_frontmatter"],
        "parent_ids": _strings(frontmatter.get("parent_ids")),
        "paths": {"wiki": _relative(path, vault)},
        "content_fingerprint": _fingerprint_bytes(data),
        "source_schema_version": str(frontmatter.get("schema_version")) if frontmatter.get("schema_version") is not None else None,
    }
    validate_catalog_entry(entry)
    return entry
