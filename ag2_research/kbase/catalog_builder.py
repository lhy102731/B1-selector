"""Build and publish the deterministic KBase discovery catalog."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import uuid
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

from .adapters import (
    adapt_source_packet,
    adapt_wiki_page,
    discover_source_packets,
    family_catalog_entries,
    load_family_index,
    load_raw_path_index,
)
from .schemas import validate_catalog_entry
from .overrides import apply_approved_overrides, load_approved_overrides
from research_automation.foundations.immutable_release import ImmutableReleaseStore


GENERATOR_VERSION = "1.0.0-p1"
DEFAULT_VAULT = Path(os.environ.get("KBASE_PATH", r"D:\KBase"))
DEFAULT_OUTPUT = Path("wiki/outputs/manifests/ag2-kbase")


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _json_fingerprint(value: Any) -> str:
    data = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _read_catalog(path: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if not path.is_file():
        return result
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                entry = json.loads(line)
                result[str(entry["source_id"])] = entry
    return result


def _wiki_inputs(vault: Path) -> Iterable[tuple[Path, str]]:
    maps = vault / "wiki" / "maps"
    if maps.is_dir():
        for path in sorted(maps.rglob("*.md")):
            yield path, "map"
    sources = vault / "wiki" / "sources"
    if sources.is_dir():
        for path in sorted(sources.rglob("*.md")):
            yield path, "source_note"


def _facets(entries: list[dict[str, Any]]) -> dict[str, Any]:
    dimensions = {
        "object_type": Counter(),
        "people": Counter(),
        "family_id": Counter(),
        "topics": Counter(),
        "source_type": Counter(),
        "voice_role": Counter(),
        "review_status": Counter(),
        "source_schema_version": Counter(),
        "date_start": Counter(),
    }
    for entry in entries:
        for key in ("object_type", "family_id", "source_type", "voice_role", "review_status", "source_schema_version", "date_start"):
            value = entry.get(key)
            if value is not None:
                dimensions[key][str(value)] += 1
        for key in ("people", "topics"):
            dimensions[key].update(str(value) for value in entry.get(key, []))
    return {
        key: dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))
        for key, counter in dimensions.items()
    }


def build_catalog(vault: Path, *, previous_dir: Path | None = None) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    vault = vault.resolve()
    packets = discover_source_packets(vault)
    raw_paths = load_raw_path_index(vault)
    family_membership, families = load_family_index(vault)
    previous = _read_catalog(previous_dir / "catalog.jsonl") if previous_dir else {}
    previous_by_packet = {
        str(entry.get("paths", {}).get("packet")): entry
        for entry in previous.values()
        if entry.get("paths", {}).get("packet")
    }
    previous_manifest: dict[str, Any] = {}
    if previous_dir and (previous_dir / "manifest.json").is_file():
        try:
            previous_manifest = json.loads((previous_dir / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            previous_manifest = {}
    previous_input_state = previous_manifest.get("input_state", {})
    overrides = load_approved_overrides(vault)
    override_state = _json_fingerprint(overrides)
    override_unchanged = previous_manifest.get("override_state") == override_state
    family_state = _json_fingerprint({key: value.get("member_shas", []) for key, value in sorted(families.items())})
    family_unchanged = previous_manifest.get("family_state") == family_state
    input_state: dict[str, str] = {}
    entries: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    reused = 0

    for path in packets:
        try:
            relative = str(path.resolve().relative_to(vault)).replace("\\", "/")
            stat = path.stat()
            state = f"{stat.st_size}:{stat.st_mtime_ns}"
            input_state[relative] = state
            old = previous_by_packet.get(relative)
            if old and previous_input_state.get(relative) == state and family_unchanged and override_unchanged:
                entry = old
                reused += 1
            else:
                entry = adapt_source_packet(path, vault, raw_paths=raw_paths, family_membership=family_membership)
            entries.append(entry)
        except Exception as error:
            errors.append({"path": str(path), "error": f"{type(error).__name__}: {error}"})

    for entry in family_catalog_entries(families):
        old = previous.get(entry["source_id"])
        if old and old.get("content_fingerprint") == entry["content_fingerprint"] and override_unchanged:
            entry = old
            reused += 1
        entries.append(entry)

    for path, object_type in _wiki_inputs(vault):
        try:
            entry = adapt_wiki_page(path, vault, object_type)
            old = previous.get(entry["source_id"])
            if old and old.get("content_fingerprint") == entry["content_fingerprint"] and override_unchanged:
                entry = old
                reused += 1
            entries.append(entry)
        except Exception as error:
            errors.append({"path": str(path), "error": f"{type(error).__name__}: {error}"})

    entries = apply_approved_overrides(entries, overrides)
    deduped: dict[str, dict[str, Any]] = {}
    duplicates: list[str] = []
    for entry in entries:
        validate_catalog_entry(entry)
        source_id = str(entry["source_id"])
        if source_id in deduped:
            duplicates.append(source_id)
            continue
        deduped[source_id] = entry
    entries = sorted(deduped.values(), key=lambda item: (item["object_type"], item["source_id"]))
    packet_entries = [entry for entry in entries if entry["object_type"] == "source_packet"]
    current_ids = {entry["source_id"] for entry in entries}
    deleted = sorted(set(previous) - current_ids)
    # Catalog version is a contract boundary for Source Briefs.  Any AG2-visible
    # metadata change (including paths-only trace repairs) must invalidate it.
    source_fingerprint = _json_fingerprint(entries)
    report = {
        "generated_at": _now(),
        "input_packet_files": len(packets),
        "catalog_entries": len(entries),
        "source_packet_entries": len(packet_entries),
        "blocked_packet_entries": sum(entry["review_status"] == "blocked" for entry in packet_entries),
        "reused_entries": reused,
        "deleted_entries": deleted,
        "duplicate_source_ids": duplicates,
        "errors": errors,
        "warnings": {
            "entries_with_warnings": sum(bool(entry["warnings"]) for entry in entries),
            "unresolved_raw_paths": sum("raw_path_unresolved" in entry["warnings"] for entry in packet_entries),
        },
    }
    manifest = {
        "catalog_schema_version": 1,
        "generator_version": GENERATOR_VERSION,
        "catalog_version": source_fingerprint[:16],
        "generated_at": report["generated_at"],
        "source_fingerprint": source_fingerprint,
        "family_state": family_state,
        "override_state": override_state,
        "input_state": input_state,
        "previous_catalog_version": None,
        "counts": {
            "input_packets": len(packets),
            "source_packets": len(packet_entries),
            "entries": len(entries),
            "families": sum(entry["object_type"] == "family" for entry in entries),
            "maps": sum(entry["object_type"] == "map" for entry in entries),
            "source_notes": sum(entry["object_type"] == "source_note" for entry in entries),
            "blocked": report["blocked_packet_entries"],
            "errors": len(errors),
            "deleted": len(deleted),
        },
        "build_parameters": {
            "packet_globs": [
                "raw/imports/*/distillation/source-packets/*.json",
                "wiki/outputs/source-packets/intake/*.json",
            ],
            "include_wiki_maps": True,
            "include_wiki_sources": True,
            "raw_mutation": False,
        },
    }
    if previous_manifest:
        manifest["previous_catalog_version"] = previous_manifest.get("catalog_version")
    return entries, manifest, report


def _write_release(directory: Path, entries: list[dict[str, Any]], manifest: dict[str, Any], report: dict[str, Any]) -> None:
    directory.mkdir(parents=True, exist_ok=False)
    with (directory / "catalog.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for entry in entries:
            handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
    (directory / "facets.json").write_text(json.dumps(_facets(entries), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (directory / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (directory / "build-report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _validate_catalog_release(directory: Path) -> dict[str, Any]:
    required = {"catalog.jsonl", "facets.json", "manifest.json", "build-report.json"}
    missing = sorted(name for name in required if not (directory / name).is_file())
    errors: list[str] = [f"missing:{name}" for name in missing]
    entries = []
    if not missing:
        try:
            entries = list(_read_catalog(directory / "catalog.jsonl").values())
            for entry in entries:
                validate_catalog_entry(entry)
            manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
            facets = json.loads((directory / "facets.json").read_text(encoding="utf-8"))
            report = json.loads((directory / "build-report.json").read_text(encoding="utf-8"))
            if manifest.get("source_fingerprint") != _json_fingerprint(entries):
                errors.append("catalog_source_fingerprint_mismatch")
            if facets != _facets(entries):
                errors.append("catalog_facets_mismatch")
            if len(entries) != int(manifest["counts"]["entries"]):
                errors.append("catalog_count_mismatch")
            if int(manifest["counts"]["source_packets"]) != int(report["input_packet_files"]):
                errors.append("packet_coverage_not_100_percent")
            if report.get("errors"):
                errors.append("adapter_errors_present")
        except Exception as error:
            errors.append(f"validation_error:{type(error).__name__}:{error}")
    return {"ok": not errors, "errors": errors, "entries": len(entries)}


def validate_release(directory: Path) -> dict[str, Any]:
    """Return the legacy catalog validation report."""
    return _validate_catalog_release(directory)


class _CatalogReleaseAdapter:
    def validate(self, release: Path) -> str:
        validation = _validate_catalog_release(release)
        if not validation["ok"]:
            raise ValueError(
                "catalog release validation failed: "
                + ",".join(str(error) for error in validation["errors"])
            )
        manifest = json.loads(
            (release / "manifest.json").read_text(encoding="utf-8")
        )
        catalog_version = manifest.get("catalog_version")
        if not isinstance(catalog_version, str) or not catalog_version:
            raise ValueError("catalog release has no immutable catalog_version")
        return catalog_version


def publish_catalog(vault: Path, *, output_relative: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    vault = vault.resolve()
    output = (vault / output_relative).resolve()
    output.relative_to(vault / "wiki" / "outputs")
    adapter = _CatalogReleaseAdapter()
    store = ImmutableReleaseStore(output, adapter=adapter)
    current = output / "current"
    expected_current_id = adapter.validate(current) if current.is_dir() else None
    build_id = dt.datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    candidate = store.stage(build_id)
    entries, manifest, report = build_catalog(
        vault,
        previous_dir=current if current.is_dir() else None,
    )
    _write_release(candidate, entries, manifest, report)
    validation = validate_release(candidate)
    if not validation["ok"]:
        return {
            "published": False,
            "candidate": str(candidate),
            "validation": validation,
            "manifest": manifest,
        }

    store.promote(candidate, expected_current_id=expected_current_id)
    return {
        "published": True,
        "current": str(current),
        "validation": validation,
        "manifest": manifest,
        "report": report,
    }


def rollback_catalog(vault: Path, *, output_relative: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    """Atomically swap current and previous; raw and candidates are untouched."""
    vault = vault.resolve()
    output = (vault / output_relative).resolve()
    output.relative_to(vault / "wiki" / "outputs")
    current, previous = output / "current", output / "previous"
    if not current.is_dir() or not previous.is_dir():
        return {
            "rolled_back": False,
            "error": "both current and previous releases are required",
        }
    adapter = _CatalogReleaseAdapter()
    store = ImmutableReleaseStore(output, adapter=adapter)
    store.rollback(expected_current_id=adapter.validate(current))
    manifest = json.loads((current / "manifest.json").read_text(encoding="utf-8"))
    return {
        "rolled_back": True,
        "catalog_version": manifest.get("catalog_version"),
        "current": str(current),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vault", default=str(DEFAULT_VAULT))
    parser.add_argument("--build-only", action="store_true")
    args = parser.parse_args()
    vault = Path(args.vault)
    if args.build_only:
        entries, manifest, report = build_catalog(vault)
        result = {"manifest": manifest, "report": report, "facets": _facets(entries)}
    else:
        result = publish_catalog(vault)
    printable = dict(result)
    if isinstance(printable.get("manifest"), dict) and "input_state" in printable["manifest"]:
        printable["manifest"] = dict(printable["manifest"])
        printable["manifest"]["input_state_count"] = len(printable["manifest"].pop("input_state"))
    print(json.dumps(printable, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result.get("published", True) else 1)


if __name__ == "__main__":
    main()
