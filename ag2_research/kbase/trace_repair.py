"""Read-only trace diagnosis and isolated catalog repair-candidate generation."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .coverage import build_navigation_coverage
from .repository import KBaseRepository


TRACE_REASONS = {"missing_raw", "raw_file_missing", "packet_unreadable", "packet_path_missing"}


def _relative(path: Path, vault: Path) -> str:
    return str(path.resolve().relative_to(vault.resolve())).replace("\\", "/")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _packet_index(vault: Path) -> dict[str, list[Path]]:
    roots = (
        vault / "raw" / "imports",
        vault / "wiki" / "outputs" / "source-packets" / "intake",
    )
    result: dict[str, list[Path]] = defaultdict(list)
    for root in roots:
        if not root.is_dir():
            continue
        for path in root.rglob("*.json"):
            if len(path.stem) == 64 and all(char in "0123456789abcdefABCDEF" for char in path.stem):
                result[path.stem.lower()].append(path.resolve())
    return result


def _candidate_raw_files(path: Path) -> list[tuple[Path, str]]:
    """Return only exact/content-addressed or conventional container members."""
    if path.is_file():
        return [(path, "declared_raw_file")]
    if not path.is_dir():
        return []
    candidates: list[tuple[Path, str]] = []
    # Video imports use a source container. This is an explicit layout contract,
    # not a fuzzy basename/content search.
    source_files = sorted(item for item in path.glob("source.*") if item.is_file())
    if len(source_files) == 1:
        candidates.append((source_files[0], "unique_source_file_in_declared_container"))
    for relative, basis in (
        (Path("transcript/asr-transcript.md"), "declared_container_asr_transcript"),
        (Path("metadata.json"), "declared_container_metadata"),
    ):
        item = path / relative
        if item.is_file():
            candidates.append((item, basis))
    return candidates


def _locate_raw(vault: Path, entry: dict[str, Any], packet: dict[str, Any]) -> dict[str, Any]:
    source_id = str(entry.get("source_id") or "").lower()
    declared: list[tuple[Path, str]] = []
    raw_path = str(entry.get("paths", {}).get("raw") or "").strip()
    if raw_path:
        declared.append((vault / raw_path, "catalog_raw_path"))
    original = str(packet.get("original_path") or "").replace("\\", "/").strip()
    if original and original.lower().startswith("raw/"):
        declared.append((vault / original, "packet_original_path"))
    if len(source_id) == 64:
        incoming = vault / "raw" / "incoming" / source_id[:2]
        if incoming.is_dir():
            for item in incoming.glob(source_id + ".*"):
                if item.is_file() and _sha256(item) == source_id:
                    return {"status": "recoverable", "path": _relative(item, vault), "basis": "sha256_content_addressed"}

    seen: set[Path] = set()
    candidates: list[tuple[Path, str]] = []
    for declared_path, origin in declared:
        for item, basis in _candidate_raw_files(declared_path):
            resolved = item.resolve()
            if resolved not in seen:
                seen.add(resolved)
                candidates.append((resolved, f"{origin}:{basis}"))
    source_candidates = [(path, basis) for path, basis in candidates if "unique_source_file" in basis]
    if len(source_candidates) == 1:
        path, basis = source_candidates[0]
        return {"status": "recoverable", "path": _relative(path, vault), "basis": basis}
    if len(candidates) == 1:
        path, basis = candidates[0]
        return {"status": "recoverable", "path": _relative(path, vault), "basis": basis}
    if candidates:
        return {
            "status": "ambiguous", "reason": "multiple_exact_declared_container_members",
            "candidates": [_relative(path, vault) for path, _ in candidates],
        }
    return {"status": "unrecoverable", "reason": "no_exact_hash_or_declared_path_candidate"}


def generate_trace_repair_candidate(
    *, vault_path: str | Path = r"D:\KBase", output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Diagnose published trace gaps and write a non-published candidate catalog."""
    vault = Path(vault_path).resolve()
    repo = KBaseRepository(vault)
    coverage = build_navigation_coverage(vault_path=vault)
    entries = list(repo.entries())
    by_id = {str(entry.get("source_id")): entry for entry in entries}
    packets = _packet_index(vault)
    repairs: dict[str, dict[str, str]] = {}
    findings: list[dict[str, Any]] = []

    for row in coverage["packets"]:
        relevant = sorted(TRACE_REASONS & set(row.get("reasons", [])))
        if not relevant:
            continue
        source_id = str(row["source_id"])
        entry = by_id[source_id]
        packet_path = str(entry.get("paths", {}).get("packet") or "")
        packet: dict[str, Any] = {}
        packet_resolution: dict[str, Any] = {"status": "unchanged"}
        try:
            if packet_path:
                value = json.loads(repo.safe_path(packet_path).read_text(encoding="utf-8"))
                if isinstance(value, dict):
                    packet = value
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
            pass
        if "packet_path_missing" in relevant or "packet_unreadable" in relevant:
            alternatives = []
            for path in packets.get(source_id.lower(), []):
                try:
                    value = json.loads(path.read_text(encoding="utf-8"))
                    if isinstance(value, dict) and str(value.get("sha256") or path.stem).lower() == source_id.lower():
                        alternatives.append(path)
                except (OSError, UnicodeError, json.JSONDecodeError):
                    continue
            if len(alternatives) == 1:
                packet_resolution = {"status": "recoverable", "path": _relative(alternatives[0], vault), "basis": "exact_source_id_packet"}
                packet = json.loads(alternatives[0].read_text(encoding="utf-8"))
            else:
                packet_resolution = {
                    "status": "ambiguous" if alternatives else "unrecoverable",
                    "reason": "multiple_exact_source_id_packets" if alternatives else "no_readable_exact_source_id_packet",
                }
        raw_resolution = _locate_raw(vault, entry, packet) if {"missing_raw", "raw_file_missing"} & set(relevant) else {"status": "unchanged"}
        patch: dict[str, str] = {}
        if packet_resolution.get("status") == "recoverable":
            patch["packet"] = packet_resolution["path"]
        if raw_resolution.get("status") == "recoverable":
            patch["raw"] = raw_resolution["path"]
        status = "recoverable" if patch and all(
            resolution.get("status") in {"recoverable", "unchanged"}
            for resolution in (packet_resolution, raw_resolution)
        ) else "unresolved"
        if status == "recoverable":
            repairs[source_id] = patch
        findings.append({
            "source_id": source_id, "reasons": relevant, "status": status,
            "packet_resolution": packet_resolution, "raw_resolution": raw_resolution,
            "candidate_patch": {"paths": patch} if patch else None,
        })

    candidate_entries: list[dict[str, Any]] = []
    for entry in entries:
        candidate = json.loads(json.dumps(entry, ensure_ascii=False))
        patch = repairs.get(str(entry.get("source_id")))
        if patch:
            candidate.setdefault("paths", {}).update(patch)
            warnings = list(candidate.get("warnings") or [])
            candidate["warnings"] = [warning for warning in warnings if warning != "raw_path_unresolved"]
        candidate_entries.append(candidate)

    now = dt.datetime.now(dt.timezone.utc)
    root = Path(output_root).resolve() if output_root else (
        vault / "wiki" / "outputs" / "manifests" / "ag2-kbase" / "repair-candidates"
    )
    candidate_dir = root / f"trace-{now.strftime('%Y%m%dT%H%M%SZ')}"
    candidate_dir.mkdir(parents=True, exist_ok=False)
    catalog_path = candidate_dir / "catalog.jsonl"
    with catalog_path.open("w", encoding="utf-8", newline="\n") as handle:
        for entry in candidate_entries:
            handle.write(json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n")
    counts = Counter(item["status"] for item in findings)
    reasons = Counter(
        resolution.get("reason")
        for item in findings for resolution in (item["packet_resolution"], item["raw_resolution"])
        if resolution.get("reason")
    )
    report = {
        "schema_version": 1, "generated_at": now.isoformat(),
        "source_catalog_version": coverage.get("catalog_version"),
        "policy": "candidate_only_no_publish_no_raw_write",
        "gap_count": len(findings), "counts": dict(sorted(counts.items())),
        "unresolved_reasons": dict(sorted(reasons.items())), "findings": findings,
        "candidate_catalog": str(catalog_path),
    }
    _atomic_json(candidate_dir / "repair-report.json", report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vault", default=r"D:\KBase")
    parser.add_argument("--output-root")
    args = parser.parse_args()
    print(json.dumps(generate_trace_repair_candidate(vault_path=args.vault, output_root=args.output_root), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
