"""Local-only new-resource intake for immediate KBase discoverability."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from .catalog_builder import publish_catalog


TEXT_SUFFIXES = {".txt", ".md", ".html", ".htm"}
PDF_SUFFIXES = {".pdf"}
VIDEO_SUFFIXES = {".mp4", ".avi", ".mov", ".mkv", ".wmv", ".flv"}
AUDIO_SUFFIXES = {".mp3", ".wav", ".m4a", ".flac", ".aac"}
IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".gif"}
OFFICE_SUFFIXES = {".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx"}
DEFAULT_INTAKE_SUFFIXES = (
    TEXT_SUFFIXES | PDF_SUFFIXES | VIDEO_SUFFIXES | AUDIO_SUFFIXES |
    IMAGE_SUFFIXES | OFFICE_SUFFIXES
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _kind(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in TEXT_SUFFIXES:
        return "text"
    if suffix in PDF_SUFFIXES:
        return "pdf"
    if suffix in VIDEO_SUFFIXES:
        return "video"
    if suffix in AUDIO_SUFFIXES:
        return "audio"
    if suffix in IMAGE_SUFFIXES:
        return "image"
    if suffix in OFFICE_SUFFIXES:
        return "office"
    return "binary"


def _write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_text(path: Path) -> tuple[str, list[str], list[str]]:
    for encoding in ("utf-8", "gbk"):
        try:
            text = path.read_text(encoding=encoding)
            lines = text.splitlines()
            layers = ["direct_text"]
            return text, layers, []
        except UnicodeDecodeError:
            continue
    return "", [], ["text_decode_failed"]


def _read_pdf(path: Path) -> tuple[str, list[str], list[str]]:
    try:
        from pypdf import PdfReader

        reader = PdfReader(str(path))
        pages = []
        for page_number, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""
            if text.strip():
                pages.append(f"[PAGE {page_number}]\n{text}")
        warnings = [] if pages else ["pdf_has_no_extractable_text; OCR_or_visual_review_required"]
        return "\n\n".join(pages), ["layout_text"], warnings
    except Exception as error:
        return "", [], [f"pdf_extraction_failed:{type(error).__name__}"]


def _media_metadata(path: Path) -> tuple[dict[str, Any], list[str]]:
    command = [
        "ffprobe", "-v", "error", "-show_entries",
        "format=duration,format_name:stream=codec_type,codec_name,width,height",
        "-of", "json", str(path),
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
        if result.returncode == 0:
            return json.loads(result.stdout or "{}"), []
        return {}, ["media_metadata_unreadable; ASR_and_visual_processing_pending"]
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        return {}, ["ffprobe_unavailable_or_failed; ASR_and_visual_processing_pending"]


def _extract(path: Path, kind: str) -> tuple[str, list[str], list[str], dict[str, Any]]:
    if kind == "text":
        text, layers, warnings = _read_text(path)
        return text, layers, warnings, {}
    if kind == "pdf":
        text, layers, warnings = _read_pdf(path)
        return text, layers, warnings, {}
    if kind in {"video", "audio"}:
        metadata, warnings = _media_metadata(path)
        return "", ["media_metadata"], warnings, metadata
    return "", ["file_metadata"], [f"{kind}_content_extraction_pending"], {}


def _evidence_from_text(text: str) -> list[dict[str, str]]:
    lines = text.splitlines()
    first = next(((index, line.strip()) for index, line in enumerate(lines, start=1) if line.strip()), None)
    if not first:
        return []
    line_number, excerpt = first
    excerpt = excerpt[:1000]
    return [{
        "text": excerpt,
        "evidence_anchor": f"[L{line_number}]",
        "evidence_quote": excerpt,
        "certainty": "low",
        "source_voice": "source",
    }]


def register_resource(
    source_path: str | Path,
    *,
    vault_path: str | Path = r"D:\KBase",
    publish: bool = False,
    external_upload_authorized: bool = False,
) -> dict[str, Any]:
    """Preserve one local resource, create a conservative packet, and optionally publish.

    No external model is called. ``external_upload_authorized`` is recorded only
    as batch-specific consent metadata and never persists as a global switch.
    """
    source = Path(source_path).resolve()
    vault = Path(vault_path).resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    source_hash = _sha256(source)
    kind = _kind(source)
    raw_relative = Path("raw") / "incoming" / source_hash[:2] / f"{source_hash}{source.suffix.lower()}"
    raw_target = vault / raw_relative
    packet_relative = Path("wiki") / "outputs" / "source-packets" / "intake" / f"{source_hash}.json"
    packet_target = vault / packet_relative
    state_target = vault / "wiki" / "outputs" / "manifests" / "ag2-kbase" / "intake" / f"{source_hash}.json"

    duplicate = raw_target.exists()
    if duplicate:
        if _sha256(raw_target) != source_hash:
            raise ValueError(f"hash collision or corrupted immutable target: {raw_target}")
    else:
        raw_target.parent.mkdir(parents=True, exist_ok=True)
        temporary = raw_target.with_name(raw_target.name + ".copying")
        if temporary.exists():
            raise FileExistsError(f"stale intake temporary requires review: {temporary}")
        shutil.copy2(source, temporary)
        if _sha256(temporary) != source_hash:
            temporary.unlink(missing_ok=True)
            raise ValueError("copied raw object failed hash verification")
        os.replace(temporary, raw_target)

    text, layers, warnings, metadata = _extract(raw_target, kind)
    summary = " ".join(text.split())[:1200]
    if not summary:
        summary = f"{kind} resource registered locally; deeper modality extraction is pending."
    evidence = _evidence_from_text(text)
    review_flags = list(warnings)
    if kind in {"video", "audio"}:
        review_flags.append("timestamped_transcript_pending")
    if kind == "video":
        review_flags.append("visual_review_pending")
    packet = {
        "schema_version": 2,
        "pipeline_revision": 1,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "sha256": source_hash,
        "original_path": str(raw_relative).replace("\\", "/"),
        "kind": kind,
        "use_mode": "reference",
        "extraction_layers": layers,
        "provider": "local_deterministic",
        "model": None,
        "record": {
            "canonical_title": source.stem,
            "title_basis": "local filename; identity review pending",
            "aliases": [source.name],
            "source_type": kind,
            "source_role": "unknown",
            "primary_people": [],
            "topics": [],
            "summary": summary,
            "methods": [],
            "claims": evidence,
            "risks": [],
            "contradictions": [],
            "visual_gaps": ["visual content not reviewed"] if kind in {"video", "image", "pdf"} else [],
            "advertising": [],
            "reliability": "unverified",
            "reliability_reasons": ["new deterministic intake; human/source-family review pending"],
            "family_key": None,
            "merge_recommendation": "identity and family assignment pending",
            "review_flags": list(dict.fromkeys(review_flags)),
        },
        "local_metadata": metadata,
    }
    _write_json_atomic(packet_target, packet)
    state = {
        "source_id": source_hash,
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "state_history": [
            "received", "identified", "raw_preserved", "locally_extracted", "source_represented",
        ],
        "ag2_discoverable": False,
        "content_status": "review_required" if review_flags else "locally_extracted",
        "kind": kind,
        "duplicate_raw": duplicate,
        "raw_path": str(raw_relative).replace("\\", "/"),
        "packet_path": str(packet_relative).replace("\\", "/"),
        "external_upload_authorized_for_this_intake": bool(external_upload_authorized),
        "external_upload_used": False,
        "pending": list(dict.fromkeys(review_flags)),
    }
    publication = None
    if publish:
        publication = publish_catalog(vault)
        state["ag2_discoverable"] = bool(publication.get("published"))
        if state["ag2_discoverable"]:
            state["state_history"].append("catalog_published")
            state["catalog_version"] = publication["manifest"]["catalog_version"]
    _write_json_atomic(state_target, state)
    return {"state": state, "packet": packet, "publication": publication}


def _normalise_suffixes(suffixes: list[str] | set[str] | None) -> set[str]:
    values = DEFAULT_INTAKE_SUFFIXES if suffixes is None else suffixes
    return {
        value.lower() if value.startswith(".") else f".{value.lower()}"
        for value in values if value.strip()
    }


def _find_sources(source_dir: Path, *, recursive: bool) -> list[Path]:
    iterator = source_dir.rglob("*") if recursive else source_dir.glob("*")
    return sorted((path for path in iterator if path.is_file()), key=lambda path: str(path).lower())


def register_directory(
    source_dir: str | Path,
    *,
    vault_path: str | Path = r"D:\KBase",
    recursive: bool = True,
    suffixes: list[str] | set[str] | None = None,
    publish: bool = False,
    dry_run: bool = False,
    external_upload_authorized: bool = False,
) -> dict[str, Any]:
    """Register a directory as one resumable, failure-isolated intake batch.

    Files are content-addressed, so rerunning a batch reuses complete prior
    intake objects. Catalog publication happens at most once after all files.
    A dry run only scans and hashes sources; it creates no files in the vault.
    """
    source_root = Path(source_dir).resolve()
    vault = Path(vault_path).resolve()
    if not source_root.is_dir():
        raise NotADirectoryError(source_root)
    allowed = _normalise_suffixes(suffixes)
    discovered = _find_sources(source_root, recursive=recursive)
    selected = [path for path in discovered if path.suffix.lower() in allowed]
    results: list[dict[str, Any]] = []

    for source in selected:
        relative = str(source.relative_to(source_root)).replace("\\", "/")
        item: dict[str, Any] = {"source": relative, "status": "pending"}
        try:
            source_hash = _sha256(source)
            item["source_id"] = source_hash
            raw = vault / "raw" / "incoming" / source_hash[:2] / f"{source_hash}{source.suffix.lower()}"
            packet = vault / "wiki" / "outputs" / "source-packets" / "intake" / f"{source_hash}.json"
            state = vault / "wiki" / "outputs" / "manifests" / "ag2-kbase" / "intake" / f"{source_hash}.json"
            complete = raw.is_file() and packet.is_file() and state.is_file()
            if complete and _sha256(raw) != source_hash:
                raise ValueError(f"corrupted immutable intake object: {raw}")
            if dry_run:
                item["status"] = "would_reuse" if complete else "would_register"
            elif complete:
                item["status"] = "reused"
            else:
                registered = register_resource(
                    source,
                    vault_path=vault,
                    publish=False,
                    external_upload_authorized=external_upload_authorized,
                )
                item["status"] = "registered"
                item["content_status"] = registered["state"]["content_status"]
        except Exception as error:  # one bad file must not abort the batch
            item["status"] = "failed"
            item["error"] = f"{type(error).__name__}: {error}"
        results.append(item)

    publication = None
    successful = sum(item["status"] in {"registered", "reused"} for item in results)
    if publish and not dry_run and successful:
        publication = publish_catalog(vault)

    counts: dict[str, int] = {}
    for item in results:
        counts[item["status"]] = counts.get(item["status"], 0) + 1
    generated_at = dt.datetime.now(dt.timezone.utc)
    batch_key = hashlib.sha256(
        (str(source_root) + "\n" + "\n".join(item["source"] for item in results)).encode("utf-8")
    ).hexdigest()[:16]
    summary = {
        "schema_version": 1,
        "batch_id": f"{generated_at.strftime('%Y%m%dT%H%M%SZ')}-{batch_key}",
        "generated_at": generated_at.isoformat(),
        "source_root": str(source_root),
        "recursive": recursive,
        "allowed_suffixes": sorted(allowed),
        "dry_run": dry_run,
        "discovered_files": len(discovered),
        "selected_files": len(selected),
        "skipped_by_suffix": len(discovered) - len(selected),
        "counts": counts,
        "files": results,
        "publication": publication,
    }
    if not dry_run:
        summary_path = (
            vault / "wiki" / "outputs" / "manifests" / "ag2-kbase" /
            "intake-batches" / f"{summary['batch_id']}.json"
        )
        _write_json_atomic(summary_path, summary)
        summary["summary_path"] = str(summary_path)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source")
    parser.add_argument("--vault", default=r"D:\KBase")
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--recursive", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--extensions", help="comma-separated extension whitelist")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-external-upload", action="store_true")
    args = parser.parse_args()
    source = Path(args.source)
    if source.is_dir():
        extensions = args.extensions.split(",") if args.extensions else None
        result = register_directory(
            source,
            vault_path=args.vault,
            recursive=args.recursive,
            suffixes=extensions,
            publish=args.publish,
            dry_run=args.dry_run,
            external_upload_authorized=args.allow_external_upload,
        )
    else:
        if args.dry_run:
            raise SystemExit("--dry-run is supported for directory intake")
        result = register_resource(
            source,
            vault_path=args.vault,
            publish=args.publish,
            external_upload_authorized=args.allow_external_upload,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
