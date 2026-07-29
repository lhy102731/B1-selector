"""Read-only access to the published KBase catalog and source layers."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable


DEFAULT_VAULT = Path(os.environ.get("KBASE_PATH", r"D:\KBase"))
CATALOG_ROOT = Path("wiki/outputs/manifests/ag2-kbase")
SUPPORTED_CATALOG_SCHEMAS = {1}


class CatalogUnavailableError(RuntimeError):
    pass


class KBaseRepository:
    def __init__(self, vault_path: str | Path | None = None, *, release_dir: str | Path | None = None):
        self.vault = Path(vault_path or DEFAULT_VAULT).resolve()
        self.release_dir = Path(release_dir).resolve() if release_dir else self._select_release()
        self.manifest = json.loads((self.release_dir / "manifest.json").read_text(encoding="utf-8"))
        self.facets = json.loads((self.release_dir / "facets.json").read_text(encoding="utf-8"))
        self._entries: dict[str, dict[str, Any]] = {}
        with (self.release_dir / "catalog.jsonl").open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    entry = json.loads(line)
                    self._entries[str(entry["source_id"])] = entry

    def _select_release(self) -> Path:
        root = self.vault / CATALOG_ROOT
        for name in ("current", "previous"):
            release = root / name
            if not all((release / item).is_file() for item in ("manifest.json", "facets.json", "catalog.jsonl")):
                continue
            try:
                manifest = json.loads((release / "manifest.json").read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if manifest.get("catalog_schema_version") in SUPPORTED_CATALOG_SCHEMAS:
                return release
        raise CatalogUnavailableError(f"no published KBase catalog under {root}")

    def entries(self) -> Iterable[dict[str, Any]]:
        return self._entries.values()

    def get(self, source_id: str) -> dict[str, Any] | None:
        return self._entries.get(str(source_id))

    def safe_path(self, relative_path: str) -> Path:
        clean = str(relative_path).replace("\\", "/").lstrip("/")
        path = (self.vault / clean).resolve()
        try:
            path.relative_to(self.vault)
        except ValueError as error:
            raise ValueError("path must stay inside KBase") from error
        return path

    def entry_for_path(self, relative_path: str) -> dict[str, Any] | None:
        normalized = str(relative_path).replace("\\", "/").lstrip("/").lower()
        for entry in self._entries.values():
            if normalized in {str(value).replace("\\", "/").lower() for value in entry.get("paths", {}).values()}:
                return entry
        return None

    def read_packet(self, entry: dict[str, Any]) -> dict[str, Any]:
        packet_path = entry.get("paths", {}).get("packet")
        if not packet_path:
            return {}
        path = self.safe_path(packet_path)
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def read_distilled_candidate(self, entry: dict[str, Any]) -> dict[str, Any]:
        candidate_path = entry.get("paths", {}).get("distilled_candidate")
        if not candidate_path:
            return {}
        path = self.safe_path(candidate_path)
        try:
            wrapper = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
            return {}
        if not isinstance(wrapper, dict):
            return {}
        gate = wrapper.get("quality_gate") if isinstance(wrapper.get("quality_gate"), dict) else {}
        candidate = wrapper.get("candidate") if isinstance(wrapper.get("candidate"), dict) else {}
        if gate.get("decision") != "accept" or gate.get("publication_eligible") is not True:
            return {}
        if str(candidate.get("source_id") or "") != str(entry.get("source_id") or ""):
            return {}
        return candidate
