"""Loader for the strategy knowledge bases under ag2_research/knowledge_base/.

Each subject (e.g. "b1_v3") lives in its own subdirectory containing a
manifest.yaml that describes everything. The loader is intentionally
deterministic and side-effect free: it reads files, validates schema
fields, and returns a KnowledgeBase dataclass.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "pyyaml is required for ag2_research.knowledge_base. "
        "Install with: pip install pyyaml"
    ) from exc

HERE = Path(__file__).resolve().parent


@dataclass
class KnowledgeBase:
    """In-memory representation of a strategy knowledge base."""

    subject: str
    kb_version: str
    as_of_phase: str
    root: Path
    manifest: dict[str, Any]
    artifacts: dict[str, Path]
    headline: dict[str, Any]
    frozen_baseline: dict[str, dict[str, float]]
    acceptance_bar: dict[str, Any]
    hard_constraints: dict[str, Any] = field(default_factory=dict)

    # -- artifact accessors -------------------------------------------------

    def read_text(self, key: str) -> str:
        """Return text contents of the named artifact (e.g. 'brief')."""
        p = self.artifacts[key]
        return p.read_text(encoding="utf-8")

    def read_json(self, key: str) -> Any:
        """Return parsed JSON for the named artifact."""
        p = self.artifacts[key]
        return json.loads(p.read_text(encoding="utf-8"))

    def list_artifacts(self) -> list[str]:
        return sorted(self.artifacts.keys())

    # -- structured queries -------------------------------------------------

    def alpha_generators(self) -> list[dict]:
        return self.read_json("alpha_generators")["entry_generators"]

    def exit_alphas(self) -> list[dict]:
        return self.read_json("exit_generators")["exit_generators"]

    def concentrators(self) -> list[dict]:
        return self.read_json("concentrators")["concentrators"]

    def dead_components(self) -> list[dict]:
        return self.read_json("dead_components")["components"]

    def interaction_summary(self) -> dict:
        return self.read_json("interactions")

    def lessons(self) -> list[dict]:
        return self.read_json("lessons")["lessons"]

    # -- convenience --------------------------------------------------------

    def fingerprint(self) -> str:
        return f"{self.subject}@{self.kb_version} ({self.as_of_phase})"


# ---------------------------------------------------------------- public API


@lru_cache(maxsize=None)
def load(subject: str) -> KnowledgeBase:
    """Load a strategy knowledge base by subject name.

    Cached: each subject is loaded once per process. Mutating the underlying
    files at runtime is not supported.
    """
    root = HERE / subject
    if not root.is_dir():
        avail = list_subjects()
        raise FileNotFoundError(
            f"Knowledge base for '{subject}' not found under {HERE}. "
            f"Available subjects: {avail}"
        )

    manifest_path = root / "manifest.yaml"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing manifest.yaml in {root}")
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))

    # Validate required fields
    required = ("kb_version", "subject", "as_of_phase", "artifacts",
                "headline", "frozen_baseline", "acceptance_bar")
    for key in required:
        if key not in manifest:
            raise ValueError(f"manifest.yaml missing required key '{key}' for {subject}")
    if manifest["subject"] != subject:
        raise ValueError(
            f"manifest subject mismatch: directory '{subject}' but manifest says "
            f"'{manifest['subject']}'"
        )

    # Resolve artifact paths
    artifacts: dict[str, Path] = {}
    for key, rel in manifest["artifacts"].items():
        ap = root / rel
        if not ap.exists():
            raise FileNotFoundError(
                f"Knowledge base '{subject}' references missing artifact "
                f"'{key}' -> {ap}"
            )
        artifacts[key] = ap

    # Hard constraints are optional (a strategy may have none yet)
    hc_path = artifacts.get("hard_constraints")
    hc = {}
    if hc_path is not None:
        hc = yaml.safe_load(hc_path.read_text(encoding="utf-8")) or {}

    return KnowledgeBase(
        subject=subject,
        kb_version=manifest["kb_version"],
        as_of_phase=manifest["as_of_phase"],
        root=root,
        manifest=manifest,
        artifacts=artifacts,
        headline=manifest["headline"],
        frozen_baseline=manifest["frozen_baseline"],
        acceptance_bar=manifest["acceptance_bar"],
        hard_constraints=hc,
    )


def list_subjects() -> list[str]:
    """Return all subject names with a valid manifest.yaml in the KB folder."""
    out = []
    for p in sorted(HERE.iterdir()):
        if p.is_dir() and (p / "manifest.yaml").exists():
            out.append(p.name)
    return out
