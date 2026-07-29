"""Generate a metadata-only maintenance report for the KBase discovery layer."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from .repository import KBaseRepository
from .telemetry import aggregate_usage
from .coverage import build_navigation_coverage


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def build_maintenance_report(*, vault_path: str | Path | None = None) -> dict[str, Any]:
    repo = KBaseRepository(vault_path)
    build_report_path = repo.release_dir / "build-report.json"
    build_report = json.loads(build_report_path.read_text(encoding="utf-8"))
    usage = aggregate_usage()
    intake_root = repo.vault / "wiki" / "outputs" / "manifests" / "ag2-kbase" / "intake"
    intake_states = []
    if intake_root.is_dir():
        for path in sorted(intake_root.glob("*.json")):
            try:
                intake_states.append(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError):
                continue
    pending = Counter(item for state in intake_states for item in state.get("pending", []))
    regression_paths = {
        "legacy": PROJECT_ROOT / "data" / "ag2_kbase" / "query-regression-results.json",
        "catalog": PROJECT_ROOT / "data" / "ag2_kbase" / "query-regression-catalog.json",
    }
    regression = {}
    for name, path in regression_paths.items():
        if path.is_file():
            try:
                regression[name] = json.loads(path.read_text(encoding="utf-8")).get("metrics", {})
            except (OSError, json.JSONDecodeError):
                pass
    return {
        "catalog_version": repo.manifest.get("catalog_version"),
        "catalog_generated_at": repo.manifest.get("generated_at"),
        "catalog_counts": repo.manifest.get("counts", {}),
        "build_health": {
            "errors": len(build_report.get("errors", [])),
            "blocked_packets": build_report.get("blocked_packet_entries", 0),
            "reused_entries": build_report.get("reused_entries", 0),
            "entries_with_warnings": build_report.get("warnings", {}).get("entries_with_warnings", 0),
        },
        "query_regression": regression,
        "usage": usage,
        "intake": {
            "resources": len(intake_states),
            "discoverable": sum(bool(state.get("ag2_discoverable")) for state in intake_states),
            "pending_counts": dict(pending),
        },
        "progressive_coverage": build_navigation_coverage(vault_path=repo.vault),
        "maintenance_priority": [
            "high-frequency no-result query hashes",
            "frequently opened sources without evidence layer",
            "frequently used dated families",
            "long compilations that need chapter navigation",
            "low-frequency visual gaps",
        ],
        "warning": "Usage frequency is a maintenance signal, not evidence that a source is correct.",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vault", default=r"D:\KBase")
    parser.add_argument("--output")
    args = parser.parse_args()
    rendered = json.dumps(build_maintenance_report(vault_path=args.vault), ensure_ascii=False, indent=2) + "\n"
    if args.output:
        target = Path(args.output)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
