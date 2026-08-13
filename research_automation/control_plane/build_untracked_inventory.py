"""CR-010 F-08: build the untracked quarantine inventory (add-only)."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

entries: list[dict[str, object]] = []
for line in open("untracked_cp_list.txt", encoding="utf-8"):
    rel = line.strip()
    if not rel:
        continue
    p = Path(rel)
    if p.is_dir():
        for child in sorted(p.rglob("*")):
            if child.is_file():
                rel_child = str(child).replace("\\", "/")
                entries.append(
                    {
                        "path": rel_child,
                        "kind": "file",
                        "sha256": hashlib.sha256(
                            child.read_bytes()
                        ).hexdigest(),
                        "bytes": child.stat().st_size,
                    }
                )
    elif p.is_file():
        entries.append(
            {
                "path": rel,
                "kind": "file",
                "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
                "bytes": p.stat().st_size,
            }
        )
    else:
        entries.append({"path": rel, "kind": "missing", "sha256": None, "bytes": 0})

entries.sort(key=lambda e: str(e["path"]))
doc = {
    "schema_version": "control_plane.untracked_quarantine_inventory.v1",
    "created_at": "2026-08-14T00:00:00Z",
    "candidate_ref": "v342-cr010-candidate-20260814",
    "purpose": (
        "CR-010 F-08: inventory of all untracked control-plane working-tree "
        "files. None are deleted or committed here; the P0 default-named FAIL "
        "gate conflict is documented. Gate verifiers MUST read only the "
        "tracked committed gate files."
    ),
    "p0_gate_conflict": {
        "tracked": (
            "research_state/control_plane/p0/attempts/p0-attempt-012/gates/"
            "official_p0_gate_v342_cr009_final.json"
        ),
        "tracked_verdict": "PASS",
        "untracked": (
            "research_state/control_plane/p0/attempts/p0-attempt-012/gates/"
            "official_p0_gate_v342_cr009.json"
        ),
        "untracked_verdict": "FAIL",
        "untracked_reason_codes": ["ACTIVE_GRANT_COUNT:2"],
        "resolution": (
            "untracked file kept as a historical artifact; the closure "
            "receipt binds the tracked _final.json; no new gate/closure may "
            "reference the untracked file"
        ),
    },
    "runtime_databases": [
        "research_state/control_plane/authority.db",
        "research_state/control_plane/authority.sqlite3",
    ],
    "entries_count": len(entries),
    "entries": entries,
}
out = json.dumps(doc, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
path = Path(
    "research_state/control_plane/rollout/lineage_audits/"
    "untracked_quarantine_inventory_cr010.json"
)
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(out, encoding="utf-8", newline="\n")
print("written:", path, "entries:", len(entries))
