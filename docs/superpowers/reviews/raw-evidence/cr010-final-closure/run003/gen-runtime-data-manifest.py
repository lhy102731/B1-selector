"""Generate run003/runtime-data-manifest.txt from the MAIN working tree.

Runtime data required by the full suite is NOT part of the candidate diff;
this manifest records the exact bytes (path / file count / SHA-256) of the
data the full suite depends on so the validation environment is
reproducible.  Source of truth: the main working tree; the run003 worktree
and the step-3 integration worktree copies must hash identically.
"""

import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

OUT = Path(sys.argv[1])
MAIN = Path("D:/workspace/a-share-quant-selector-main")
ROOTS = [
    (MAIN / "ag2_research" / "kbase", "ag2_research/kbase"),
    (MAIN / "research_state" / "control_plane", "research_state/control_plane"),
]
EXTRA = MAIN / "tools" / "ths_yuanhang_bridge" / "YuanhangBridge.dll"

lines = [
    "# CR-010 runtime-data manifest (run005)",
    "# runtime data required by the full suite; NOT part of the candidate diff",
    "# generated: " + datetime.now(timezone.utc).isoformat(),
    "# source: " + str(MAIN) + " (main working tree)",
    "# format: <relative path><TAB><sha256>",
]
total = 0
for root, label in ROOTS:
    if not root.is_dir():
        lines.append(f"# {label}: DIRECTORY MISSING")
        continue
    paths = sorted(p for p in root.rglob("*") if p.is_file())
    lines.append(f"# {label}: {len(paths)} files")
    total += len(paths)
    for p in paths:
        rel = p.relative_to(MAIN).as_posix()
        h = hashlib.sha256(p.read_bytes()).hexdigest()
        lines.append(f"{rel}\t{h}")
if EXTRA.exists():
    h = hashlib.sha256(EXTRA.read_bytes()).hexdigest()
    rel = EXTRA.relative_to(MAIN).as_posix()
    lines.append(f"# {rel}: 1 file")
    lines.append(f"{rel}\t{h}")
    total += 1
else:
    lines.append(f"# {rel}: MISSING")
lines.append(f"# total: {total} files")
OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("written", OUT)
print("\n".join(lines[:6]))
print("\n".join(lines[-4:]))