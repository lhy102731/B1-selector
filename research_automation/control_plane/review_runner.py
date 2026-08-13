"""CR-010 F-04: reproducible independent reviewer runner.

Runs one independent reviewer (A or B) against a committed candidate and
emits:
- prompt file (binds the candidate commit/tree hash explicitly)
- raw output file (verbatim model response)
- calls manifest JSON (model/provider/invocation id/UTC timestamps/tokens/
  prompt_sha256/raw_output_sha256)

The prompt instructs the model to review ONLY committed evidence at the
given candidate; the runner never feeds uncommitted working-tree files.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _repo_head(repository_root: Path) -> tuple[str, str]:
    import subprocess

    def run(*args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(repository_root), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if result.returncode != 0:
            raise RuntimeError(f"git {args[0]} failed: {result.stderr[-300:]}")
        return result.stdout.strip()

    commit = run("rev-parse", "HEAD")
    tree = run("rev-parse", "HEAD^{tree}")
    return commit, tree


def _call_model(
    *,
    base_url: str,
    api_key: str,
    model: str,
    prompt: str,
    max_tokens: int = 8000,
    timeout_ms: int = 600_000,
) -> tuple[str, int, int]:
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    request = urllib.request.Request(
        base_url.rstrip("/") + "/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "content-type": "application/json",
            "authorization": "Bearer " + api_key,
        },
        method="POST",
    )
    with urllib.request.urlopen(
        request, timeout=timeout_ms / 1000.0
    ) as response:
        payload = json.loads(response.read().decode("utf-8"))
    text = payload["choices"][0]["message"]["content"]
    usage = payload.get("usage") or {}
    input_tokens = int(usage.get("prompt_tokens", 0))
    output_tokens = int(usage.get("completion_tokens", 0))
    return text, input_tokens, output_tokens


def _provider_spec(provider: str) -> tuple[str, str, str]:
    """Return (base_url, api_key_env, default_model) for a provider name."""
    import re

    env = {}
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if env_path.exists():
        content = env_path.read_text(encoding="utf-8")
        for match in re.finditer(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$", content, re.M):
            env[match.group(1)] = match.group(2).strip()
    specs = {
        "deepseek": (
            "https://api.deepseek.com",
            "AG2_DEEPSEEK2_API_KEY",
            "deepseek-v4-flash",
        ),
        "glm": (
            "https://ark.cn-beijing.volces.com/api/coding/v3",
            "AG2_ZHIPU_API_KEY",
            "glm-5.2",
        ),
        "doubao": (
            "https://ark.cn-beijing.volces.com/api/coding/v3",
            "AG2_DouBao_API_KEY",
            "doubao-seed-2.0-pro",
        ),
        "grok": (
            "https://token173.net/v1",
            "AG2_Grok_API_KEY",
            "grok-4.5",
        ),
    }
    if provider not in specs:
        raise ValueError(f"unknown provider: {provider}")
    base_url, key_env, default_model = specs[provider]
    key = os.environ.get(key_env) or env.get(key_env, "")
    if not key:
        raise RuntimeError(f"provider {provider} has no API key ({key_env})")
    return base_url, key, default_model


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", required=True,
                        choices=["deepseek", "glm", "doubao", "grok"])
    parser.add_argument("--reviewer", required=True, choices=["A", "B"])
    parser.add_argument("--phase", required=True,
                        choices=["P0", "P6", "P7", "P8", "C0"])
    parser.add_argument("--attempt", required=True)
    parser.add_argument("--out-dir", required=True,
                        help="evidence dir for prompt/raw/calls outputs")
    parser.add_argument("--model", default=None)
    parser.add_argument("--prompt-file", default=None,
                        help="read prompt from this file instead of building")
    parser.add_argument("--max-tokens", type=int, default=8000)
    args = parser.parse_args(argv)

    repository_root = Path(__file__).resolve().parents[2]
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    commit, tree = _repo_head(repository_root)
    base_url, api_key, default_model = _provider_spec(args.provider)
    model = args.model or default_model

    if args.prompt_file:
        prompt = Path(args.prompt_file).read_text(encoding="utf-8")
    else:
        prompt = (
            "You are an independent reviewer (Reviewer " + args.reviewer
            + ") for the " + args.phase + " gate of the V3.4.2 control plane "
            "(CR-010 corrective recovery).\n"
            "CANDIDATE (commit/tree): " + commit + " / " + tree + "\n"
            "ATTEMPT: " + args.attempt + "\n\n"
            "Review ONLY committed evidence at the candidate above. Never "
            "reference files or code branches that do not exist in the "
            "committed tree. Verify: (1) gate/closure time causality "
            "(closure closed_at UTC > gate created_at); (2) evidence blob "
            "binding (sha256 + ticket/evidence semantics); (3) test receipt "
            "full contract; (4) any hard-crash/reconciler claims match real "
            "code; (5) the C0 official 24-cycle evidence chain. Reply with "
            "findings (severity, id, summary, code refs) and a final verdict "
            "(APPROVE / HOLD / REJECT)."
        )

    prompt_sha256 = _sha256_bytes(prompt.encode("utf-8"))
    invocation_id = f"cr010-review-{args.phase.lower()}-{args.reviewer.lower()}-{int(time.time())}"
    started = _utc_now()
    text, input_tokens, output_tokens = _call_model(
        base_url=base_url,
        api_key=api_key,
        model=model,
        prompt=prompt,
        max_tokens=args.max_tokens,
    )
    completed = _utc_now()

    raw_ref = (
        f"cr010_reviewer_{args.reviewer.lower()}_{args.phase.lower()}_raw.txt"
    )
    prompt_ref = (
        f"cr010_reviewer_{args.reviewer.lower()}_{args.phase.lower()}_prompt.txt"
    )
    calls_ref = f"cr010_review_{args.phase.lower()}_calls.json"
    (out_dir / prompt_ref).write_text(prompt, encoding="utf-8", newline="\n")
    (out_dir / raw_ref).write_text(text, encoding="utf-8", newline="\n")
    calls = {
        args.reviewer.lower(): {
            "model": model,
            "provider": args.provider,
            "invocation_id": invocation_id,
            "started_at_utc": started,
            "completed_at_utc": completed,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "candidate_commit": commit,
            "candidate_tree": tree,
            "prompt_ref": prompt_ref,
            "prompt_sha256": prompt_sha256,
            "raw_output_ref": raw_ref,
            "raw_output_sha256": _sha256_bytes(
                text.encode("utf-8")
            ),
        }
    }
    calls_path = out_dir / calls_ref
    if calls_path.exists():
        existing = json.loads(calls_path.read_text(encoding="utf-8"))
        existing.update(calls)
        calls = existing
    calls_path.write_text(
        json.dumps(calls, ensure_ascii=False, sort_keys=True,
                   separators=(",", ":")) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(
        json.dumps(
            {
                "reviewer": args.reviewer,
                "provider": args.provider,
                "model": model,
                "candidate_commit": commit,
                "candidate_tree": tree,
                "invocation_id": invocation_id,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "raw_output_sha256": calls[args.reviewer.lower()][
                    "raw_output_sha256"
                ],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
