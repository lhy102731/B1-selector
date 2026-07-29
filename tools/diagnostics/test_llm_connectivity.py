"""Explicit CLI connectivity check for configured LLM profiles."""
from __future__ import annotations

import sys
import time
from pathlib import Path


def _find_project_root() -> Path:
    root = Path(__file__).resolve()
    while not (root / "AGENTS.md").exists() and root != root.parent:
        root = root.parent
    return root


def main() -> int:
    """Run one short live request per profile only when called explicitly."""
    project_root = _find_project_root()
    project_text = str(project_root)
    if project_text not in sys.path:
        sys.path.insert(0, project_text)

    import autogen
    from ag2_research import ResearchConfig

    config = ResearchConfig()
    results: list[tuple[str, ...]] = []

    for profile_name in config.list_profiles():
        label = profile_name
        try:
            llm_config = config.get_llm_config(profile_name)
        except KeyError:
            results.append((label, "SKIP", "profile not found"))
            continue

        profile_config = llm_config["config_list"][0]
        model = profile_config.get("model", "?")
        extra_keys = [
            key
            for key in profile_config
            if key not in ("model", "api_key", "base_url")
        ]
        thinking_info = (
            ", ".join(f"{key}={profile_config[key]}" for key in extra_keys)
            if extra_keys
            else "none"
        )

        print(
            f"[{label}] testing... model={model}, "
            f"thinking=({thinking_info})"
        )

        try:
            agent = autogen.AssistantAgent(
                name=f"test_{label}",
                system_message="Reply with exactly one word: OK",
                llm_config=llm_config,
            )
            user = autogen.UserProxyAgent(
                name="tester",
                human_input_mode="NEVER",
                max_consecutive_auto_reply=0,
                code_execution_config=False,
            )
            started = time.time()
            result = user.initiate_chat(agent, message="Say OK", max_turns=1)
            elapsed = time.time() - started

            chat_history = getattr(result, "chat_history", [])
            response_text = ""
            for message in chat_history:
                if message.get("name") == f"test_{label}":
                    response_text = message.get("content", "")[:100]
                    break

            status = "OK" if response_text else "NO_RESPONSE"
            print(f"  -> {status} ({elapsed:.1f}s) {response_text[:80]}")
            results.append(
                (label, status, str(model), thinking_info, f"{elapsed:.1f}s")
            )
        except Exception as error:
            error_message = str(error)[:120]
            print(f"  -> FAIL: {error_message}")
            results.append(
                (label, "FAIL", str(model), thinking_info, error_message)
            )

    print(f"\n{'=' * 70}")
    print(
        f"{'Profile':<18} {'Status':<12} {'Model':<30} "
        f"{'Thinking':<25} {'Time/Error'}"
    )
    print(f"{'-' * 70}")
    for result_row in results:
        if len(result_row) == 5:
            print(
                f"{result_row[0]:<18} {result_row[1]:<12} "
                f"{result_row[2]:<30} {result_row[3]:<25} {result_row[4]}"
            )
        else:
            print(f"{result_row[0]:<18} {result_row[1]:<12}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
