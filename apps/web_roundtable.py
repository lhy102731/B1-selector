"""Web UI for multi-LLM roundtable — Flask + SSE real-time streaming."""
from __future__ import annotations

import json
import queue
import sys
import threading
import time
from pathlib import Path

import autogen
from flask import Flask, Response, render_template, request, jsonify

# Ensure project root is on path
_PROJECT_ROOT = Path(__file__).resolve()
while not (_PROJECT_ROOT / 'AGENTS.md').exists() and _PROJECT_ROOT != _PROJECT_ROOT.parent:
    _PROJECT_ROOT = _PROJECT_ROOT.parent
sys.path.insert(0, str(_PROJECT_ROOT))

_CONCLUSION_DIR = _PROJECT_ROOT / "artifacts" / "research" / "roundtable" / "conclusions"
_DISCUSSION_DIR = _PROJECT_ROOT / "artifacts" / "research" / "roundtable" / "discussions"
_LEGACY_DISCUSSION_DIR = _PROJECT_ROOT / "discussions"

from ag2_research.config import ResearchConfig
from ag2_research.tools import get_tools_for_agent
from research_automation.control_plane.web_guard import (
    WebAuthorizationError,
    authorize_mutation,
    authorize_thread,
)

app = Flask(__name__, template_folder=str(_PROJECT_ROOT / "ag2_research" / "templates"))
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.jinja_env.auto_reload = True
app.jinja_env.cache = {}
# DENIED_WEB by default.  Only a trusted local control-plane adapter may set
# this to a WebAuthorizationContext; HTTP clients cannot manufacture one.
app.config["CONTROL_PLANE_WEB_AUTH"] = None
cfg = ResearchConfig()

# Active discussions: {discussion_id: {"thread": Thread, "queue": Queue, "messages": []}}
DISCUSSIONS: dict[str, dict] = {}
_disc_counter = 0


def _web_auth():
    return app.config.get("CONTROL_PLANE_WEB_AUTH")


def _safe_discussion_path(directory: Path, filename: str) -> Path:
    """Resolve one roundtable Markdown filename without permitting traversal."""
    if not isinstance(filename, str) or not filename.strip():
        raise ValueError("discussion filename is required")
    filename = filename.strip()
    name = Path(filename).name
    if (
        name != filename
        or not name.startswith("roundtable_")
        or Path(name).suffix.lower() != ".md"
    ):
        raise ValueError("invalid discussion filename")
    base = directory.resolve()
    candidate = (base / name).resolve()
    if candidate.parent != base:
        raise ValueError("discussion path escapes the allowed directory")
    return candidate


def _existing_discussion_path(filename: str) -> Path | None:
    """Prefer a new artifact, then fall back to the read-only legacy store."""
    for directory in (_DISCUSSION_DIR, _LEGACY_DISCUSSION_DIR):
        candidate = _safe_discussion_path(directory, filename)
        if candidate.is_file():
            return candidate
    return None


def _stream_roundtable(topic: str, participants: list[dict], msg_queue: queue.Queue, research_context: str = "", save_filename: str = None):
    """Run roundtable in background thread, pushing messages to queue."""
    try:
        authorize_thread(_web_auth())
    except WebAuthorizationError as error:
        msg_queue.put({"type": "error", "content": str(error)})
        msg_queue.put({"type": "done"})
        return
    rt = cfg._raw.get("roundtable", {})
    max_rounds = rt.get("max_rounds", 999)
    system_template = rt.get("system_prompt", "你是 {label}，正在参加一个量化策略研究圆桌讨论。")

    agents: dict[str, autogen.AssistantAgent] = {}
    votes: dict[str, str] = {}
    vote_reasons: list[str] = []
    vote_lock = threading.Lock()
    concluded = [False]
    total_agents = len(participants)

    # Define vote factory ONCE outside the loop (avoid closure-over-loop bugs)
    def _make_vote(caller_name):
        def _vote(vote: str, reason: str = "") -> str:
            with vote_lock:
                # Don't overwrite existing votes from same caller
                if caller_name in votes:
                    return f"你已经投过票了 ({votes[caller_name]})。当前: {sum(1 for v in votes.values() if v == 'yes')}同意 / {sum(1 for v in votes.values() if v == 'no')}反对"
                votes[caller_name] = vote.lower()
                if reason:
                    vote_reasons.append(f"{caller_name}({vote}): {reason}")
                yes = sum(1 for v in votes.values() if v == "yes")
                no = sum(1 for v in votes.values() if v == "no")
                msg_queue.put({"type": "vote_update", "name": caller_name, "vote": vote, "reason": reason, "yes": yes, "no": no, "total": total_agents})
                if yes == total_agents:
                    ct = f"全票通过: {yes}同意 / 0反对\n" + "\n".join(vote_reasons)
                    concluded[0] = True
                    cd = _CONCLUSION_DIR
                    ts = time.strftime("%Y%m%d_%H%M%S")
                    cf2 = cd / f"conclusion_{ts}.md"
                    try:
                        authorize_mutation(
                            _web_auth(),
                            operation="WEB_WRITE",
                            callable_name="_stream_roundtable",
                            paths=(cd, cf2),
                        )
                    except WebAuthorizationError as error:
                        msg_queue.put({"type": "error", "content": str(error)})
                        return ""
                    cd.mkdir(parents=True, exist_ok=True)
                    cf2.write_text(f"# 讨论结论\n\n**日期:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n**参与模型:** {', '.join(p2['label'] for p2 in participants)}\n\n---\n\n## 投票结果\n\n{ct}\n\n", encoding="utf-8")
                    msg_queue.put({"type": "concluded", "content": ct, "conclusion_file": str(cf2)})
                    return f"全票通过 ({yes}/{total_agents})，讨论结束。"
                return f"已投票: {vote}。当前: {yes}同意 / {no}反对 / {total_agents}人（需全票同意，每人只能投一次）"
        return _vote

    for p in participants:
        label = p["label"]
        is_gemini = "gemini" in p["profile"].lower()
        llm_cfg = cfg.get_llm_config(p["profile"], model=p.get("model"))
        system_msg = system_template.format(label=label)
        if research_context:
            system_msg += f"\n\n研究背景：\n{research_context}"

        agent = autogen.AssistantAgent(
            name=label, system_message=system_msg.strip(),
            llm_config=llm_cfg, code_execution_config=False,
        )
        if is_gemini:
            # Text-based tool calling for Gemini (native function calling breaks with thought_signature)
            tool_list = get_tools_for_agent(["list_code", "read_code", "list_research_docs", "read_research_doc", "get_strategy_config", "list_available_data"])
            tool_map = {tf.__name__: tf for tf in tool_list}
            tool_help = []
            for tf in tool_list:
                import inspect
                sig = inspect.signature(tf)
                params = ', '.join(f'{k}={v.annotation.__name__ if hasattr(v.annotation,"__name__") else str(v.annotation)}' for k, v in sig.parameters.items())
                tool_help.append(f"| {tf.__name__}({params}) | {tf.__doc__[:150] if tf.__doc__ else ''} |")
            extra = "\n## 可用工具\n\n格式：`##TOOL:工具名##\n##ARGS:JSON参数##`\n\n| 工具 | 说明 |\n|------|------|\n" + "\n".join(tool_help)
            extra += "\n\n调用示例：\n##TOOL:list_code##\n##ARGS:{\"pattern\": \"*.md\"}##"
            extra += "\n\n`##TOOL:vote_to_conclude##` | 投票：vote='yes'或'no', reason=理由"
            system_msg += extra
        else:
            for tool_func in get_tools_for_agent(["list_code", "read_code", "list_research_docs", "read_research_doc", "get_strategy_config", "list_available_data"]):
                agent.register_for_llm()(tool_func)
                agent.register_for_execution()(tool_func)

            vf = _make_vote(label)
            agent.register_for_llm(name="vote_to_conclude", description="投票表决是否结束讨论。每人只能投一次。vote='yes'同意结束或'no'继续。reason简述理由。")(vf)
            agent.register_for_execution(name="vote_to_conclude")(vf)

        agents[label] = agent

    coordinator_profile = rt.get("coordinator", participants[0]["profile"])
    manager_llm = cfg.get_llm_config(coordinator_profile)

    groupchat = autogen.GroupChat(
        agents=list(agents.values()), messages=[],
        max_round=max_rounds, speaker_selection_method="round_robin",
        allow_repeat_speaker=True,
    )
    manager = autogen.GroupChatManager(groupchat=groupchat, llm_config=manager_llm)

    # Build initial message
    participant_list = "\n".join(f"  - {p['label']}" for p in participants)
    initial_message = f"""研究课题：{topic}

参与模型：
{participant_list}

讨论形式：
- 前几轮：每位参与者分析课题、查阅代码文档、自由辩论。
- 当讨论充分、结论已收敛时，可调用 vote_to_conclude 发起投票。
- 投票规则：每人只能投一次。yes=同意结束，no=继续。必须全票 yes 才结束。
- 投完票后闭嘴等待，除非你对其他人投票后提出的新观点有实质异议，否则不要重复发言或重复投票。

背景资料：
{research_context[:200] + '...(已截断)' if len(research_context) > 200 else (research_context or '无')}

{list(agents.values())[0].name}，你先开始。"""

    # Context optimization: if context is a file path, tell agents to read it on demand
    # If it's short text, include inline. If it's long discussion content, truncate.
    if research_context:
        if (research_context.startswith("discussions/") or
                research_context.startswith("artifacts/research/roundtable/conclusions/")):
            # File path — agents can read on demand via read_code tool
            for agent in agents.values():
                current = agent.system_message
                agent.update_system_message(current + f"\n\n你可以使用 read_code 工具读取历史讨论文件: {research_context}")
        elif len(research_context) > 1500:
            summary = research_context[:1500] + "\n...(已截断，可用 read_code 读取完整内容)"
            for agent in agents.values():
                current = agent.system_message
                agent.update_system_message(current + f"\n\n历史讨论摘要：\n{summary}")
        elif len(research_context) > 0:
            for agent in agents.values():
                current = agent.system_message
                agent.update_system_message(current + f"\n\n历史讨论：\n{research_context}")

    # Profile lookup for detecting Gemini messages
    agent_profile_map = {p["label"]: p["profile"] for p in participants}

    # Tool map for text-based tool execution
    tool_list = get_tools_for_agent(["list_code", "read_code", "list_research_docs", "read_research_doc", "get_strategy_config", "list_available_data"])
    tool_map = {tf.__name__: tf for tf in tool_list}

    def _handle_text_tools(content: str, speaker: str) -> tuple[str, bool]:
        """Parse ##TOOL:name## and ##ARGS:{...}## from content. Execute and return (result_text, was_vote)."""
        import re as _re, json as _json
        pattern = r'##TOOL:(\w+)##\s*\n?\s*##ARGS:(\{.*?\})##'
        matches = list(_re.finditer(pattern, content, _re.DOTALL))
        if not matches:
            return "", False
        results = []
        was_vote = False
        for m in matches:
            tname = m.group(1)
            try:
                args = _json.loads(m.group(2))
            except Exception:
                args = {}
            if tname == "vote_to_conclude":
                was_vote = True
                vote = args.get("vote", "yes")
                reason = args.get("reason", "")
                with vote_lock:
                    votes[speaker] = vote.lower()
                    if reason:
                        vote_reasons.append(f"{speaker}({vote}): {reason}")
                    yes = sum(1 for v in votes.values() if v == "yes")
                    no = sum(1 for v in votes.values() if v == "no")
                    msg_queue.put({"type": "vote_update", "name": speaker, "vote": vote, "reason": reason, "yes": yes, "no": no, "total": total_agents})
                    if yes == total_agents:
                        ct = f"全票通过: {yes}同意 / 0反对\n" + "\n".join(vote_reasons)
                        concluded[0] = True
                        concl_dir = _CONCLUSION_DIR
                        ts = time.strftime("%Y%m%d_%H%M%S")
                        cf2 = concl_dir / f"conclusion_{ts}.md"
                        try:
                            authorize_mutation(
                                _web_auth(),
                                operation="WEB_WRITE",
                                callable_name="_stream_roundtable",
                                paths=(concl_dir, cf2),
                            )
                        except WebAuthorizationError as error:
                            msg_queue.put({"type": "error", "content": str(error)})
                            return ""
                        concl_dir.mkdir(parents=True, exist_ok=True)
                        with open(cf2, "w", encoding="utf-8") as cfh:
                            cfh.write(f"# 讨论结论\n\n**日期:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                            cfh.write(f"**参与模型:** {', '.join(p2['label'] for p2 in participants)}\n\n---\n\n## 投票结果\n\n{ct}\n\n")
                        msg_queue.put({"type": "concluded", "content": ct, "conclusion_file": str(cf2)})
                        results.append(f"投票: {vote} — 全票通过，讨论结束。")
                    else:
                        results.append(f"投票: {vote} — 当前 {yes}同意/{no}反对/{total_agents}人（需全票同意）")
            elif tname in tool_map:
                try:
                    result = tool_map[tname](**args)
                    results.append(f"[{tname}] 结果:\n{str(result)[:3000]}")
                except Exception as e:
                    results.append(f"[{tname}] 错误: {e}")
        return "\n\n".join(results), was_vote

    # Polling thread — sends new messages to queue
    def _poll_messages():
        seen = 0
        while not concluded[0]:
            msgs = groupchat.messages
            # Fix empty user messages (Kimi API rejects them)
            for m in msgs:
                if m.get("role") == "user" and not (m.get("content") or "").strip():
                    m["content"] = " "
            while seen < len(msgs):
                msg = msgs[seen]
                name = msg.get("name", "")
                content = msg.get("content", "") or ""
                if name and name != "chat_manager" and content and content.strip():
                    # Handle text-based tool calls from Gemini
                    if "##TOOL:" in content:
                        result, was_vote = _handle_text_tools(content, name)
                        if result:
                            groupchat.messages.append({"role": "user", "name": "_tools", "content": "[工具结果] " + result})
                    msg_queue.put({
                        "type": "message",
                        "name": name,
                        "content": content,
                        "timestamp": time.strftime("%H:%M:%S"),
                    })
                seen += 1
            time.sleep(0.3)

    poll_thread = threading.Thread(target=_poll_messages, daemon=True)
    poll_thread.start()

    # Auto-save thread — saves progress every 60s so crashes don't lose everything
    log_dir = _DISCUSSION_DIR
    if save_filename:
        save_path = _safe_discussion_path(log_dir, save_filename)
        # Preserve old content when continuing
        old_content = ""
        source_path = _existing_discussion_path(save_filename)
        if source_path is not None:
            old_content = source_path.read_text(encoding="utf-8") + "\n\n---\n## 继续讨论 — " + time.strftime('%Y-%m-%d %H:%M:%S') + "\n\n"
    else:
        ts = time.strftime("%Y%m%d_%H%M%S")
        save_path = log_dir / f"roundtable_{ts}.md"
        old_content = ""

    try:
        authorize_mutation(
            _web_auth(),
            operation="WEB_WRITE",
            callable_name="_stream_roundtable",
            paths=(log_dir, save_path),
        )
    except WebAuthorizationError as error:
        msg_queue.put({"type": "error", "content": str(error)})
        msg_queue.put({"type": "done"})
        return
    log_dir.mkdir(parents=True, exist_ok=True)

    def _auto_save():
        last_count = 0
        while not concluded[0]:
            time.sleep(60)
            msgs = list(groupchat.messages)
            if len(msgs) <= last_count:
                continue
            last_count = len(msgs)
            try:
                content = old_content
                for msg in msgs:
                    name = msg.get("name", "")
                    text = (msg.get("content", "") or "").strip()
                    if name and text:
                        content += f"### {name}\n\n{text}\n\n"
                authorize_mutation(
                    _web_auth(),
                    operation="WEB_WRITE",
                    callable_name="_stream_roundtable",
                    paths=(log_dir, save_path),
                )
                save_path.write_text(content, encoding="utf-8")
            except Exception:
                pass

    auto_save_thread = threading.Thread(target=_auto_save, daemon=True)
    auto_save_thread.start()

    try:
        first_agent = list(agents.values())[0]
        first_agent.initiate_chat(manager, message=initial_message)
    except Exception as e:
        msg_queue.put({"type": "error", "content": str(e)})
    finally:
        msg_queue.put({"type": "done"})

        # Final save
        content = old_content if old_content else f"# Roundtable Discussion\n\n**Topic:** {topic}\n\n**Date:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n---\n\n"
        for msg in groupchat.messages:
            name = msg.get("name", "")
            text = (msg.get("content", "") or "").strip()
            if name and text:
                content += f"### {name}\n\n{text}\n\n"
        try:
            authorize_mutation(
                _web_auth(),
                operation="WEB_WRITE",
                callable_name="_stream_roundtable",
                paths=(log_dir, save_path),
            )
            save_path.write_text(content, encoding="utf-8")
        except WebAuthorizationError as error:
            msg_queue.put({"type": "error", "content": str(error)})
            return
        msg_queue.put({"type": "saved", "path": str(save_path), "filename": save_path.name})


@app.route("/")
def index():
    template_path = _PROJECT_ROOT / "ag2_research" / "templates" / "roundtable.html"
    return Response(template_path.read_text(encoding="utf-8"), mimetype="text/html")


@app.route("/api/discussions", methods=["GET"])
def api_discussions():
    """List saved discussion files."""
    candidates: dict[str, Path] = {}
    for log_dir in (_LEGACY_DISCUSSION_DIR, _DISCUSSION_DIR):
        if not log_dir.exists():
            continue
        for path in log_dir.glob("roundtable_*.md"):
            current = candidates.get(path.name)
            if current is None or path.stat().st_mtime > current.stat().st_mtime:
                candidates[path.name] = path
    if not candidates:
        return jsonify([])
    files = sorted(
        candidates.values(), key=lambda path: path.stat().st_mtime, reverse=True
    )
    result = []
    for f in files[:50]:  # latest 50
        stat = f.stat()
        # Extract topic from first line
        topic = ""
        try:
            with open(f, encoding="utf-8") as fh:
                for line in fh:
                    if line.startswith("**Topic:**"):
                        topic = line.replace("**Topic:**", "").strip()
                        break
        except Exception:
            pass
        result.append({
            "filename": f.name,
            "topic": topic or f.name,
            "date": time.strftime("%Y-%m-%d %H:%M", time.localtime(stat.st_mtime)),
            "size_kb": round(stat.st_size / 1024, 1),
        })
    return jsonify(result)


@app.route("/api/discussions/<filename>", methods=["GET"])
def api_load_discussion(filename):
    """Load a saved discussion file."""
    try:
        filepath = _existing_discussion_path(filename)
    except ValueError:
        return jsonify({"error": "invalid filename"}), 400
    if filepath is None:
        return jsonify({"error": "not found"}), 404
    try:
        content = filepath.read_text(encoding="utf-8")
    except Exception:
        return jsonify({"error": "read failed"}), 500

    # Parse markdown into messages
    messages = []
    current_name = None
    current_content = []
    topic = ""
    for line in content.split("\n"):
        if line.startswith("**Topic:**"):
            topic = line.replace("**Topic:**", "").strip()
        elif line.startswith("### "):
            if current_name and current_content:
                messages.append({"name": current_name, "content": "\n".join(current_content).strip()})
            current_name = line[4:].strip()
            current_content = []
        elif current_name is not None:
            current_content.append(line)
    if current_name and current_content:
        messages.append({"name": current_name, "content": "\n".join(current_content).strip()})

    return jsonify({"topic": topic, "messages": messages, "filename": filename})


@app.route("/api/profiles", methods=["GET"])
def api_profiles():
    profiles = cfg.list_profiles()
    rt = cfg._raw.get("roundtable", {})
    default_participants = rt.get("participants", [])
    return jsonify({
        "profiles": profiles,
        "participants": default_participants,
    })


@app.route("/api/start", methods=["POST"])
def api_start():
    global _disc_counter
    try:
        authorize_thread(_web_auth())
    except WebAuthorizationError as error:
        return jsonify({"error": str(error), "code": "WEB_AUTH_REQUIRED"}), 403
    data = request.get_json()
    topic = data.get("topic", "").strip()
    participants = data.get("participants", [])
    research_context = data.get("context", "").strip()
    save_filename = data.get("save_filename", None)

    if save_filename is not None:
        try:
            save_filename = _safe_discussion_path(
                _DISCUSSION_DIR, save_filename
            ).name
        except ValueError:
            return jsonify({"error": "invalid save filename"}), 400

    if not topic:
        return jsonify({"error": "请输入研究课题"}), 400
    if not participants:
        return jsonify({"error": "请至少选择一个模型"}), 400

    # If context is a file path, load its content
    if research_context:
        ctx_path = _PROJECT_ROOT / research_context
        if ctx_path.exists():
            research_context = ctx_path.read_text(encoding="utf-8")

    _disc_counter += 1
    disc_id = str(_disc_counter)
    msg_queue = queue.Queue()

    thread = threading.Thread(
        target=_stream_roundtable,
        args=(topic, participants, msg_queue, research_context, save_filename),
        daemon=True,
    )
    thread.start()

    DISCUSSIONS[disc_id] = {"thread": thread, "queue": msg_queue, "messages": []}
    return jsonify({"discussion_id": disc_id})


@app.route("/api/stream/<disc_id>")
def api_stream(disc_id):
    if disc_id not in DISCUSSIONS:
        return Response("data: {\"type\":\"error\",\"content\":\"discussion not found\"}\n\n", mimetype="text/event-stream")

    msg_queue = DISCUSSIONS[disc_id]["queue"]

    def generate():
        while True:
            try:
                msg = msg_queue.get(timeout=30)
                yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"
                if msg.get("type") in ("done", "error"):
                    break
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

    return Response(generate(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    })


@app.route("/api/discussions/<disc_id>", methods=["DELETE"])
def api_stop(disc_id):
    try:
        authorize_mutation(
            _web_auth(),
            operation="WEB_DELETE",
            callable_name="api_stop",
            paths=(),
        )
    except WebAuthorizationError as error:
        return jsonify({"error": str(error), "code": "WEB_AUTH_REQUIRED"}), 403
    if disc_id in DISCUSSIONS:
        DISCUSSIONS[disc_id]["queue"].put({"type": "stopped"})
        del DISCUSSIONS[disc_id]
    return jsonify({"status": "stopped"})


if __name__ == "__main__":
    print("\n  量化多模型圆桌讨论 — Web 界面")
    print("  http://localhost:5000\n")
    app.run(host="127.0.0.1", port=5000, debug=False, threaded=True)
