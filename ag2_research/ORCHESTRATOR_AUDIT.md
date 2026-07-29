# CURRENT_RUNTIME_BEHAVIOR

结论：**config.yaml 中声明的 AG2 Research OS v0.9 Sequential Pipeline 没有被运行时真正执行。**

运行时仍主要是 `autogen.GroupChat` 驱动的讨论流程，而不是 YAML 中声明的 deterministic sequential pipeline / gate state machine。

## 实际运行路径：brainstorm workflow

入口：`Orchestrator.run_brainstorm()`，见 `ag2_research/orchestrator.py:46-92`。

实际步骤：

1. 如果 `agent_ids is None`，只读取 `workflows.brainstorm.agents`：
   - `ag2_research/orchestrator.py:69-72`
   - 这会拿到当前 config 的 agent roster：
     `research_proposer -> data_validator -> experiment_executor -> risk_controller -> strategy_synthesizer -> system_orchestrator`
2. 创建 agents：`ag2_research/orchestrator.py:73-74`
3. 创建 `autogen.GroupChat`：`ag2_research/orchestrator.py:76-82`
   - `speaker_selection_method="round_robin"` **硬编码**
   - `allow_repeat_speaker=True` **硬编码**
   - `max_round=max_rounds` 使用函数参数默认值 `25`，不是 YAML 的 `workflows.brainstorm.max_rounds: 8`
4. 初始发言者：`agents.get("Coordinator") or list(agents.values())[-1]`
   - `ag2_research/orchestrator.py:85-87`
   - 当前没有 `Coordinator`，所以选择最后一个 agent，即 `System_Orchestrator`
5. 初始 prompt 来自 `_build_brainstorm_prompt()`：`ag2_research/orchestrator.py:298-318`
   - 该 prompt 仍是旧 brainstorm/debate 语义：
     - `Round 1: Each expert presents...`
     - `Round 2: Debate...`
     - `Round 3: Risk_Manager...`
     - `Round 4: Coordinator...`
     - `Alpha_Researcher, you go first.`
   - 这与当前 v0.9 角色名和 sequential pipeline 不一致。

### brainstorm 的实际顺序

由于 agents roster 最后是 `System_Orchestrator`，且它先 initiate，round-robin 大概率形成：

```text
System_Orchestrator(initial)
→ Research_Proposer
→ Data_Validator
→ Experiment_Executor
→ Risk_Controller
→ Strategy_Synthesizer
→ System_Orchestrator
→ Research_Proposer
→ ...重复直到 max_round=25 或对话终止
```

这只是 round-robin 讨论顺序，不是配置中声明的 gated pipeline。

特别是，配置期望：

```text
System_Orchestrator
→ Research_Proposer
→ [Registry/Proposal Gate by Orchestrator]
→ Data_Validator
→ [Data Gate by Orchestrator]
→ Experiment_Executor
→ [Execution/Preflight Gate by Orchestrator]
→ Risk_Controller
→ [Risk Gate by Orchestrator]
→ Strategy_Synthesizer
→ System_Orchestrator
```

但当前运行时不会在 `Research_Proposer` 与 `Data_Validator` 之间插入 Orchestrator gate，也不会在每个阶段后阻断/批准下一步。

## 实际运行路径：review workflow

入口：`Orchestrator.run_review()`，见 `ag2_research/orchestrator.py:94-137`。

实际步骤：

1. 读取 `workflows.review.agents`：`ag2_research/orchestrator.py:110-111`
2. 创建 agents：`ag2_research/orchestrator.py:113`
3. 创建 `autogen.GroupChat`：`ag2_research/orchestrator.py:115-121`
   - `speaker_selection_method="auto"` **硬编码**
   - `allow_repeat_speaker=True` **硬编码**
   - `max_round=wf.get("max_rounds", 10)`，这个字段会生效，目前为 4
4. 初始发言者：`agents.get("Coordinator") or list(agents.values())[-1]`
   - 当前 review roster 最后是 `system_orchestrator`，因此由 Orchestrator 发起
5. 初始 prompt 仍使用旧角色：
   - `Risk_Manager: Identify...`
   - `Strategy_Architect: Assess...`
   - `Coordinator: Provide...`
   - 见 `ag2_research/orchestrator.py:127-134`

### review 的实际顺序

`speaker_selection_method="auto"` 由 AG2 manager/LLM 自动选择说话者；并不保证：

```text
Risk_Controller → Strategy_Synthesizer → System_Orchestrator
```

也不保证 Orchestrator gate 发生在中间。

## 实际运行路径：solo workflow

`orchestrator.py` 中没有 `run_solo()`，也没有 generic `run_workflow(workflow_id)` dispatcher。

因此 `workflows.solo` 当前只是 YAML 配置，**不会被本 orchestrator runtime 自动执行**。

如果外部代码手动调用 `run_chat()` 并传入 solo agents：

- `run_chat()` 会使用 `speaker_selection_method="round_robin"`，见 `ag2_research/orchestrator.py:161-166`
- `run_chat()` 会让 `first_agent = list(agents.values())[0]` 发起，见 `ag2_research/orchestrator.py:173-174`
- 但它不会读取 `workflows.solo.type / speaker_selection / unsafe_for_production / max_rounds`

因此 solo workflow 的 YAML 安全标记目前不具备 runtime enforcement。

## RoundRobin / GroupChat 总结

当前 Research OS runtime 仍依赖：

- `autogen.GroupChat`
- hard-coded `speaker_selection_method="round_robin"` in brainstorm/run_chat
- hard-coded `speaker_selection_method="auto"` in review
- hard-coded `allow_repeat_speaker=True`

所以它仍然更接近“受 prompt 约束的多 Agent 讨论”，不是严格的 Research OS sequential state machine。

---

# CONFIG_RUNTIME_MISMATCH

以下 YAML 配置目前不会真正生效，或只部分生效。

## 1. `workflows.*.type: sequential`

位置：`ag2_research/config.yaml:438-480`

当前 runtime 没有读取 `type` 来选择 sequential runner。`run_brainstorm()` 和 `run_review()` 都直接创建 `autogen.GroupChat`。

影响：高。YAML 写的是 sequential，runtime 仍是 GroupChat。

## 2. `workflows.brainstorm.speaker_selection: sequential`

位置：`ag2_research/config.yaml:446`

不生效。`run_brainstorm()` 硬编码：

```python
speaker_selection_method="round_robin"
```

见 `ag2_research/orchestrator.py:80`。

影响：高。Sequential Pipeline 不被 runtime 执行。

## 3. `workflows.review.speaker_selection: sequential`

位置：`ag2_research/config.yaml:456`

不生效。`run_review()` 硬编码：

```python
speaker_selection_method="auto"
```

见 `ag2_research/orchestrator.py:119`。

影响：中-高。review 不按 deterministic order 执行。

## 4. `workflows.*.allow_repeat_speaker: false`

位置：`ag2_research/config.yaml:447,457,468`

不生效。`run_brainstorm()` / `run_review()` / `run_chat()` 均硬编码 `allow_repeat_speaker=True`。

影响：中。可能导致重复发言和讨论循环。

## 5. `workflows.brainstorm.max_rounds: 8`

位置：`ag2_research/config.yaml:449`

默认不生效。`run_brainstorm()` 使用函数参数默认值 `max_rounds=25`，没有 `wf.get("max_rounds")` fallback。

见 `ag2_research/orchestrator.py:51,79`。

影响：中-高。v0.9 的 bounded one-pass 保护没有真正应用到 brainstorm 默认路径。

## 6. `workflows.*.pipeline_order`

位置：`ag2_research/config.yaml:445,466`

完全不生效。`orchestrator.py` 没有读取 `pipeline_order`。

影响：高。配置中声明的：

```text
System_Orchestrator → Research_Proposer → ... → System_Orchestrator
```

不是 runtime source of truth。

## 7. `workflows.*.execution: one_pass_per_role`

位置：`ag2_research/config.yaml:448`

完全不生效。没有 runtime 检查每个角色只执行一次。

影响：高。round-robin 会循环。

## 8. `workflows.*.coordinator: system_orchestrator`

位置：`ag2_research/config.yaml:450,459,470`

不被直接读取。runtime 使用：

```python
agents.get("Coordinator") or list(agents.values())[-1]
```

见 `ag2_research/orchestrator.py:86,127`。

当前能选到 `System_Orchestrator` 是因为 roster 顺序把它放在最后，是“顺序偶然正确”，不是读取 coordinator 字段。

影响：中。配置字段与 runtime 脱节。

## 9. `workflows.solo.unsafe_for_production: true`

位置：`ag2_research/config.yaml:480`

不生效，因为没有 `run_solo()` 或 generic workflow dispatcher。

影响：中。安全标记只是文档化，不是 runtime guard。

## 10. v0.9 `memory_packet` / `registry_gate.owner`

位置：`ag2_research/config.yaml:132-171` 等。

这些规则已进入 agent prompt 和 config，但 orchestrator runtime 没有程序级创建 `memory_packet`，也没有程序级执行 Registry Gate。

影响：高。Memory Packet / Registry Gate 仍是 prompt-level soft control，不是 runtime state machine。

---

# MINIMAL_PATCH

## 最小补丁层级 1：让 YAML workflow 字段真正被读取

估计：约 **15–30 行**。

最少需要改：

1. `run_brainstorm()`：
   - 使用 `wf.get("max_rounds")` 作为默认值，而不是函数参数默认 25。
   - 读取 `wf.get("speaker_selection")`。
   - 读取 `wf.get("allow_repeat_speaker")`。
   - 读取 `wf.get("coordinator")`，而不是依赖 `list(agents.values())[-1]`。
2. `run_review()`：
   - 同样读取 `speaker_selection / allow_repeat_speaker / coordinator`。
3. 把旧 prompt 中的旧角色名替换为当前角色名。

但注意：这个补丁只能减少 config/runtime mismatch，**不能真正实现 gate-by-gate sequential state machine**。

## 最小补丁层级 2：让 Sequential Pipeline 近似按顺序跑

估计：约 **30–60 行**。

在层级 1 基础上：

1. 当 `wf.type == "sequential"` 时：
   - 使用 `wf.pipeline_order` 或 `wf.agents` 构造执行顺序。
   - 将 `max_rounds` 限定为 `len(pipeline_order)` 或 YAML max_rounds。
   - 禁止 repeat speaker。
2. 如果 AG2 不支持 `speaker_selection_method="sequential"`，则需要映射为受控 round-robin + 固定 roster + bounded rounds。

但注意：这仍然只是“固定顺序发言”，不是程序级 gate。Data_Validator 仍可能在 Orchestrator 没有真实批准的情况下按顺序发言。

## 最小补丁层级 3：真正执行 v0.9 Research OS gate state machine

估计：约 **70–120 行**，仍属于小补丁，不是重构。

需要新增或改造一个小型 dispatcher：

```text
run_workflow(workflow_id)
  load wf
  if wf.type == sequential:
      run System_Orchestrator step 0
      pass memory_packet to Research_Proposer
      run Orchestrator gate
      if pass -> Data_Validator
      run Orchestrator gate
      if pass -> Experiment_Executor
      run Orchestrator gate
      if pass -> Risk_Controller
      run Orchestrator gate
      if pass -> Strategy_Synthesizer
      final Orchestrator commit
```

关键点：

- Orchestrator 必须在每个 role artifact 后拥有一次 runtime decision turn。
- Gate fail 必须能 stop，不允许下一 Agent 自动发言。
- `max_revision_attempts=2` 必须由 Python 计数，而不是 prompt 自觉。
- `memory_packet` 最好由 Orchestrator runtime 组装/缓存，至少要作为单一 message 输入给下游 Agent。

这是让 v0.9 真正落地的最小有效 patch。

---

# ESTIMATED_RISK

**高。**

原因：

1. config.yaml 中声明的 sequential pipeline 当前没有被 runtime 执行。
2. Registry Gate / Data Gate / Risk Gate 仍是 prompt-level soft control，没有程序级阻断。
3. `brainstorm` 默认仍是 `round_robin + max_rounds=25 + allow_repeat_speaker=True`。
4. `_build_brainstorm_prompt()` 和 `run_review()` prompt 仍包含旧角色名，可能诱导模型按旧角色体系发言。
5. `pipeline_order / execution / unsafe_for_production / allow_repeat_speaker` 等关键 YAML 字段未被 dispatch 层读取。
6. 当前能让 Orchestrator 首发，主要依赖 agent roster 的最后位置，而不是 `coordinator: system_orchestrator` 字段。

风险性质：

- 对“能否运行”风险：中。代码可以运行。
- 对“是否真的执行 Research OS 控制语义”风险：高。
- 对“是否重复讨论/绕过 gate”风险：高。

---

# RECOMMENDATION

**建议：B. 小补丁。**

不建议 A（保持现状）：

- 因为 v0.9 的核心声明——Sequential Pipeline、Registry Gate centralization、Memory Packet、Loop Termination——目前多数只停留在 YAML/prompt 层，没有 runtime enforcement。

不建议 C（重构 orchestrator）：

- 目前不需要大规模重构。现有 `Orchestrator` 类、`create_agents()`、`ResearchConfig` 都可以保留。
- 只需要补一个 config-aware sequential dispatcher，或在现有 `run_brainstorm()` / `run_review()` 中加入 `wf.type == "sequential"` 分支。

推荐最小路径：

1. 第一阶段小补丁：
   - `run_brainstorm()` / `run_review()` 读取 YAML 的 `max_rounds / speaker_selection / allow_repeat_speaker / coordinator`。
   - 替换旧 role prompt 文案。
   - 预计 15–30 行。
2. 第二阶段小补丁：
   - 增加 `run_sequential_workflow()` 或 `wf.type == "sequential"` 分支。
   - 使用 `pipeline_order`。
   - Orchestrator 在每个 artifact 后做 gate decision。
   - Python 层执行 `max_revision_attempts=2`。
   - 预计 70–120 行。

最终建议：

```text
B. 小补丁
```

理由：当前不是策略逻辑问题，也不是角色体系问题，而是 **config 与 runtime dispatcher 没接上**。小补丁即可把 v0.9 的 YAML 控制语义落到 runtime；不需要重构整个 AG2 系统。
