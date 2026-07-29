# Research Automation Layer v1

> **Current execution status:** this is a legacy, unaudited engine retained for
> compatibility and evidence replay. Direct execution is fail-closed; it may
> run only through a later authorized control-plane campaign adapter. The
> examples below describe the legacy design and are not production runbooks.

在已完成的 **AG2 Research OS** 之上构建的“自动实验闭环系统”。它把原本的人工流程
（AG2讨论 → 人工改代码 → 人工跑回测 → 人工写报告 → 人工更新 Registry/Snapshot/Handoff
→ 再回到 AG2）自动化为一条可编排、可门控、可回退的闭环。

> 边界声明：本层**不优化策略、不优化 AG2、不修改任何现有 Research Memory / Project Memory /
> Registry 文件**。所有自动产出均为 **增量 delta 工件**，写入 `research_automation/_output/<experiment_id>/`
> 暂存目录，由人工/后续合并步骤决定是否并入真实 memory。Claude Code 调用为**抽象接口层，不含真实 API**。

---

## 1. 目录结构（Phase 1 / Phase 10）

```
research_automation/
├── README.md                  # 本文件：架构 / 图 / 实施顺序
├── __init__.py                # 公共导出面
├── experiment_schema.yaml     # Phase 2/3/5/6/9 的契约 schema
├── experiment.py              # Phase 2/3 Experiment 对象 + 状态机
├── task_queue.py              # 实验任务队列（FIFO + 可选 JSON 持久化）
├── automation_controller.py   # Phase 1 控制器：驱动整个生命周期
├── experiment_runner.py       # Phase 4 执行层：Claude Code / 回测 抽象 + Stub
├── result_parser.py           # Phase 5 回测结果标准化解析器
├── report_generator.py        # Phase 5 实验报告生成
├── registry_updater.py        # Phase 6 Registry 自动分类 + entry 生成
├── snapshot_updater.py        # Phase 7 Snapshot 增量 delta
├── handoff_updater.py         # Phase 8 Handoff 增量 delta
├── approval_gate.py           # Phase 9 Human Approval Gate
└── _output/<experiment_id>/   # 暂存输出（task.md / result / report / *_delta.yaml / experiment.json）
```

---

## 2. YAML Schema 总览（Phase 10-②）

完整契约见 `experiment_schema.yaml`，要点：

- **experiment**：统一实验对象字段（见下方 Phase 2）。
- **lifecycle**：状态机 states / failure_states / transitions（见下方 Phase 3）。
- **standardized_metrics**：`sharpe, cagr, win_rate, max_drawdown, ndcg, ic, rank_ic, turnover, trades, extra, source`。
- **registry_taxonomy**：`duplicate / partial_overlap / failed / verified / open / none`（复用 Research OS 同一套）。
- **approval_gate.escalate_if**：>5 改动文件 / 删除文件 / schema 变更 / 回测异常 / Registry 冲突 / Memory 冲突。
- **output.apply_policy**：只写 delta，不自动改真实 memory。

---

## 3. Python 类设计（Phase 10-③）

| 模块 | 主要类 / 函数 | 职责 |
|---|---|---|
| `experiment.py` | `Experiment`, `ExperimentStatus`, `Proposal`, `RegistryReference`, `StandardMetrics`; `TRANSITIONS` | 统一对象 + 状态机（`transition/escalate/reject/fail`，非法跃迁抛 `LifecycleError`） |
| `task_queue.py` | `TaskQueue`, `ExperimentTask` | 优先级 FIFO，可选 JSON 持久化 |
| `experiment_runner.py` | `CodeChangeExecutor`(ABC), `ClaudeCodeExecutor`, `StubCodeChangeExecutor`, `BacktestExecutor`(ABC), `StubBacktestExecutor`, `generate_experiment_task_md()` | Phase 4 执行边界，依赖注入；无真实 API |
| `result_parser.py` | `BacktestResultParser` | Phase 5：json→report.md→equity.csv 优先级解析，标准化输出 |
| `report_generator.py` | `ReportGenerator` | 由 Experiment+metrics 生成 `report.md` |
| `registry_updater.py` | `RegistryUpdater` | Phase 6：复用 `MemoryRouter.registry_gate` 分类 + 生成 `registry_entry.yaml`，自增 id |
| `snapshot_updater.py` | `SnapshotUpdater` | Phase 7：仅增量 `snapshot_delta.yaml` |
| `handoff_updater.py` | `HandoffUpdater` | Phase 8：`current_best_hypothesis/current_blockers/next_experiments/latest_results` |
| `approval_gate.py` | `ApprovalGate`, `ApprovalDecision` | Phase 9：四类检查（registry/code/backtest/memory） |
| `automation_controller.py` | `AutomationController` | Phase 1：驱动状态机、调用各模块、产出 AG2 反馈包 |

复用（不复制）Research OS：`ag2_research.orchestrator.MemoryRouter` 与 `RegistryGate` 是
Registry 分类与 memory 读取的**唯一来源**。

---

## 4. 模块关系图（Phase 10-④）

```
                         ┌─────────────────────────────┐
        AG2 (Research OS) │  run_sequential_workflow     │  产出 proposal
                         └──────────────┬──────────────┘
                                        │ proposal(dict)
                                        ▼
                                  TaskQueue ──► ExperimentTask
                                        │
                                        ▼
                        ┌────────────────────────────────────────┐
                        │          AutomationController            │
                        │  (驱动 Experiment 状态机 + 门控编排)       │
                        └───┬─────┬─────┬─────┬─────┬─────┬─────┬──┘
                            │     │     │     │     │     │     │
              ┌─────────────┘     │     │     │     │     │     └──────────────┐
              ▼                   ▼     ▼     ▼     ▼     ▼                     ▼
       RegistryUpdater     ApprovalGate │  experiment_runner  ResultParser  Report/Reg/Snap/Handoff
       (分类/冲突/entry)   (人审门)      │  (ClaudeCode/Backtest │ (标准化指标)   Updaters (增量 delta)
              │                          │   抽象 + Stub)        │
              └──────────► reuse ◄───────┴──── MemoryRouter / RegistryGate (ag2_research)
                                                  (唯一读 memory + 唯一注册表分类)
                                        │
                                        ▼
                          out/<id>/  *.md / result / *_delta.yaml / experiment.json
                                        │
                                        ▼
                       ag2_feedback_packet() ──► 回到 AG2 继续讨论（闭环）
```

---

## 5. 数据流图（Phase 10-⑤）

```
proposal(dict)
  → Experiment(PROPOSED)
  → RegistryUpdater.classify ──► RegistryReference{status, matched_id, action}
  → ApprovalGate.check_registry / check_memory_conflict
        conflict?  ──► ESCALATED_TO_USER
        reject?    ──► REJECTED
  → APPROVED → generate_experiment_task_md() ──► out/<id>/experiment_task.md
  → IMPLEMENTING → CodeChangeExecutor.apply() ──► CodeChangeResult{changed_files, git_commit, ...}
        ApprovalGate.check_code_change ──► (>5/删除/schema) ESCALATED_TO_USER
  → BACKTESTING → BacktestExecutor.run() ──► out/<id>/result/{metrics.json, equity.csv, trades.csv}
        BacktestResultParser.parse ──► StandardMetrics
        ApprovalGate.check_backtest ──► (anomaly) ESCALATED_TO_USER
  → REPORTING → ReportGenerator ──► out/<id>/report.md
  → REGISTRY_UPDATE → RegistryUpdater.build_entry/write_delta ──► out/<id>/registry_entry.yaml
  → SNAPSHOT_UPDATE → SnapshotUpdater ──► out/<id>/snapshot_delta.yaml
  → HANDOFF_UPDATE → HandoffUpdater ──► out/<id>/handoff_delta.yaml
  → COMPLETED → ag2_feedback_packet() ──► AG2
```

---

## 6. 状态机图（Phase 10-⑥）

```
        PROPOSED
        │   │   └────────────► REJECTED        (registry action=reject)
        │   └────────────────► ESCALATED_TO_USER (registry/memory 冲突)
        ▼
        APPROVED ─────────────► ESCALATED_TO_USER / FAILED
        ▼
        IMPLEMENTING ─────────► ESCALATED_TO_USER (>5/删除/schema) / FAILED
        ▼
        BACKTESTING ──────────► ESCALATED_TO_USER (anomaly) / FAILED
        ▼
        REPORTING ────────────► FAILED
        ▼
        REGISTRY_UPDATE ──────► ESCALATED_TO_USER / FAILED
        ▼
        SNAPSHOT_UPDATE ──────► FAILED
        ▼
        HANDOFF_UPDATE ───────► FAILED
        ▼
        COMPLETED   (终态)

终态：COMPLETED / FAILED / ESCALATED_TO_USER / REJECTED
非法跃迁由 Experiment.transition() 抛 LifecycleError 拦截。
```

---

## 7. 后续实施顺序（Phase 10-⑦）

当前交付为**接口层 + 抽象层 + 离线可运行闭环（Stub 驱动）**。落地真实自动化的建议顺序：

1. **接 AG2 → 队列**：把 `run_sequential_workflow` 的 proposal 产出转成 `ExperimentTask` 入队（薄适配器）。
2. **实现 `ClaudeCodeExecutor.apply()`**：用受控子进程真正调用 `claude code experiment_task.md`，解析返回的 changed_files / git commit（替换 Stub）。
3. **实现真实 `BacktestExecutor`**：包装现有 `backtest_optimized.py`，按 scope 运行并输出 `result/metrics.json`（不改回测逻辑，仅包装命令）。
4. **校准 `BacktestResultParser`**：对齐项目真实 metrics 字段名 / 报告格式。
5. **Registry 合并器（人审后）**：新增一个显式的 `apply_delta` 工具，把 `registry_entry.yaml` 追加进真实 registry（默认关闭，需人工确认）。
6. **Snapshot/Handoff 合并器**：同上，仅在人审通过后并入。
7. **接 AG2 回环**：把 `ag2_feedback_packet()` 作为下一轮 AG2 讨论的输入 message。
8. **持久化与并发**：`TaskQueue` 换为持久后端，`AutomationController` 支持并发 worker（每实验独立 workspace / git worktree）。

---

## 8. 离线闭环验证（已通过）

`AutomationController` 用默认 Stub 执行器可完整跑通：

- A. 新颖假设 → `COMPLETED`，产出 task.md / result / report.md / registry_entry / snapshot_delta / handoff_delta / experiment.json。
- B. 改动 >5 文件 → `ESCALATED_TO_USER`。
- C. 删除文件 + schema 变更 → `ESCALATED_TO_USER`。
- D. 命中已 FAILED 的 `pe_max=30` → Registry 冲突 → `ESCALATED_TO_USER`。
- E. 回测 anomaly → `ESCALATED_TO_USER`。
- F. 队列 drain 双任务 → 均 `COMPLETED`，生成 AG2 反馈包。
- G. `ClaudeCodeExecutor` 仅返回计划命令 `claude code experiment_task.md`，不真正执行（接口层）。

真实 `registry_b1_v2.yaml / snapshot_b1.yaml / handoff_b1_v1.yaml / b1_memory.yaml / project_b1_v2.yaml`
在验证后保持未被修改。
