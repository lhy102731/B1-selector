# AG2 Research OS — README

> 多智能体策略研究框架（Research OS）。
> 本目录 `ag2_research/` 是**研究底座**：提供多智能体编排、记忆路由（MemoryRouter）、注册表门控（RegistryGate）。
> 它被上层 `research_automation/`（全自动实验链路）调用，本身不直接驱动全自动闭环。

---

## 1. 这一层是什么

`ag2_research/` 用 [AG2 / AutoGen](https://github.com/ag2ai/ag2) 实现一个多角色研究框架：

- **Orchestrator** —— 编排 brainstorm / review / roundtable 等会话，管理 GroupChat。
- **MemoryRouter** —— 统一读取各策略的 memory / registry / snapshot / handoff / project YAML（在仓库根以 glob 匹配）。
- **RegistryGate** —— 对新假设做查重与分类（duplicate / partial_overlap / failed / verified / open / none）。
- **agents / tools / config** —— 角色定义、工具注入、会话配置。

对外暴露（`__init__.py`）：
```python
from ag2_research import Orchestrator, ResearchConfig, ResearchSession
```

---

## 2. 与「全自动链路」的关系

```
research_automation/   ← 全自动实验链路（入口 run_research_cycle.py）
        │  import
        ▼
ag2_research/          ← 本目录：Research OS（MemoryRouter / RegistryGate / Orchestrator）
        │  import
        ▼
strategy/ utils/ backtest_optimized.py ...
```

- **全自动链路**（参数自动实验闭环）的入口在 `../run_research_cycle.py`，主体在 `../research_automation/`。
- 那条链路会 import 本目录的 `MemoryRouter`（读记忆/注册表）和 `RegistryGate`（假设查重）。
- 本目录的 `Orchestrator`（多智能体会话）是**交互式研究**入口，由 `../run_research.py` 驱动，与全自动链路是互补关系，不是同一个东西。

> 简言之：想跑「全自动参数实验」→ 用上层 `run_research_cycle.py`；想跑「多智能体讨论」→ 用 `run_research.py`（本目录提供引擎）。

---

## 3. 目录内容

| 文件 | 职责 |
|---|---|
| `__init__.py` | 公共导出：`Orchestrator / ResearchConfig / ResearchSession` |
| `orchestrator.py` | 会话编排 + `MemoryRouter` + `RegistryGate` |
| `agents.py` | 创建各角色 agent |
| `tools.py` | 为 agent 注入工具（含对 `backtest_optimized` 的引用） |
| `config.py` / `config.yaml` | 会话与角色配置 |
| `ROLE_SYSTEM.md` | 6 角色治理规范 |
| `CONTROL_LAYER_SPEC.yaml` | 控制层规范 |
| `ORCHESTRATOR_AUDIT.md` | 编排器审计（注：指出声明的顺序管道在运行时未真正执行） |
| `CHANGELOG.md` | 变更日志 |
| `templates/` | snapshot / handoff 模板 + roundtable.html |
| `discussions/` | 结构化讨论档案规范（与顶层 `discussions/` 文本日志互补） |

---

## 4. 怎么用

### 4.1 交互式多智能体研究（本层直接用法）

```bash
# 入口在上层
python ../run_research.py brainstorm --topic "How to improve B1 win rate above 60%"
python ../run_research.py roundtable
```

或编程式：
```python
from ag2_research import Orchestrator

orch = Orchestrator()
orch.run_brainstorm(
    topic="How to improve B1 win rate above 60%?",
    research_context="V2 is production champion ...",
)
```

### 4.2 全自动实验链路（上层，调用本层）

> 这是「全自动链路」的真正入口，主体在 `../research_automation/`。

**第 0 步：选策略**
- `b1` —— full 能力，baseline=真实 champion，delta 真实可比（推荐先跑）
- `brick` —— experimental，baseline=V2 默认（非 V2.1 champion），会打印 CAVEAT
- `b3` —— 无 backtest harness，启动即拒，别跑

**第 1 步：干跑确认（不真跑）**
```bash
python ../run_research_cycle.py --strategy b1 --source proposer --rounds 1 --per-round 1 --dry-run
```
看输出：`capability=full`、命令含 champion 全套参数 + 单变量 override、task_id 是 md5 后缀。

**第 2 步：最小闭环（1 轮 2 候选，~1 分钟）**
```bash
python ../run_research_cycle.py --strategy b1 --source proposer --rounds 1 --per-round 2
```
跑完看：
```bash
cat ../research_automation/_output/candidates/candidate_pool.yaml
ls ../research_automation/_output/reports/
```

**第 3 步：无人值守（5 轮 × 4 候选，~10 分钟）**
```bash
python ../run_research_cycle.py --strategy b1 --source proposer --rounds 5 --per-round 4
```
后台跑：
```bash
python ../run_research_cycle.py --strategy b1 --source proposer --rounds 5 --per-round 4 > run.log 2>&1 &
```

**先跑自己的 idea，跑完自动续：**
```bash
python ../run_research_cycle.py --source hybrid \
  --idea "pe_max=30,50,80" --idea "turnover_max" \
  --auto-source proposer --rounds 5 --per-round 4
```

**限定时长：**
```bash
python ../run_research_cycle.py --strategy b1 --source proposer --rounds 20 --per-round 4 --max-minutes 30
```

---

## 5. 停止全自动链路

三选一：
- 双击 `../stop.bat`
- 建空文件 `../research_automation/_output/STOP`（下一轮前检测到即停）
- Ctrl+C

---

## 6. 跑完看什么

全自动链路产物全在 `../research_automation/_output/`：

```
runs/<cycle_id>/
  ├── baseline/result/          # champion 基线回测
  └── experiments/<id>/
      └── report.md             # 单实验报告（delta、changed_files=(none)）
candidates/candidate_pool.yaml  # 全候选账本（promotion_status + delta_vs_baseline）
reports/<策略>_r<轮次>_<日期>_<时间>.md  # 每日汇总
```

报告分类：
- **Verified** —— 建议晋升（仍需人工决定）
- **Tested** —— 比 baseline 好，未交叉验证
- **Rejected** —— 变差或没跑成

> **晋升永远人工**：报告只是建议。看哪个 candidate 的 `delta_vs_baseline` 真正正向且稳定，再手动改 champion（仓库根的 `registry_*/snapshot_*` YAML）。链路自己不会动这些生产记忆。

---

## 7. 安全边界

- 全自动链路**只写** `research_automation/_output/`（delta 工件 + 候选池 + 报告）。
- **永不修改** Champion / Registry / Snapshot / Handoff / Memory（仓库根的 `registry_*` / `snapshot_*` / `handoff_*` / `*_memory.yaml`）。
- 晋升 = 人工 only。
- 详见 `../research_automation/safety.py` 的 `SAFE_WRITE_ROOTS` 与 `assert_safe_path`。

---

## 8. 依赖

- `autogen`（AG2 / AutoGen）—— 多智能体运行时
- `pyyaml` —— 读 config / 记忆 YAML
- 上层策略与回测：`../strategy/`、`../utils/`、`../backtest_optimized.py`、`../run_b1_v3.py`、`../backtest_brick_v2.py`

---

## 9. 已知问题（来自审计，供参考）

- `ORCHESTRATOR_AUDIT.md` 指出：`config.yaml` 声明的顺序管道在运行时未真正执行（`orchestrator.py` 用 `round_robin` 而非有序管道，且部分 prompt 引用了已不存在的旧角色名）。
- `templates/` 中 snapshot/handoff 模板的 `version` 字段（2.0）与仓库根实际文件（1.0）不一致。
- 本目录 `discussions/` 与顶层 `discussions/` 命名重叠但内容不同（前者是结构化档案规范，后者是 roundtable 文本日志）。

这些是 Research OS 内部的一致性问题，不影响全自动链路（全自动链路主要用本目录的 `MemoryRouter` / `RegistryGate`，不用 `Orchestrator` 的顺序管道）。
