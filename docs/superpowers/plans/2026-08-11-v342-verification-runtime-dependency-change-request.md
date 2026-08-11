# V342 Verification Runtime Dependency Change Request

> ID：`V342-DEP-001`
> 提出日期：2026-08-11
> 提出者：corrective-recovery executor（隔离 worktree，NON_AUTHORITATIVE_PREPARATION）
> 状态：`DRAFT / FOR USER DECISION / GATE HELD`
> 关联计划：[2026-08-11-v342-corrective-recovery-plan.md](2026-08-11-v342-corrective-recovery-plan.md) §6 Task 4 Step 4.1/4.2 与 §8 全局停止条件

## 1. 事实

`pip-compile`（pip-tools 7.6.0，与两个生产 lock 同 resolver）对合并后的
`requirements/verification-runtime.in`（control-plane.in + quant-runtime.in 全量合并）解析失败：

```
ResolutionImpossible:
  ag2-0.13.3 (以及 ag2[openai]-0.13.3) 元数据声明 httpx<1,>=0.28.1
  mootdx-0.11.7 (PyPI 最新版) 元数据声明 httpx>=0.25.0,<0.26.0
  → 两约束交集为空
```

- exact failure 全文：`research_state/control_plane/p0/attempts/p0-attempt-005/evidence/verification_runtime_resolve_failure.txt`（4787 bytes）。
- mootdx 无更新版本（PyPI 最新 0.11.7，requires_dist 仍为 `httpx<0.26.0,>=0.25.0`），不存在升级 mootdx 的路径。
- ag2 0.13.3 的 `httpx>=0.28.1` 是 **ag2 包自身元数据**，不是 control-plane.in 的 direct input 可以单独放宽的——任何基于 pip 元数据的解析器都会拒绝。
- **实证兼容证据**：当前系统 Python 3.13 环境安装的是 `httpx==0.25.0` + `ag2==0.13.3` + `openai==2.38.0`，该组合已完整跑通控制平面全量 discovery（**1593 tests OK**，含 ag2/openai 适配器路径的 controller 测试）。即 ag2 0.13.3 在 httpx 0.25.x 下功能正常，偏离只发生在声明的元数据层。
- 计划 §4.2 规定：单一 verification runtime 无解时"建立 dependency change request 并停止 Gate；不拆成两个环境、不跳过 import tests"。因此本 CR 是 Gate HOLD 的正式触发点。

## 2. 提议选项（需用户三选一）

### Option A（推荐）：verification runtime 固定 `httpx==0.25.2`，接受声明元数据偏离

- 仅修改 `requirements/verification-runtime.in`（新增 `httpx==0.25.2` 行）；**两个生产 .in 与两个生产 lock（control-plane.lock / quant-runtime.lock）一律不改**。
- verification-runtime.lock 生成方式：先安装 httpx==0.25.2 后以 `pip-compile` 解析**除 ag2 外的全部 direct inputs**，再对 ag2==0.13.3 与其 openai extra 依赖做显式条目 + 文档化偏离声明（偏离仅限 verification/test 环境，不进生产）。
- 风险：若 ag2 未来在 httpx 0.25 下暴露不兼容，影响范围仅测试验证环境；生产 control-plane 环境仍为 httpx 0.28.1。
- 依据：当前环境 1593 tests 实证。

### Option B：修改 control-plane.in 的 httpx 约束为 `<0.26`

- 影响生产 control-plane 环境的依赖声明（当前 lock 为 0.28.1，需重新解析生产 lock 并重新验证生产环境）。
- 与 ag2 0.13.3 元数据冲突仍存在（ag2 声明 >=0.28.1），未来 `pip install -r` 会报不一致；不推荐。

### Option C：豁免计划 §4.2，verification 拆成 control-plane 与 quant 两个环境

- 违反计划"不拆成两个环境、不跳过 import tests"；需要用户明确豁免该计划条款，且 full discovery 必须在两个环境分别以完整 receipts 运行，可信度按计划 F4 标准重新评估。

## 3. 建议批准语句

```
APPROVE_DEPENDENCY_CHANGE id=V342-DEP-001 option=A
```

## 4. 不批准的后果

- 按计划 §8：verification runtime 无解 → 所有 Gate（P0/P6/P7/P8/C0）保持 HOLD；P0 无法 re-gate，后续阶段无法串行推进。
- 当前系统 Python 环境（httpx 0.25.0）可继续用于日常测试运行，但不能作为正式 verification runtime 关闭 Gate。
