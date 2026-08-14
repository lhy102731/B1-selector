# V3.4.2 CR-010 功能实现整改 — 完成度报告（2026-08-15）

> 对照：`docs/superpowers/reviews/2026-08-15-v342-cr010-content-completion-review.md`（R01-R07）
> 规则遵守：先复现"现状失败"再改代码（复现证据：`research_automation/control_plane/cr010_r01_r07_reproduction_evidence.md`）；未修改 set_param/reset/rollback；未改变测试范围/cycles/seed/阈值；未停止用户进程；修复均配套行为测试；旧 evidence/Gate/closure/ruling 零改写。
> 结论：**P8 全部硬门通过；C0 部分硬门未完成 → HOLD**

## 1. 逐 R finding 状态

| Finding | 状态 | 修复 commit | 关键行为变化 |
|---|---|---|---|
| R01 evaluate_v2 调用路径 | **FIXED** | 973b7a3 | `evaluate_v2` 从真实 worker payload 派生 outcome（`derive_outcome`，拒绝 caller 指定 outcome），Authority broker 原子消费 nonce；缺 payload/非法 outcome/broker 不一致全部 fail-closed；7 个真实行为测试（含负向） |
| R02 RESULT_STAGED 验证 | **FIXED** | 973b7a3 | 新增 `final_eval_evidence.py`：content-addressed object + per-ticket fixed claim（同卷、唯一、committed blob、内容 hash）预暂存验证；orchestrator + stores 双层 fail-closed；dangling/错误 hash/orphan/stale version 测试 |
| R03 recovery lease durable | **FIXED** | 8f29d42 | schema v3 新增 `final_eval_recovery_leases_v1`（durable ISSUED→COMPLETED）；`_close_final_eval_binding`（RESULT_STAGED→CLOSED）与 `_finalize_final_eval_binding`（CLOSED→AUTHORITY_TERMINAL+Authority finish）拆分为独立事务；CRASH_AFTER.CLOSED 新 crash 边界（7 个边界）；`expected_version` fail-closed；lease 身份/phase/ticket/evidence 绑定 |
| R04 FinalEvalRuntime 接线 | **FIXED** | 5afb132 | runtime 驱动 durable saga（bind→evaluate_v2→orchestrate→reconcile），返回真实 committed claim ref（evidence_ref 永不为 None）；binding 校验 fail-closed；OPEN_HOLDOUT seam 经 runtime 唯一调用（测试验证）；生产 wiring + 行为测试 |
| R05 production fixture | **FIXED** | eb23c2a | `rollout_chaos_fixtures.py` 全部 fixture 生产化（protocol/member/execution_spec/claim_grant/authorized_campaign/authority_fixture 从 tests.* 移植）；`rollout_chaos.py` 零 `tests.*` import；duck-typed 身份 provider |
| R05b worker 真实 step | **PARTIAL** | 38f4b38 | worker 不再固定 SUCCEEDED：对 durable root 计算真实 state digest、真实 PID 身份、durable pause 事件、committed evidence refs；`verify`/`recover` 执行真实校验。**未完成**：官方 campaign 仍由主进程模拟驱动，未逐 step 走 worker 子进程协议 |
| R06 子进程协议/replay | **PARTIAL** | 38f4b38, a22fe01 | durable pause/resume 真实 journal 事件 ✓；NetworkGuard 真实拦截 + deny probe ✓；exact invariant set fail-closed（10/10，缺任一项 report 失败）✓；AtomicPublisher 重写（同卷 temp+fsync+原子 create-only link，并发/崩溃测试）✓；官方 run 使用 fresh-process worker verify ✓。**未完成**：second-root replay（不同进程+不同 root）因 event payload 内嵌 root 路径导致跨 root digest 不等（已记录为 gap，未降级为假通过） |
| R07 no-side-effect | **FIXED** | 3eda8aa | 新增 `c0_no_side_effect.py` 全表面快照（Authority/Operational store、data/knowledge/config/strategy/research_automation/tools 树、保护文件、网络探针计数、git）；driver stage3c 接入；6 个测试（篡改任一表面 fail-closed） |

## 2. 修改的源码与测试

- `final_evaluator.py` — evaluate_v2 真实 outcome 派生 + 原子消费 + Mapping/FinalEvalSagaError 导入
- `final_eval_evidence.py`（新）— object/claim 发布 + 预暂存验证
- `final_eval_orchestrator.py` — 新 sink 合同 + 预暂存验证 + expected_version fail-closed + CLOSED crash 点
- `final_eval_reconciler.py` — close→finalize 两段式 + CRASH_AFTER.CLOSED
- `stores.py` — schema v3 + durable recovery lease + close/finalize 拆分 + staging 验证
- `final_eval_runtime.py` — durable saga 驱动 + binding 校验 + 真实 evidence_ref
- `rollout_chaos_fixtures.py` — 生产化 fixture 移植（零 tests.*）
- `rollout_chaos.py` — 真实 invariant 三件套 + exact-set fail-closed + fresh-process worker verify + 语义 replay 探测
- `rollout_chaos_worker.py` — 真实 step 执行（digest/PID/pause/evidence）
- `c0_no_side_effect.py`（新）— 全表面 no-side-effect
- 测试：`test_control_plane_final_evaluator/orchestrator/stores/runtime/rollout_chaos(_worker/_publication/_fixtures)/c0_no_side_effect` 全部配套行为测试

## 3. 验证命令真实结果

| 命令 | 结果 |
|---|---|
| `python -m unittest tests.test_control_plane_final_eval_saga tests.test_control_plane_final_eval_runtime tests.test_control_plane_final_evaluator tests.test_control_plane_final_eval_orchestrator` | **Ran 117 tests OK**（含 7 个 R02 负向 + 7 个 crash 边界 + fresh-process recovery + runtime durable saga） |
| `python -B -s -m unittest tests.test_control_plane_rollout_chaos_fixtures tests.test_control_plane_rollout_chaos_worker tests.test_control_plane_rollout_chaos tests.test_control_plane_rollout_chaos_publication` | **Ran 35 tests OK**（含 4 个 exact-set fail-closed + 4 个并发/崩溃 publication + worker 真实 step） |
| `python -B -s -m unittest discover -s tests -p "test_*.py"` | **Ran 2630 tests OK（skipped=1）** — receipt: `receipt_full_discovery.json`（绑定 HEAD 8deb0b3） |
| 附加：`python -m unittest tests.test_control_plane_c0_no_side_effect` | **Ran 6 tests OK**（R07） |

## 4. 仍存在的功能缺口

1. **[C0 阻断] 官方 campaign 未逐 step 走 worker 子进程协议** — `run_c0_simulation` 主进程内驱动模拟；worker 的 verify/recover 已真实执行并被官方 run 调用，但 24-cycle 的每步执行仍非 worker 子进程（fresh-process crash/recovery 尚未在 campaign 执行路径上实现）。
2. **[C0 阻断] second-root replay 未接通** — 跨 root 时 event payload 内嵌 fixture root 路径，digest/semantic 不等；需定位并消除 payload 中的绝对路径后，才能实现不同进程+不同 root 的 replay 一致性验证（未以降级方式假通过）。
3. **[C0 中] worker 的逐 step 行为仍为有界实现** — prepare/start/model_call/... 各 step 当前共享 digest/evidence 计算，未各自执行完整 controller 转移（未伪造完成，如实记录）。

## 5. 最终判定

**P8 硬门（R01-R04 + evaluate_v2 行为测试 + RESULT_STAGED 验证 + durable recovery + 7 crash 边界 + runtime 接线 + OPEN_HOLDOUT 唯一入口）：全部通过。**

**C0 硬门（R05-R07）：R05/R07 通过；R06 的 invariant fail-closed、durable pause/resume、NetworkGuard、AtomicPublisher 通过；但官方 rollout 的 worker 子进程逐 step 执行与 second-root replay 未完成。**

**结论：HOLD（C0 两个硬门未闭环；P8 已闭环）。**
