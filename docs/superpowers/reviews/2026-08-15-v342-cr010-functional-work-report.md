# V3.4.2 CR-010 功能实现整改 — 会话工作报告（2026-08-15）

> 报告范围：2026-08-15 功能整改会话的完整工作记录（对照独立复核 R01-R07）
> 基线：`docs/superpowers/reviews/2026-08-15-v342-cr010-content-completion-review.md`（P8/C0 HOLD）
> 最终 HEAD：`8398ab4`（工作树干净）
> 状态：**P8 全闭；C0 三个硬门未闭 → HOLD**（如实记录，未伪造完成）

## 1. 会话目标与方法

按用户指令执行 CR-010 功能实现整改：只判断/修复实际功能，不把流程问题当功能；不通过补报告/JSON/disposition 伪造完成；旧 Gate/closure/receipt/ruling/history evidence 全部保留。

方法：**先逐项复现"现状失败"证据（`research_automation/control_plane/cr010_r01_r07_reproduction_evidence.md`），再改代码，修复必配行为测试**。

## 2. 复现结论（全部 confirmed）

| Finding | 复现证据 |
|---|---|
| R01 | `evaluate_v2` 调 `consume(request)` 缺 keyword-only `outcome` → TypeError（真实调用复现） |
| R02 | orchestrator 测试用不存在路径的 sink 仍进入 RESULT_STAGED（文件 ls 不存在，测试 OK） |
| R03 | recovery lease 仅内存对象；CLOSED→AUTHORITY_TERMINAL→ticket finish 同一事务；`expected_version` 从未校验 |
| R04 | `FinalEvalRuntime` 纯内存 `_saga_state/_steps` + `evidence_ref=None` + 生产零调用者 |
| R05 | `rollout_chaos.py` lazy import 7 个 `tests.*` fixture；worker 对所有 step 固定返回 SUCCEEDED |
| R06 | produced=14 vs EXACT=10，缺 `durable_pause_resume/fresh_process_identity/network_denied` 仍写 `pass=true`；`require_exact_invariant_set` 自身 `Mapping` NameError；AtomicPublisher 直接 O_EXCL 写最终路径（无 temp/rename/崩溃安全）；pause 仅 marker；replay 同进程同 root |
| R07 | stage3c no-side-effect 仅 git status 行 |

## 3. 修复与行为变化（按 commit）

| Commit | 修复 | 关键行为变化 |
|---|---|---|
| `973b7a3` | R01+R02 | `evaluate_v2` 从真实 worker payload 派生 outcome（`derive_outcome` 拒绝 caller 指定），Authority broker 原子消费 nonce；缺 payload/非法 outcome/broker 不一致 fail-closed。新增 `final_eval_evidence.py`：content-addressed object + per-ticket fixed claim（同卷/唯一/committed blob/内容 hash）预暂存验证；orchestrator + stores 双层 fail-closed；dangling/错误 hash/orphan/stale-version 负向测试 |
| `8f29d42` | R03 | schema v3 新增 `final_eval_recovery_leases_v1`（durable ISSUED→COMPLETED）；`_close_final_eval_binding`（RESULT_STAGED→CLOSED）与 `_finalize_final_eval_binding`（CLOSED→AUTHORITY_TERMINAL+Authority finish）拆独立事务；**CRASH_AFTER.CLOSED 新边界（共 7 个）**；`expected_version` fail-closed；lease 身份/phase/ticket/evidence 绑定 |
| `5afb132` | R04 | `FinalEvalRuntime` 驱动 durable saga（bind→evaluate_v2→orchestrate→reconcile），返回真实 committed claim ref（**evidence_ref 永不为 None**）；binding 校验 fail-closed；OPEN_HOLDOUT seam 唯一经 runtime 调用 |
| `eb23c2a` | R05 | 全部 fixture 移植到 `rollout_chaos_fixtures.py`（protocol/member/execution_spec/claim_grant/authorized_campaign/authority_fixture）；**rollout_chaos.py 零 `tests.*` import**；duck-typed 身份 provider |
| `a0d0f26` | R06-C0-3/C0-4 | **exact invariant set fail-closed（10/10）**：`durable_pause_resume`（真实 journal pause/resume 事件）、`fresh_process_identity`（crash-recovery 身份追踪）、`network_denied`（NetworkGuard 真实拦截+deny probe）三件套；per-cycle 冗余项移入诊断；`run_c0_simulation` 强制 exact-set。**AtomicPublisher 重写**：同卷 temp+fsync+原子 create-only `os.link`（并发同字节幂等/异字节冲突/写入中崩溃仅留 orphan temp），4 个并发/崩溃测试 |
| `3eda8aa` | R07 | 新增 `c0_no_side_effect.py` 全表面快照（Authority/Operational store、data/knowledge/config/strategy/research_automation/tools 树、保护文件、网络探针计数、git status）；driver stage3c 接入；篡改任一表面 fail-closed，6 个测试 |
| `38f4b38`/`a22fe01` | R05b/R06-partial | worker 执行**真实有界 step**（对 durable root 计算真实 state digest、真实 PID 身份、durable pause 事件、committed evidence refs；`verify`/`recover` 真实校验）；官方 run 通过 **fresh-process worker 子进程 verify**（真实 PID 证据入 `worker_verify` 字段，scenario_log 保持确定性）；second-root replay 因 root 路径泄漏记录为 gap（未降级假通过） |
| `8deb0b3`/`f014907` | schema 对齐 | migration/durability/coordinator 测试断言对齐 v3；full discovery receipt 修正（2630 PASS） |

## 4. 验证结果（receipt 全部 committed）

| 命令 | 结果 | receipt |
|---|---|---|
| P8 focused（saga/runtime/evaluator/orchestrator） | **Ran 117 tests OK** | `cr010_functional/receipt_p8_focused.json` |
| C0 focused（`-B -s`，fixtures/worker/chaos/publication） | **Ran 35 tests OK** | `cr010_functional/receipt_c0_focused.json` |
| full discovery（`-B -s discover`） | **Ran 2630 tests OK（skipped=1）** | `cr010_functional/receipt_full_discovery.json` |
| R07 专项 | 6 tests OK | — |

receipts 含 command/cwd/runtime/commit+tree/log ref+sha256/exit/counts。日志：`research_state/control_plane/rollout/lineage_audits/cr010_functional/*.log`。

## 5. 仍存在的功能缺口（C0 三个硬门，HOLD 依据）

1. **[C0 阻断]** 官方 24-cycle campaign 仍主进程内驱动，未逐 step 走 worker 子进程协议（fresh-process crash/recovery 未在 campaign 执行路径上实现）；
2. **[C0 阻断]** second-root replay 未接通：**event payload 内嵌 fixture root 绝对路径**（已证明同进程不同 root digest 不等），需先消除泄漏（怀疑 LearningCommit evidence_ref / claim lineage 含绝对路径）再实现不同进程+不同 root 的 replay 一致性；
3. **[C0 中]** worker 各 step 为有界实现（共享 digest/evidence 计算），未各自执行完整 controller 转移。

## 6. 约束遵守

- 未修改 set_param/reset/rollback；未改变测试范围/cycles/seed/阈值；未停止用户进程；
- 旧 evidence/Gate/closure/receipt/ruling 零改写（git diff 证明）；用户保护文件未触碰；
- root secret 仅内存；无 Unicode emoji 源码；修复全部配套行为测试。

## 7. 交付物索引

- 功能完成度报告：`docs/superpowers/reviews/2026-08-15-v342-cr010-functional-completion-report.md`
- 复现证据：`research_automation/control_plane/cr010_r01_r07_reproduction_evidence.md`
- receipts/日志：`research_state/control_plane/rollout/lineage_audits/cr010_functional/`
- 被审报告：`docs/superpowers/reviews/2026-08-15-v342-cr010-content-completion-review.md`
