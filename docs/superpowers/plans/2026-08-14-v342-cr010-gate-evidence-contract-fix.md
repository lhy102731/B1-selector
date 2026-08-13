---
# V3.4.2 Corrective Recovery — CR-010：Gate/Closure 因果链与证据合同整改

> **状态：NON_AUTHORITATIVE_PREPARATION（草案，未获批准，不构成任何执行授权）**
> 起草日期：2026-08-14
> 前置：独立复核 `docs/superpowers/reviews/2026-08-13-v342-gate0-to-f-integrated-review.md`（HOLD / F-01 至 F-08）
> 分支：`codex/v342-control-plane`（当前 detached HEAD `b30b433`）
> 前置核验：本会话逐项只读核验（源码 + git 历史 + JSON 时间戳 + 调用 manifest），状态见 §5

---

## 1. 目的

独立复核（F-01 至 F-08）指出：CR-009 重建的 Gate lineage 在**可机械验证的证据合同**上存在缺陷——closure 时间戳时区基准不一致导致因果链无法机器证明、P8 的 6 个 hard-crash 声明与实际触发点不符、C0 的 24-cycle official proof 不存在、Reviewer A/B 未绑定 candidate 且 provider 未区分、full discovery receipt 未绑定最终 HEAD、授权 provenance 无机械验证、OPEN_HOLDOUT 入口声明为 LEGACY_UNAUDITED、工作树含 untracked 冲突 Gate。

本 CR 只修复**证据合同机制与重新 gate**，不改写任何旧 Gate/closure/ruling/lineage 字节。

## 2. 不可变边界（add-only）

- 旧 P0/P6/P7/P8/C0 Gate、closure、TaskReport、evidence、Authority/Operational 行、`authority_ruling_cr009.json`、`overall_review_fixes_cr009.json` **全部保持原样**；
- 本 CR 产生的所有工件以新 commit 追加；不覆盖、不删除、不原地修补；
- 用户受保护文件（CHANGELOG.md / daily_run.py / daily_select.py / docs/b1_v3_results.md）不触碰；
- 不删除任何 dirty/untracked 文件（仅 inventory/quarantine）；
- 不执行：真实 Campaign、C1 rerun、promotion、`set_param`/reset/rollback、live store migration；
- 唯一允许的 LLM 调用：批准后的 Reviewer A/B 独立审查（provider/model 见 §3.9）。

## 3. 技术变更范围（草案，批准后实施）

### 3.1 [F-01] 时间源统一与 closure 因果链（重新 gate 前必改）
- gate/closure 全部使用 **UTC**（`datetime.now(timezone.utc)`），拒绝本地时区；
- closure `closed_at` 必须机械可证 `> gate.created_at`（verifier 断言）；
- 重新生成 5 个 phase 的 gate/closure（旧版本保留）。

### 3.2 [F-01] freeze 覆盖最终 HEAD
- 所有源码修复 commit 后重新 freeze；freeze manifest pin 的 `git_commit` 必须是该 phase gate 时的 HEAD；
- verifier 增加"freeze pin 之后无未覆盖源码修改"检查（新增 freeze-gate 完整性断言）。

### 3.3 [F-02] P8 hard-crash 证明补全
- orchestrator：为 `AUTHORIZED`、`CLOSED`、`AUTHORITY_TERMINAL` 转移补 crash hook（或从 CRASH_POINTS 移除无触发点的名字——实现选择补全，保持 6 点承诺）；
- 测试矩阵：删除不存在的 `CRASH_AFTER.REQUEST_FROZEN` 行、补 `AUTHORITY_TERMINAL` 行；每个 child 断言 `returncode` + "下一状态未发生"；
- `sink()` 必须真实创建并提交 result blob（断言 same-volume object + fixed claim + Authority CAS 绑定）；
- 新增 fresh-process reconciler 集成测试：child 崩溃 → **新进程**取得 recovery lease → 调 reconciler → 断言 outcome 不翻转。

### 3.4 [F-03] C0 证据链重建
- `_ATTEMPT_ID` 从 CLI/授权注入（`c0-attempt-003`），移除 official path 的 `lru_cache`；
- `cmd_rollout` 改走 worker 协议 + create-only AtomicPublisher，禁止 `write_text` 覆盖；
- NetworkGuard 实现真实 DNS/socket/subprocess 拦截（而非仅清 env + localhost 探测）；
- CLI registry 将 rollout 改为 `authority_required=True`；
- 生成 official 24-cycle report + fixed claim + second-root replay + no-side-effect receipt + official-run ticket（全部 committed，绑定 attempt-003）。

### 3.5 [F-05] test receipt 完整合同
- `task_reports.py` 的 `_TEST_RECEIPT_FIELDS` 扩展：executable、完整命令、cwd、运行时版本、lock hash、candidate commit/tree、UTC 起止时间、stdout/stderr ref + hash；
- 对最终修复后 HEAD 重跑 full discovery，生成完整合同 receipt；
- 1765+482（或新数字）必须有 committed receipt 与日志 hash 绑定。

### 3.6 [F-06] 授权 provenance 机械验证
- 新增 approval record verifier：消费用户批准的 source/tree/envelope/manifest hash；
- `ActivationCoordinator.run()` 增加 approval record 参数，Authority row / TaskReport / Gate 绑定 approval hash；
- 无法取得原始批准时明确标 `AUTHORITY_PROVENANCE_UNVERIFIED`（不自行推导 ACTIVE）；
- 本次整改的批准：以用户对 CR-010 的明确确认 + 本草案 commit hash 作为 approval record。

### 3.7 [F-07] OPEN_HOLDOUT 入口升级
- P8/C0 policy 中 `TrustedEvaluator.evaluate_v2` 条目从 `LEGACY_UNAUDITED` 升级为 reviewed，补 `declared_phase`；
- P0 激活 policy（f871b45b…）补 OPEN_HOLDOUT 条目（如果 P0 gate 需要引用）；
- 验证 `final_eval_runtime.py` 接入 durable orchestrator/reconciler（或明确记录"运行时内存状态机 + durable 编排分离"的设计边界并测试证明）。

### 3.8 [F-08] 工作树快照治理
- 对 control-plane 区所有 untracked 文件做 inventory + quarantine 清单（add-only，不删除）；
- 明确 Gate verifier 读取的唯一 committed gate 文件路径（`*_final.json` 约定）；
- 建立明确 branch/tag 指向候选 HEAD（当前 detached，需命名，如 `v342-cr010-candidate`）。

### 3.9 [F-04] Reviewer A/B 重新审查（candidate 绑定 + provider 区分）
- Reviewer A = deepseek-chat/deepseek-v4-flash；**Reviewer B = glm-5.2 或 doubao-seed-2.0-pro**（实测 OK）；
- 每份 prompt 必须引用候选 HEAD commit/tree hash；
- calls manifest 增加 provider、invocation ID、完整 UTC 时间戳；
- gate schema 增加 `review_receipts` 字段并强制非空；
- reviewer prompt 只允许审查 committed 代码（禁止引用不存在的文件/字段）。

### 3.10 重新 gate 顺序（修复完成后）
1. P0 → P6 → P7 → P8 → C0（沿用 JIT activation + freeze → gate build → fresh verify → close → closure → supplement → 幂等 replay）；
2. 每 phase 双 Reviewer（A/B provider 不同）→ 新 authority ruling（v2，取代 v1 前先 add-only 标注）。

## 4. 授权要求（Gate 0 硬门）

实施前需用户批准：
1. 本 CR 的精确范围与不可变边界（§2）；
2. Reviewer B provider 选择（glm-5.2 vs doubao-seed-2.0-pro）与调用披露；
3. 重新生成 5 个 gate/closure（旧版本保留）与重新 authority ruling；
4. 新增 approval record verifier（F-06）——若无法机械验证历史授权，历史 attempt 标 `AUTHORITY_PROVENANCE_UNVERIFIED`。

## 5. 核验结论摘要（2026-08-14 只读核验，全部 confirmed/disputed 见下）

| Finding | 状态 | 核心证据 |
|---|---|---|
| F-01 | CONFIRMED | closure `+08:00` vs gate `Z`，UTC 换算全部倒置；0069356/810bf73 晚于 closure/freeze pin |
| F-02 | CONFIRMED | CRASH_POINTS 6 名但 hook 仅 3 触发点；测试矩阵含不存在 REQUEST_FROZEN |
| F-03 | CONFIRMED | `_ATTEMPT_ID="c0-attempt-001"` 硬编码；run_research 写 attempt-001；rollout authority_required=False；NetworkGuard 无真实拦截 |
| F-04 | CONFIRMED（修正：8 次调用非 10 组；P0 reviewer 在旧 attempt-005） | 全部 `deepseek-v4-flash` 无 provider/ID；P8 B-04 引用代码不存在 |
| F-05 | CONFIRMED | schema 4 字段；2566 绑定旧 head ad01bdc；1765+482 无 committed receipt |
| F-06 | CONFIRMED | run() 不消费 approval record；scope receipt 自标 NON_AUTHORITATIVE |
| F-07 | PARTIAL-CONFIRMED（P0 激活 policy 无 OPEN_HOLDOUT，P8/C0 有） | 唯一声明是 LEGACY_UNAUDITED；runtime 内存状态机 evidence_ref=None |
| F-08 | CONFIRMED | detached HEAD；分支 ref 停 07313d2；P0 有 untracked FAIL gate + tracked PASS gate |

## 6. 验收标准（对齐复核 HOLD 解除标准）

- [ ] 全部 5 个新 gate/closure：closure `closed_at` UTC 且机械可证 > gate `created_at`；
- [ ] freeze pin 覆盖 gate 时 HEAD，verifier 无"freeze 后源码修改"告警；
- [ ] P8：≥6 个真实 crash 触发点 + fresh-process reconciler 测试（新进程 recovery lease）+ sink 真实提交 claim；
- [ ] C0：official 24-cycle report/claim/replay/no-side-effect/ticket 全 committed，attempt=c0-attempt-003，CLI authority_required=True；
- [ ] full discovery 完整合同 receipt 绑定最终 HEAD（executable/命令/cwd/版本/lock/commit/UTC/log hash）；
- [ ] approval record verifier 存在且有测试（有/无/篡改三分支）；
- [ ] OPEN_HOLDOUT policy 条目 reviewed + phase 非空；runtime 接线说明记录；
- [ ] 全部 untracked control-plane 文件 quarantined（inventory 清单 committed）；
- [ ] 5 phase 双 Reviewer（A/B provider 不同），prompt 绑定 candidate hash，gate `review_receipts` 非空；
- [ ] 新 authority ruling v2 add-only 签发；旧 ruling 未改写；
- [ ] 旧 evidence/Gate/closure/store 行零改写（diff 证明）。
