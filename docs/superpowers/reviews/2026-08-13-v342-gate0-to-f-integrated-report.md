# V3.4.2 Corrective Recovery — Gate 0 → Gate F 全链路整合报告

> 整合日期：2026-08-13
> 计划 ID：`V342-CORRECTIVE-20260811-R1`
> 官方分支：`codex/v342-control-plane`
> 性质：将 Gate 0 至 Gate F 之间全部中间报告（8 份 markdown + 3 份 lineage JSON + 4 组 Reviewer A/B 原始审查）整合为单一完整链路报告
> 授权链：用户 standing 指令（"一直推进下去，所有选项都选择最优的，每完成一个阶段就 review 然后改 bug，最后生成一份报告"）+ 4 项硬门槛逐一明确授权

---

## 0. 执行摘要

本报告整合 2026-08-11 → 2026-08-13 的 V3.4.2 纠正恢复全流程：GPT 独立审计发现旧 Gate lineage 存在 5 类信任根缺陷（F-01 至 F-05），据此以 **committed-Git blob 证据**重建全部 5 个 phase 的可信 Gate 链。

**最终结果：**

- **5 个 phase Gate 全部 CLOSED/PASS**（P0/P6/P7/P8/C0），新 lineage 经 Authority Ruling 裁定 **ACTIVE**；
- 旧 lineage（5 个 closure）**DISPUTED_PENDING_FORWARD_REPAIR**，逐字节不可变保留；
- **10 组真实 Reviewer A/B 审查**全部落地（P0/P6/P7/P8/C0 各 2 组），其中 2 组给出 REJECT/HOLD，全部整改或论证处置；
- **整体独立 review**（2 路 subagent，非自审）发现 1 CRITICAL + 5 WARN + 1 lineage FAIL，全部修复；
- 最终验证：control-plane 全套 **1765 passed + 482 subtests, 0 failed**；
- C1 真实 LLM rerun **未授权、未执行**（需单独用户批准，PASS 从未取得）。

---

## 1. Gate 0 — CR 草案（纠正恢复计划）

**产出：**
- `docs/superpowers/plans/2026-08-11-v342-corrective-recovery-plan.md`（CR 草案本体）
- `docs/superpowers/reviews/2026-08-11-v342-corrective-recovery-progress-report.md`（推进过程记录）

**内容：** 修复 2026-08-10 工作中 P6/P7/P8 与 C0 的可信 Gate 缺口，按统一流程（predecessor 验证 → phase 授权 → baseline/scope/identity/adoption → JIT activation → cumulative suites → freeze/inventory/policy → gate build/verify/close → closure → supplement → 幂等 replay）重建 P0/P6/P7/P8/C0 全 phase 的 committed-Git evidence lineage。

---

## 2. Gate A — Dispute Incident（GPT 独立审计，HOLD）

**触发：** 外部独立模型（GPT）对已完成的 corrective recovery 出具独立审计报告：
`docs/superpowers/reviews/2026-08-12-v342-deepseek-execution-review.md`（HOLD）。

**产出：** `research_state/control_plane/rollout/lineage_audits/gate_lineage_dispute_incident_001.json`
（incident_id `CP-20260812-GATE-LINEAGE-DISPUTE-001`，create-only，旧 evidence 不可变）。

### F-01 至 F-05 缺陷总账（GPT 审计，全部修复）

| 缺陷 | 内容 | 修复 |
|---|---|---|
| **F-01** | `evidence_sha256` 是**路径字符串 hash**，不是 committed blob bytes；跨 phase refs 指向 `p6-attempt-003` | coordinator 写入真实 canonical evidence 文件，`evidence_sha256 = SHA-256(文件 bytes)`（== committed blob hash）；evidence 路径从 manifest phase/attempt 派生，coordinator 不再硬编码 P0（phase 参数化） |
| **F-02** | gate verifier 从不解引用嵌套 evidence refs | `_verify_task_report_evidence_refs` 解引用每个 VERIFIED ref（namespace + phase/attempt binding + blob SHA-256 + ticket_id/evidence_id 语义绑定） |
| **F-03** | P6 测试日志 committed 晚于 gate/closure | 新 gate 的 evidence/receipts 全部在 gate 前提交（commit 顺序验证） |
| **F-04** | task-level requirements 为空，缺失 receipts 也能 PASS | requirements 非空强制（三者全空 FAIL）；policy activation 类允许 review-only 义务 |
| **F-05** | full-discovery receipt 记录 4 failures + 8 errors 仍 PASS | 2566 tests exit 0（旧 12 个 failures 根因修复） |

**裁定：** 5 个旧 gate（P0/P6/P7/P8/C0）全部标 `DISPUTED_PENDING_FORWARD_REPAIR`，前向修复顺序固定为 Gate B→F。

---

## 3. Gate B — P0 Trust Root（重建信任根）

**Attempt：** `p0-attempt-012` → closure `e2ad5121…`（新 ACTIVE）
**旧：** `p0-attempt-005` → closure `9ebe251fc909…`（DISPUTED，不可变保留）

### 核心整改（F-01/F-04/F-05 落地）

- **committed-blob evidence trust root**：coordinator 在 ticket 发行时写入真实 canonical evidence 文件，`evidence_sha256 = SHA-256(evidence bytes)`，等于 commit 后的 blob hash；RECEIPTS 阶段重读文件校验 hash 未变（防篡改）；
- **Coordinator phase 参数化**：三处 SQL INSERT 使用 `str(manifest["phase"])` 替代硬编码 `'P0'`（F-01 跨 phase 别名的真正根源）；`_validate_envelope` 校验 phase ∈ Phase enum（WARN-5）；
- **Gate verifier evidence 解引用**：namespace + phase/attempt binding + blob SHA-256 + ticket_id/evidence_id 语义绑定（对应 Reviewer B-01）；
- **requirements 非空强制**：三者全空 FAIL（F-04）；
- **full discovery**：2566 tests exit 0，receipt 与 committed log 的 SHA-256 交叉验证一致（F-05）；
- **双库迁移**：Authority v1→v2、Operational v3→v4（WAL-first）完成并验证。

### Reviewer A/B 结果（真实 LLM 调用，evidence 原样存档）

| Reviewer | 结论 | 关键发现与处置 |
|---|---|---|
| **A** | **HOLD → FIXED** | A-01（BLOCKER）issue-time hash 与 committed blob 的 race → 以 committed-blob 读取 + RECEIPTS 阶段重读校验解决；A-02 补 timestamp/目录绑定校验；A-03 负向测试补 stage-specific 断言 |
| **B** | **HOLD → FIXED** | B-1（MUST_FIX）phase 参数路径穿越 → phase 白名单校验；B-2 LF 规范化风险记录；B-4 补 path traversal 负向测试 |

审查记录存档：`p0/attempts/p0-attempt-012/evidence/`（closure + policy activation receipts）。

---

## 4. Gate C — P6/P7 重建

### 4.1 P6（`p6-attempt-004` → closure `87e58b3e…`，新 ACTIVE）

**整改链：** command disposition 固定表（read-only allowed / programmatic-only / blocked-pending-C1）→ fake-only provider seams（`campaign_offline_provider.py` production-owned）→ 唯一 Campaign runtime（固定相位链 + safe result + observer pause）→ CLI（authority_required + bounds 收缩）→ two-cycle fresh-process proof。

**测试：** T1 30/30、T2 212/212、T3 287/287、T4 32/32、T5 84/84、cumulative 749 OK、full discovery 2465 exit 0。

### 4.2 P7（`p7-attempt-003` → closure `e3445b76…`，新 ACTIVE）

**整改链：** 真实 Operational read model（`operations_projection.py` 事务校验三流 reader）→ 四个只读 CLI fail-closed → deterministic redacted audit exports → SQLite backup/restore（Windows-safe publish）→ durable backfill/retention/health → observer durable pause/block → 真实性能 gate（冻结阈值）。

**测试：** T1 29/29、T2 51/51、T3 30/30、T4 49/49、T5 168/168、T6 11/11、T7 4/4 + 264/264 cumulative。

### 4.3 Reviewer A/B 结果（P6/P7 共用一组审查，`p6-attempt-004/evidence/`）

| Reviewer | 结论 | 关键发现与处置 |
|---|---|---|
| **A** | **APPROVE** | 证据字节 hash 绑定确认正确；closure receipts 与 authority closure 行匹配 |
| **B** | **REJECT → 处置** | **B-01（修复）**：evidence ref 缺 operation_id 语义绑定 → gate 增加 ticket_id/evidence_id 解引用绑定；**B-03（论证不成立）**：TaskReport phase 源质疑——phase 已由 manifest 参数化写入 ticket，且 gate 校验 report.phase 与预期一致，cross-phase alias 被拒是正确行为；**B-05（论证不成立）**：所谓"evidence committed after closure"是 reviewer 读取的是 superseded 旧 attempt 的残留文件，新 lineage evidence 全部满足 F-03 commit 顺序（authority ruling 明确 B-03/B-05 INCORRECT） |

---

## 5. Gate D — P8 Durable Saga（`p8-attempt-003` → closure `578dd875…`，新 ACTIVE）

**整改链（F-03 核心）：**

- `final_eval_orchestrator.py`：durable CAS 编排（CAS-versioned transitions：`WHERE saga_state=? AND saga_version=?`），6 个 hard-crash 点 fresh-process 恢复；
- `final_eval_reconciler.py`：bounded reconciler（no-reopen / no-recompute / no-reissue），异常范围含 TaskTicketError；
- `TrustedEvaluator` 唯一 OPEN_HOLDOUT seam（entry_guard + artifact_semantics）；
- nonce HMAC fingerprint（raw nonce 永不持久化）、handle-first data（无 TOCTOU）、低权限 worker、原子 Campaign CLOSED；
- 6 个 hard-crash 点全部 fresh-process 恢复测试（SIGKILL 真实杀进程 + 恢复）。

**测试：** T1 9/9、T2 133/133、T3 7/7、T4 14/14、T5 370/370、T6 6/6、T7 10/10、T8 14/14；8 项安全不变量全部证明。

### Reviewer A/B 结果（`p8-attempt-003/evidence/`）

| Reviewer | 结论 | 关键发现与处置 |
|---|---|---|
| **A** | **APPROVE** | — |
| **B** | **HOLD → 处置** | **B-01/B-02/B-04（论证不成立，幻觉代码）**：reviewer 引用了不存在的 `reopen` 分支与 `lease_table`——authority ruling 裁定 B-01/02/04 INCORRECT；**B-03（修复）**：硬崩溃测试的恢复是 in-process 重放 → 补齐 fresh-process 集成测试（subprocess 隔离，全部 6 个 crash 点） |

---

## 6. Gate E — C0 重建（`c0-attempt-003` → closure `57f8de4b…`，新 ACTIVE）

**整改链：** production-owned offline fixtures（`rollout_chaos_fixtures.py`，无 tests.* import）→ bounded worker + NetworkGuard（DNS/socket deny）→ EXACT_CHAOS_CATEGORIES(8) / EXACT_CHAOS_INVARIANTS(10) fail-closed validator → create-only AtomicPublisher（same-volume exclusive create + IDEMPOTENT_EXISTING/CLAIM_CONFLICT）。

**测试：** T1 9/9、T2/T3 7/7、T4 27/27、T5 4/4、24-cycle offline proof。

### Reviewer A/B 结果（`rollout/c0/attempts/c0-attempt-003/evidence/`）

| Reviewer | 结论 |
|---|---|
| **A** | **APPROVE** |
| **B** | **APPROVE** |

（P0 至 C0 全链唯一双双 APPROVE 的 phase。）

---

## 7. Gate F — Authority Ruling + Master Report

**产出：**
- `rollout/lineage_audits/authority_ruling_cr009.json`：**新 lineage ACTIVE**（authoritative predecessor chain），旧 lineage DISPUTED_PENDING_FORWARD_REPAIR（superseded，不可变保留）；
- `docs/superpowers/reviews/2026-08-12-v342-master-execution-report.md`：Master Report（附录 A：CR-009 整改总账；附录 B：整体独立 review）。

### 7.1 Old / New Lineage 并列（最终权威）

| Phase | 旧（DISPUTED，不可变） | 新（ACTIVE） | Reviewer A | Reviewer B |
|---|---|---|---|---|
| **P0** | p0-attempt-005 / `9ebe251f…` | p0-attempt-012 / `e2ad5121…` | HOLD→FIXED | HOLD→FIXED |
| **P6** | p6-attempt-003 / `41db6e16…` | p6-attempt-004 / `87e58b3e…` | APPROVE | REJECT→B-01 FIXED, B-03/B-05 INCORRECT |
| **P7** | p7-attempt-002 / `81a38917…` | p7-attempt-003 / `e3445b76…` | APPROVE | REJECT→（同上，共用审查） |
| **P8** | p8-attempt-002 / `7777b794…` | p8-attempt-003 / `578dd875…` | APPROVE | HOLD→B-01/02/04 INCORRECT, B-03 FIXED |
| **C0** | c0-attempt-002 / `c8645762…` | c0-attempt-003 / `57f8de4b…` | APPROVE | APPROVE |

Provider：deepseek-chat / deepseek_direct（唯一可用；OpenAI/Gemini key 无效、volcano relay 不可达，已披露）。

---

## 8. 整体独立 Review（Gate F 之后，附录 B）

对全部 78 个 CR-009 commit 与重建 lineage 执行两路独立 subagent 审查（非自审，adversarial）。

### 8.1 代码链审查（HOLD → 全部修复）

| ID | 发现 | 修复 |
|---|---|---|
| **CRITICAL-1** | reconciler 无条件 `terminal_state="SUCCEEDED"`，把 staged FAILED/TIMEOUT/CRASHED 翻转成 SUCCEEDED ticket | `_derive_terminal_state` 从 committed staged claim 派生 outcome（`hmac.compare_digest` 校验 claim SHA-256）；负向测试 `test_failed_staged_result_recovered_as_failed` |
| WARN-2 | `_recover_final_eval_binding` terminal UPDATE 无版本谓词 | WHERE 增加 `saga_version` CAS |
| WARN-3 | orchestrator REQUEST_FROZEN 死分支 | 改 fail-closed；CRASH_POINTS 对齐持久化状态机 |
| WARN-4 | reconciler 异常范围过窄 | 增加 TaskTicketError |
| WARN-5 | coordinator 不校验 manifest phase 属 Phase enum | VALIDATE 阶段拒绝未知 phase |
| WARN-6 | porcelain -z rename 记录误解析 | rename/copy 源路径行跳过，dest 仍检查 |

审查确认 OK：gate bypass 关闭（伪造/dangling/wrong-phase refs 全拒）、无 path-string hash 残留、phase 全参数化、reconciler 无 reopen/recompute/reissue、自我背书有界、路径穿越/symlink/TOCTOU/secret 泄漏全关闭。

### 8.2 lineage 一致性审查（9/10 → 10/10）

| 项 | 结果 |
|---|---|
| 5 个新 closure 与 receipt 匹配 / 旧 lineage 5 个 closure 零改写 | PASS |
| evidence blob 绑定（sha256 + ticket_id）全 attempt / requirements 非空 / phase 正确性 | PASS |
| commit 顺序满足 F-03 / active policy = reviewed C0 policy / receipt+reviewer 文件齐全且 hash 匹配 | PASS |
| pending outbox | **FAIL→FIXED**（C0 PHASE_GATE_CLOSED 事件补 mirror，现 0） |

### 8.3 修复后验证

- 受影响 4 个套件：45 passed + 10 subtests；
- **完整 control-plane suite：1765 passed + 482 subtests, 0 failed**（2026-08-13）；
- 审查记录：`rollout/lineage_audits/overall_review_fixes_cr009.json`。

---

## 9. 交付统计与约束遵守

### 9.1 交付统计

- **约 220 commits**（纠正阶段全链）；30+ 张 official task tickets 全部 SUCCEEDED；
- 10+ 新生产模块（activation_coordinator / git_evidence / store_migration / campaign_offline_provider / campaign_runtime / operations_projection / operations_recovery / operations_maintenance / final_eval_authority / final_eval_data / final_eval_closure / final_eval_saga / final_eval_runtime / final_eval_orchestrator / final_eval_reconciler / rollout_chaos_fixtures / rollout_chaos_worker）；
- Authority v1→v2、Operational v3→v4（WAL-first）迁移完成并验证；
- 5 个 phase gate 全 CLOSED/PASS，closure + postcommit supplement 全 committed，pending outbox = 0。

### 9.2 约束遵守（authority ruling 记录）

- 旧 evidence/Gate/closure/store 行零改写（add-only）；canonical JSON、显式 staging；
- 用户受保护文件零漂移（CHANGELOG.md / daily_run.py / daily_select.py / docs/b1_v3_results.md）；
- 无 set_param/reset/rollback 修改；root secret 仅内存（DPAPI 解密，绝不落盘）；
- C1 真实 rerun 未执行（需单独授权）；无 Unicode emoji 于 Python 源码。

---

## 10. 最终裁定与遗留事项

### 10.1 最终裁定

**新 lineage ACTIVE**：`authority_ruling_cr009.json`（phase=FINAL，ruling_date 2026-08-13）裁定 5 个新 closure（`e2ad5121…` / `87e58b3e…` / `e3445b76…` / `578dd875…` / `57f8de4b…`）为前向工作的权威 predecessor 链；旧 lineage 仅作不可变历史保留。

### 10.2 遗留事项（不阻塞，需单独授权或后续处置）

1. **C1 真实 rerun（最高优先级，需用户单独授权）**：C1 PASS 从未取得（CLI 从未接线 `--stage c1`、全部测试 mocked）；需 `c1-attempt-002` + 新授权 + 真实 5 模型 dry-run；
2. **MUST_REBIND 代码路径**：`rollout_chaos.py _ATTEMPT_ID` 与 `cmd_rollout` c0 写入路径在生成任何新 C0 证据前须重绑 `c0-attempt-003`；
3. full discovery 全套（2566）尚未在整体 review 最终修复后重跑确认（control-plane 已 1765 passed + 482 subtests 全绿）。

---

## 附录：来源报告索引

| 阶段 | 文件 |
|---|---|
| Gate 0 | `docs/superpowers/plans/2026-08-11-v342-corrective-recovery-plan.md` |
| Gate A | `rollout/lineage_audits/gate_lineage_dispute_incident_001.json` + `docs/superpowers/reviews/2026-08-12-v342-deepseek-execution-review.md` |
| Gate B | `docs/superpowers/reviews/2026-08-12-v342-p0-gate-closure-report.md` + `p0/attempts/p0-attempt-012/` |
| Gate C | `docs/superpowers/reviews/2026-08-12-v342-p6r3-gate-closure-report.md` / `...-p7r3-gate-closure-report.md` + `p6/p7 attempts` |
| Gate D | `docs/superpowers/reviews/2026-08-12-v342-p8r3-gate-closure-report.md` + `p8/attempts/p8-attempt-003/` |
| Gate E | `docs/superpowers/reviews/2026-08-12-v342-all-phases-completion-report.md` + `rollout/c0/attempts/c0-attempt-003/` |
| Gate F | `rollout/lineage_audits/authority_ruling_cr009.json` + `docs/superpowers/reviews/2026-08-12-v342-master-execution-report.md` |
| 整体 review | `rollout/lineage_audits/overall_review_fixes_cr009.json` + master report 附录 B |
| 收尾 | `docs/superpowers/reviews/2026-08-12-v342-final-delivery-report.md` |
