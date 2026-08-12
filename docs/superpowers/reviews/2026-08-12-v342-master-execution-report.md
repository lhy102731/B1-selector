# V3.4.2 Corrective Recovery — 综合执行总报告（Master Report）

> 整合日期：2026-08-12
> 计划 ID：`V342-CORRECTIVE-20260811-R1`
> 官方分支：`codex/v342-control-plane`
> 覆盖范围：Task 4–25（P0/P6/P7/P8/C0 全 phase re-gate + C1 lineage 审计 + P2 收尾）
> 源报告：6 份 phase 报告（P0/P6R3/P7R3/P8R3 gate closure + all-phases completion + final delivery）

---

## 0. 执行摘要

本计划修复 2026-08-10 工作中 P6/P7/P8 与后续 C0 的可信 Gate 缺口，用 committed-Git 证据重新建立可信 Gate lineage。最终成果：

- **5 个 phase Gate 全部 CLOSED/PASS**（P0/P6/P7/P8/C0），closure + postcommit supplement 全部 committed，pending outbox = 0；
- **30+ 张 official task tickets 全部 SUCCEEDED**，TaskReports 绑定验证通过；
- **约 220 commits**、10+ 个新生产模块、Authority v2 / Operational v4 双库迁移完成；
- **C1 按计划仅做只读 lineage 审计**（不执行真实 LLM rerun，PASS 从未取得，待单独授权）；
- 用户受保护文件零漂移、quarantine manifest 4163 项保持一致。

---

## 1. 执行方式与授权

### 1.1 授权链
- 用户 standing 指令："改为自动批准进行下一步" + "一直工作下去直到结束吧" → 修正为"除非重大决策问题不然就一直推进下去"，并记录于 `standing_directive_execution_basis_v2.json`；
- **硬门槛**（不因 standing 指令自动放行，均已获明确授权）：
  1. `AUTHORIZE_STORE_MIGRATION id=P0-CR-008 targets=authority,operational`（用户选择推荐项）；
  2. 外部独立模型审阅（用户授权，记录于 `hard_gate_authorization_receipt.json`）；
  3. P0 authority 状态受控 SQL 修复（用户 AskUserQuestion 明确批准）；
  4. 真实 C1 LLM rerun —— **未授权、未执行**（按计划列为待单独批准事项）。

### 1.2 全程未触碰
真实 Campaign/LLM/data/KBase/Holdout、A 股数据/scheduler/ACL/promotion、`set_param`/reset/rollback、用户运行中的长任务、四个受保护用户文件（CHANGELOG.md / daily_run.py / daily_select.py / docs/b1_v3_results.md，Task 0 hash 零漂移）。

---

## 2. 各 Phase 执行结果

### 2.1 P0（p0-attempt-005）— closure `9ebe251fc909…` ✅

| 交付 | 结果 |
|---|---|
| Git committed evidence | `git_evidence.py` committed-blob reader（12 测试）+ Gate build/verify/close 全链路 committed |
| Authority v2 | `final_eval_authorizations_v1` 表 + 全局唯一约束（plan+holdout×2、nonce fingerprint UNIQUE、result_claim UNIQUE） |
| Operational v4 | WAL-first 迁移 + `ops_access_event_integrity` sealed chain + 4 张 derived 表 |
| Activation coordinator | 单进程 JIT ticket/lease，phase VALIDATE→…→OUTBOX，v1 bootstrap / v2 normal / migration 三模式 |
| Verification runtime | `requirements/verification-runtime.lock`（httpx 0.25.2 + ag2 0.13.3 via V342-DEP-001 Option A），venv 内 1593 control-plane tests exit 0 |
| Live migration | authority v1→v2 + operational v3→v4 双库完成并验证（backup receipts 齐全） |
| Full discovery | 2418 tests，12 pre-existing failures（base commit 复现证明，out of P0 scope） |
| 受控 SQL 修复 | 16 ACTIVE grants / 16 tickets 归档为 rerecord-archive（用户授权），保留 rerecord-008 权威 2 tickets + 1 grant |
| Gate 链 | build PASS → fresh verify PASS → close PASS → drain → closure → supplement → replay PASS |

### 2.2 P6（p6-attempt-003）— closure `41db6e1670e4…` ✅

| Task | 交付 | 测试 |
|---|---|---|
| T1 disposition | command disposition 固定表（read-only allowed / programmatic-only / blocked-pending-C1）+ routing 单元隔离 + export 修复 | 30/30 |
| T2 provider seams | `campaign_offline_provider.py`（production-owned fake）+ AG2/OpenAI-compatible/CLI 三 seam | 212/212 |
| T3 runtime | `campaign_runtime.py`（固定相位链 + safe result + observer pause） | 287/287 |
| T4 CLI | `campaign` 命令注册（authority_required）+ bounds 收缩 | 32/32 |
| T5 two-cycle | `tests/helpers/control_plane_campaign_runtime_child.py` fresh-process proof | 84/84 |
| Gate | 6 succeeded tickets + 264/264 cumulative + full discovery 2465 exit 0 | PASS |

### 2.3 P7（p7-attempt-002）— closure `81a38917a03d…` ✅

| Task | 交付 | 测试 |
|---|---|---|
| T1 projection | `operations_projection.py` 事务校验三流 reader + durable projection writer + UNAVAILABLE/UNWIRED publication | 29/29 |
| T2 CLI | audit/doctor/export allow_real（logical hash，不 hash live WAL）+ exit-code 契约 | 51/51 |
| T3 audit exports | deterministic redacted manifests（secret/holdout 白名单） | 30/30 |
| T4 backup/restore | `operations_recovery.py` SQLite backup API 一致快照 + staging + Windows-safe publish | 49/49 |
| T5 backfill/retention | `operations_maintenance.py` batch idempotency + fresh-process resume + retention metadata | 168/168 |
| T6 observer | `P7CampaignRuntimeObserver` durable pause/block | 11/11 |
| T7 performance | 真实 append/status/backup/CLI 计时 vs 冻结阈值（25ms p95 / 1.5s cold / 0.5s warm / 30s backup / 1.5s CLI） | 4/4 + 264/264 |
| Gate | 8 succeeded tickets + 静态边界（compileall/diff/import scan 全 clean） | PASS |

### 2.4 P8（p8-attempt-002）— closure `7777b79499ea…` ✅

| Task | 交付 | 测试 |
|---|---|---|
| T1 Authority binding | `final_eval_authority.py`：FinalEvalRequestV2（无 raw nonce、research-plan 身份独立）+ nonce HMAC fingerprint + sealed begin CAS | 9/9 |
| T2 remove caller outcome | `TrustedEvaluator.evaluate_v2`（outcome 由 broker 派生；V1 historical-only） | 133/133 |
| T3 handle-first data | `final_eval_data.py`：VerifiedRootHandle（volume serial + file identity）+ HandleFirstOpener（单一 handle 校验，无 TOCTOU） | 7/7 |
| T4 low-priv worker | `final_eval_worker.py` strict-JSON stdin 协议 + 拒绝未知字段/NaN/越界 | 14/14 |
| T5 Campaign CLOSED | `CampaignStatus.CLOSED` + lease-bound 单事务 terminal audit | 370/370 |
| T6 CLOSED guards | controller 全入口 CLOSED 前置 guard | 6/6 |
| T7 durable saga | `final_eval_saga.py` REQUEST_FROZEN→AUTHORITY_TERMINAL 固定链 + outcome 派生 | 10/10 |
| T8 trusted runtime | `final_eval_runtime.py` factory（仅内存 capability + opaque root + launcher + sink） | 14/14 |
| Gate | 9 succeeded tickets + 8 项安全不变量证明 | PASS |

### 2.5 C0（c0-attempt-002）— closure `c86457621f66…` ✅

| Task | 交付 | 测试 |
|---|---|---|
| T1 offline fixtures | `rollout_chaos_fixtures.py` production-owned（clock/PID/provider/member/scope，无 tests.* import） | 9/9 |
| T2/T3 worker + network | `rollout_chaos_worker.py` bounded single-step + NetworkGuard（DNS/socket deny、proxy 清理） | 7/7 |
| T4 exact sets | EXACT_CHAOS_CATEGORIES(8) / EXACT_CHAOS_INVARIANTS(10) fail-closed validator | 27/27 |
| T5 publication | create-only AtomicPublisher（same-volume exclusive create + fixed claim + IDEMPOTENT_EXISTING/CLAIM_CONFLICT） | 4/4 |
| Gate | 6 succeeded tickets + 24-cycle offline proof | PASS |

### 2.6 C1（lineage-only audit，非 rollout）— ✅ 审计完成

- 旧 P8R2/C0R1 refs 全部分类（历史引用保留；2 处 MUST_REBIND 代码路径）；
- `c1-attempt-001` 标 HISTORICAL_IMMUTABLE_DISPUTED（无 gate/closure/official report，identities 冻结）；
- C1 source adoption：10 commits（43ebb48..aceaec87）全部 REUSE_AFTER_REVALIDATION；
- **rerun obligation = YES（强制）**：C1 PASS 从未取得，需 `c1-attempt-002` + 新授权 + 真实 5 模型 dry-run（未调用模型，待用户单独批准）；
- Audit artifact `lineage_audits/c0-attempt-001-supersession-001.json` 已提交。

---

## 3. Completion Matrix（最终权威）

| Phase | Attempt | Succeeded Tickets | Gate Ref | Closure ID | Verdict | Outbox |
|---|---|---|---|---|---|---|
| **P0** | p0-attempt-005 | 3 | `gates/official_p0_gate_v342_cr008_final.json` | `9ebe251fc909…` | **PASS** | 0 |
| **P6** | p6-attempt-003 | 6 | `gates/official_p6_gate_v342_p6r3.json` | `41db6e1670e4…` | **PASS** | 0 |
| **P7** | p7-attempt-002 | 8 | `gates/official_p7_gate_v342_p7r3.json` | `81a38917a03d…` | **PASS** | 0 |
| **P8** | p8-attempt-002 | 9 | `gates/official_p8_gate_v342_p8r3.json` | `7777b79499ea…` | **PASS** | 0 |
| **C0** | c0-attempt-002 | 6 | `gates/official_c0_gate_v342_c0r2.json` | `c86457621f66…` | **PASS** | 0 |

每 phase：implementation ✓、focused/full tests ✓、reviews（standing directive 记录）、Gate ✓、closure ✓、postcommit supplement ✓、幂等 replay ✓。

---

## 4. 对计划的修改与偏差（已逐项记录）

1. **P0**：authority 状态受控 SQL 修复（16 grants/16 tickets 归档）；policy 激活 attempt 归属修复（v16 policy + 第 3 张 TaskReport）；gate baseline 绑定改为 payload sha256 语义；Gate 输入链 v9→v16 迭代（20+ 契约修复）。
2. **P6**：phase grant 模式（operator grant_*）与 P0 coordinator-grant 不同；`cmd_export` 修复；`_campaign_boundary` 单元隔离。
3. **P7**：backfill 持久化复用 v4 表（batch key 作 backfill_id）；`rollout_chaos.py` tests.* 惰性化（Step 9.4 import scan）；activation 脚本 task report 文件名修复（T1 report 被覆盖后恢复）。
4. **P8**：V1 evaluate 保留为 historical、production 走 evaluate_v2；closure writer 用 isinstance 区分 recovery lease。
5. **C0**：fixtures/worker/publication 按计划新建；exact invariant 10 项 / category 8 项冻结。

---

## 5. 交付统计

- **Commits**：约 220（纠正阶段全链，P0 迁移后累计）；
- **Tickets**：30+ 张 official task tickets 全部 SUCCEEDED（TaskReports 绑定验证通过）；
- **新生产模块**（10+）：`activation_coordinator.py`、`git_evidence.py`、`store_migration.py`、`campaign_offline_provider.py`、`campaign_runtime.py`、`operations_projection.py`、`operations_recovery.py`、`operations_maintenance.py`、`final_eval_authority.py`、`final_eval_data.py`、`final_eval_closure.py`、`final_eval_saga.py`、`final_eval_runtime.py`、`rollout_chaos_fixtures.py`、`rollout_chaos_worker.py`；
- **迁移**：Authority v1→v2、Operational v3→v4（WAL-first）完成并验证；
- **测试**：full discovery 2465+ tests exit 0（P6 后各阶段持续验证）；focused suites 各 phase 全绿。

---

## 6. Remaining Risks / Recommendations

1. **C1 真实 rerun（最高优先级，需用户单独授权）**：C1 PASS 从未取得；需 `c1-attempt-002` + 新授权 + 真实 5 模型 LLM dry-run 后方可声明 C1 完成；
2. **MUST_REBIND 代码路径**：`rollout_chaos.py _ATTEMPT_ID` 与 `cmd_rollout` c0 写入路径在生成任何新 C0 证据前须重绑 `c0-attempt-002`；
3. **Reviewer A/B 外部模型审阅**：按 standing directive 记录（invocation-level 独立性，provider 切换在 C1）；
4. Step 4.13 full discovery exit 0 未达成（12 个 pre-existing failures，base commit 复现证明，out of P0 scope）；
5. Step 4.11 缺 3 模块独立 receipt 记录（已在 cumulative suite 覆盖）。

---

## 7. 交付状态

- 本报告为 corrective recovery 综合执行总报告；已获批准前不 push、不建 PR、不 merge 到 main/production、不 deploy。
- 建议下一会话：C1 真实 rerun（需用户单独授权真实 LLM 调用）。
