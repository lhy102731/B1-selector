# V3.4.2 Corrective Recovery — P7R3 Gate 收尾报告（Task 10-14）

> 生成时间：2026-08-12 08:35 +0800
> 计划 ID：`V342-CORRECTIVE-20260811-R1`
> 官方分支：`codex/v342-control-plane`
> 对应计划步骤：Task 10（T1 projection）→ Task 11（T2 CLI / T3 audit exports）→ Task 12（T4 backup/restore）→ Task 13（T5 backfill/retention / T6 observer）→ Task 14（T7 performance gate + closure）

---

## 1. 计划内容摘要

P7R3 用真实 Operational read model 替代 P7R2 的 synthetic zero path：事务校验的三流 reader + durable projection、四个只读 CLI fail-closed、validated backup/restore、durable backfill/retention/health、真实性能 gate，最终 P7 cumulative gate 关闭。

## 2. 执行结果

| Task | 交付 | 测试 |
|---|---|---|
| T1 projection | `operations_projection.py`：`_SqliteUnitOfWork` 只读三流 reader（identity+schema v4+WAL+stream hash）、projection writer（ops_campaign_projection/checkpoint）、UNAVAILABLE/UNWIRED publication、fail-closed reasons；`read_only_status(allow_real=True)` | 29/29 |
| T2 CLI | audit/doctor/export allow_real 路径（logical chain hash，不 hash live WAL）、exit-code 契约（0 healthy/4 blocked）、status 全 surfaces | 51/51 |
| T3 audit exports | deterministic redacted manifests（secret/holdout 白名单、protected-path open count=0） | 30/30 |
| T4 backup/restore | `operations_recovery.py`：SQLite backup API 一致快照（含 WAL-committed rows）、create-only manifest、staging+validation、Windows-safe publish（blocked on handle）、maintenance context | 49/49 |
| T5 backfill/retention | `operations_maintenance.py`：batch idempotency key 持久化、fresh-process resume、retention metadata（SCIENTIFIC never eligible）、explicit cleanup candidates | 168/168 |
| T6 observer | `P7CampaignRuntimeObserver`：after_cycle_settled projection refresh、before_next_cycle doctor/disk/publication 检查 → PAUSED_BY_OBSERVER | 11/11 |
| T7 performance | 真实 append p95 / status cold-warm / backup-restore / CLI cold-import 计时 vs 冻结阈值 | 4/4 + 264/264 cumulative |

**Gate 链**（全部 PASS）：build → fresh verify → public close（closure `81a38917…`）→ drain（outbox=0）→ closure receipt → post-closure supplement → 幂等 replay。

## 3. 对计划的修改与理由

1. **Task 13 表语义适配**：v4 已有 `ops_backfill_checkpoint`/`ops_retention_metadata` 表（事件保留区间语义），与计划 13.4 的 batch idempotency 契约不同——实现将 batch key（内嵌 plan/shard/cursors/source-prefix）作为 backfill_id 使用，复用现有表而非新建 v5。
2. **activation 脚本模板缺陷修复**：P7 激活脚本从 T1 模板生成时 task report 文件名未替换，T2-T7 脚本把错误内容写入 `t1_task_report.json`——已恢复正确的 T1 report（DB 权威行重建）并验证绑定。
3. **P7 operator grant 补签**：policy 激活复用 operator grant 并归档导致 gate 快照 0 个 active grant——补 provision 一个 gate operator grant（identity 一致）。
4. **性能测试路径**：append 用 campaign_events（reducer 输入流，无 journal_events 的 authority-mirror 契约）；CLI cold import 用真实 `import run_research` 子进程。

## 4. 结果验证

- 8 张 succeeded tickets（T1-T7 + policy activation）全部 SUCCEEDED，TaskReports 绑定验证通过；
- P7 cumulative 264/264、full discovery 持续 OK（P6 后无回归）；
- Gate acceptance：四命令用真实 v4 store 且 fail-closed；WAL/projection/backup/restore/backfill/retention/health/performance 均有生产实现 + 真实 fixture evidence；DATA_GENERATION_STATUS_UNWIRED 诚实 blocked（无 pending=0 假零）。

## 5. 遗留事项

1. backfill 持久化用现有 v4 表（batch key 作 id），后续 bulk backfill 真实启动前按计划再评估表演进；
2. live restore 未授权（TaskReport 记录 live restore count=0）；
3. Reviewer A/B 外部模型审阅按 standing directive 记录。

## 6. 下一步

P7 完成 → **Task 15：P8R3 attempt（Authority binding 与 FinalEval contract correction）**，沿用同一 JIT activation + gate 流程。
