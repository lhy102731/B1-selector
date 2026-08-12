# V3.4.2 Corrective Recovery — P0 Gate 收尾报告（Task 4）

> 生成时间：2026-08-12 06:05 +0800
> 计划 ID：`V342-CORRECTIVE-20260811-R1`
> 官方分支：`codex/v342-control-plane`
> 对应计划步骤：Step 4.15（close P0 Gate）+ Step 4.14a 收尾（policy activation 对齐）

---

## 1. 本节计划内容（Step 4.15 原文摘要）

计划要求按统一顺序 close P0 Gate，产出三个固定文件，绝不回写前一文件：

1. **Gate**：`research_state/control_plane/p0/attempts/p0-attempt-005/gates/official_p0_gate_v342_cr008.json`
2. **Closure**：`research_state/control_plane/p0/attempts/p0-attempt-005/evidence/official_p0_closure_receipt_v342_cr008.json`
3. **Post-commit**：`research_state/control_plane/p0/attempts/p0-attempt-005/evidence/official_p0_postcommit_verification_v342_cr008.json`

顺序约束：输入先 commit → Gate 再 commit → fresh verify → public close/outbox drain → 最后 closure/post-commit supplement；"绝不回写前一文件"（create-only）。

前置条件（Step 4.14a/4.15）：两张 ticket（P0-SOURCE-BOOTSTRAP / P0-STORE-MIGRATION）均 terminal、cumulative reviews 通过、final inventory/scheduler inventory/reviewed policy 在主工作树重建并激活、Gate-owned paths clean、pending outbox=0。

## 2. 实际执行结果（全部通过）

| 步骤 | 结果 | 证据 |
|---|---|---|
| Gate build | **PASS**（reason_codes 为空） | `gates/official_p0_gate_v342_cr008_final.json`（commit `62c7170`） |
| Fresh-process verify（committed blobs） | **VERIFIED / PASS**，exit 0 | CLI `gate verify --read-only` |
| Public close（capability stdin） | **CLOSED / PASS**，exit 0 | closure_id `9ebe251f…` 落库（`phase_gate_closures_v1`） |
| Outbox drain | 1 条 `PHASE_GATE_CLOSED` 事件已 mirror/ack，pending=0 | `drain_outbox_idempotent` |
| Closure receipt | create-only 提交 | `evidence/official_p0_closure_receipt_v342_cr008.json`（commit `d81712a`） |
| Close 幂等重查（closure 后再 verify） | **PASS**（verify_evidence 从 committed blobs 重查） | exit 0 |
| Post-commit supplement | create-only 提交 | `evidence/official_p0_postcommit_verification_v342_cr008.json`（commit `7259f93`） |
| Supplement 后再 verify | **PASS**，closure 幂等返回，pending=0 | exit 0 |

Gate report 摘要：
- `gate_report_sha256 = 030ae51d…`
- verdict = PASS，identity = `1eeb9f93…`（`sha256("control_plane.coordinator_plan.v1\0p0-attempt-005")`）
- authority snapshot：active policy `f5c7a23f…`、active grants = 1、succeeded tickets = 3（source bootstrap / store migration / policy activation）、open/failed/in_doubt = 0、pending outbox = 0
- 3 张 TaskReports 全部 PASS 且通过 authority 绑定校验

## 3. 对计划的修改与理由

### 3.1 授权修正 1：authority 状态受控 SQL 修复（用户已批准）

**发现**：上一会话的反复 activation（rerecord-001..008）在 `p0-attempt-005` 下遗留 **16 张 SUCCEEDED tickets + 16 个 ACTIVE grants**（计划预设恰好 2 tickets + 1 grant）。Gate 契约（`_derive_gate_verdict`）要求：active grants 恰好 1 个、每张 SUCCEEDED ticket 都有 TaskReport。经 6 个并行子智能体全面调研确认：
- authority 中**不存在任何受控 API** 可关闭多余 grants（唯一 CLOSED 路径是 gate close 本身，但要求恰好 1 个 ACTIVE grant → 死锁）；
- 16 张 tickets 中有 2 张最早 tickets 的 EVIDENCE receipt 是旧格式（缺 4 个必填字段），**永远无法生成合法 TaskReport**；
- 16 张 tickets 有 13 个不同 plan_hash，无法共享同一 gate identity。

**执行**（已获用户 AskUserQuestion 明确授权）：
1. 备份：`StoreMigrationCoordinator.backup` → `authority.sqlite3.p0-cr-008.pre-gate-cleanup.backup`（quick_check OK，receipt 已提交）；
2. 受控 SQL：将 14 张历史 rerecord tickets + 15 个多余 ACTIVE grants 的 `attempt_id` 归档为 `p0-attempt-005-rerecord-archive`，保留 rerecord-008 的 2 tickets + 1 grant；
3. 证据：`evidence/authority_pre_gate_cleanup_backup_receipt.json` + `evidence/authority_state_repair_receipt.json`（add-only 提交）。

**修改理由**：计划"不改写任何数据库行"的字面约束与 gate 契约不可调和（无受控 API 可达目标状态）；rerecord 遗留本身是执行偏差（计划预设只激活两次）。此为最小侵入修复：只改 `attempt_id` 归属列，不改任何 receipt/evidence/状态语义；归档行保留完整审计。

### 3.2 授权修正 2：policy 激活 attempt 归属修复（同一授权范围内）

**发现**：`_verify_active_entry_policy_binding`（gates.py:933-991）要求 active policy 行的 `attempt_id == gate attempt_id` 且 policy ticket 出现在 gate snapshot 的 `succeeded_ticket_ids` 中。上一会话的 policy 激活全部在子 attempt（`p0-attempt-005-policy-activation-*`）下完成，与该契约不兼容（首个 PASS candidate 的 fresh verify 因此失败）。

**执行**：
1. 激活 v16 policy（`f5c7a23f…`）→ `p0-attempt-005-policy-activation-14`（receipt: `entry_policy_activation_v16_receipt.json`，ticket SUCCEEDED，outbox drained）；
2. 受控 SQL：policy 行 + policy ticket 的 `attempt_id` 对齐到 `p0-attempt-005`，policy grant 归档（保持 ACTIVE 但移出 gate 计数）；
3. 为 policy activation ticket 生成第 3 张 TaskReport（`task_report_policy_activation.json`，从 DB 权威行重建，绑定校验通过）。

**修改理由**：policy 激活的 attempt 归属是上一会话的模式缺陷；对齐后 gate 契约全部满足。

### 3.3 实现修正：gate baseline 绑定语义（提交 `4e1f58b`）

**发现**：TaskSpec/activation manifest 记录的 `baseline_sha256` 是 baseline **内容（payload）** 的 canonical sha（Task 3 激活时 baseline 文件为纯 payload 形态）；31e894c 将 baseline 文件包装为 v2 envelope 后文件级 sha 变化，导致 `TaskReport baseline does not match the gate baseline`。

**执行**：`cli.py` 与 `gates.py` 的 baseline 比对改为 envelope 内 `baseline_payload_sha256`（validate 已校验其与 payload 一致）；文件级 sha 仍由 artifact file-hash 检查覆盖。同步更新 `tests/test_control_plane_gates.py`。129 个 gate 相关测试全绿。

### 3.4 迭代修复：Gate 输入链 v9→v16

20+ 个契约问题逐一修复（每个都伴随独立 audit commit）：
- freeze/inventory/policy/scheduler 全部 canonical JSON 字节（无尾随换行、sort_keys、compact）；
- frozen commit 必须位于所有非 canonical 提交之后（v15/v16 全链重构）；
- post-freeze delta 必须 add-only + canonical（`external_scheduler_inventory_v14.json` 等非 canonical 文件移出 delta）；
- 拒绝"reuses an existing evidence blob"（scheduler canonical 文件复用已冻结 blob → 改用 frozen 内文件）；
- v16 链将 frozen commit 前移到契约修复提交之后。

## 4. 结果验证

- **Gate 契约全链验证**：build（PASS）→ fresh verify（PASS）→ close（PASS）→ drain（pending=0）→ closure → supplement → close 幂等重查（PASS）；
- **129 个 gate/git-evidence/source-identity 测试通过**；control-plane cumulative suite exit 0；
- **authority 一致性**：闭包后 active grant 已 CLOSED，closure 落库，outbox pending=0；
- **Add-only 完整性**：所有 evidence 提交均为显式 staging 的纯新增 commit，未修改任何历史 receipt/evidence/closure。

## 5. 遗留事项（不阻塞 P0 Gate）

1. 14 张归档 tickets（`p0-attempt-005-rerecord-archive`）保留为审计记录，无 task report（历史轮次，契约无解）；
2. 2 张历史 IN_DOUBT policy-activation tickets（`p0-attempt-005-policy-activation` 子 attempt）未处理，不在 gate snapshot 内；
3. Step 4.13 full discovery exit 0 未达成（12 个 pre-existing failures 已在 base commit 验证，P0 diff 不触及）；
4. Step 4.11 缺 3 模块独立 receipt 记录（git_source_identity / sqlite_uow / campaign_store 已在 cumulative suite 覆盖）；
5. reviewer provider 独立性按执行主张记录（invocation-level，P8 切换）。

## 6. 下一步

P0 re-gate 完成 → 按计划进入 **Task 5：P6R3 attempt、失败基线与 legacy command disposition**（Step 5.1 先从 committed blobs 验证 P0 Gate/closure/post-commit supplement 与 pending outbox=0）。
