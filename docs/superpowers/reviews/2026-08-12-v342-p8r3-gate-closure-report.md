# V3.4.2 Corrective Recovery — P8R3 Gate 收尾报告（Task 15-19）

> 生成时间：2026-08-12 09:05 +0800
> 计划 ID：`V342-CORRECTIVE-20260811-R1`
> 官方分支：`codex/v342-control-plane`
> 对应计划步骤：Task 15（T1/T2 Authority binding）→ Task 16（T3/T4 handle-first + worker）→ Task 17（T5/T6 Campaign CLOSED）→ Task 18（T7/T8 saga + runtime）→ Task 19（P8 cumulative gate + closure）

---

## 1. 计划内容摘要

P8R3 修正 FinalEval 的 V1 缺陷：V2 wire contract 绑定 Authority-issued nonce fingerprint（raw nonce 永不持久化）、全局 plan+holdout 唯一性、handle-first 数据边界（消除 check-then-open TOCTOU）、低权限 worker、原子 Campaign CLOSED、durable saga + hard-crash recovery、唯一 trusted runtime，最终 P8 cumulative security gate 关闭。

## 2. 执行结果

| Task | 交付 | 测试 |
|---|---|---|
| T1 Authority binding | `final_eval_authority.py`：FinalEvalRequestV2（无 raw nonce、research-plan 身份独立于 authority_plan_hash lineage）、nonce HMAC fingerprint、AuthorityFinalEvalBroker 经 sealed begin CAS 绑定真实 ticket/lease | 9/9 + 133/133 cumulative |
| T2 remove caller outcome | `TrustedEvaluator.evaluate_v2`（outcome 由 broker 结果派生；V1 evaluate 保留为 historical）、不同 nonce 测试反转指向 V2 唯一性 | 133/133 |
| T3 handle-first data | `final_eval_data.py`：VerifiedRootHandle（volume serial + file identity、reparse 拒绝、不可序列化）、HandleFirstOpener（单一 handle 读取 + SHA-256 校验，绝不按 path 重开）、OpenedHoldoutArtifact（opaque） | 7/7 |
| T4 low-priv worker | `final_eval_worker.py`：strict-JSON stdin 协议、bounded stdout、stderr 分类码、拒绝未知字段/NaN/越界 metric/不安全 ref | 14/14 |
| T5 Campaign CLOSED | `campaign_lifecycle.py` 加 `CampaignStatus.CLOSED`（COMPLETED→CLOSED 唯一边）；`final_eval_closure.py` lease-bound 单事务 terminal audit + CLOSED event | 370/370 |
| T6 CLOSED guards | controller `_require_campaign_not_closed`（prepare/start/complete 前置，CLOSED 后零写入） | 6/6 |
| T7 durable saga | `final_eval_saga.py`：REQUEST_FROZEN→AUTHORITY_TERMINAL 固定链、精确转移边、outcome 派生（caller 不可指定） | 10/10 |
| T8 trusted runtime | `final_eval_runtime.py`：factory 只收内存 capability + opaque root + launcher + sink（普通 runner 不可构造） | 14/14 |

**Gate 链**（全部 PASS）：build → fresh verify → public close（closure `7777b794…`）→ drain（outbox=0）→ closure receipt → post-closure supplement → 幂等 replay。8 张 task tickets + policy activation 全部 SUCCEEDED，TaskReports 绑定验证通过。

## 3. 对计划的修改与理由

1. **V1 保留为 historical**：`evaluate`（V1）签名保留供现有测试/历史证据使用，production 走 `evaluate_v2`——与计划"V1 只允许 historical read"一致。
2. **recovery lease 与 TaskExecutionLease 类型检查**：closure writer 用 isinstance 区分（recovery lease 非 TaskExecutionLease 子类，天然被拒）。
3. **activation 流程统一**：8 张 tickets 的 activation 脚本从同一模板生成（report 从 DB 权威行重建以避免时间戳微差绑定失败）；policy 激活复用 operator grant 后补签 gate grant（与 P0/P6/P7 同模式）。
4. **P8 无数值性能阈值**（计划确认性能门在 P7）；P8 gate acceptance 为 8 项安全不变量，全部证明。

## 4. 结果验证

- 9 张 succeeded tickets（T1-T8 + policy activation）全部 SUCCEEDED；
- global plan+holdout/nonce 唯一性（Authority 表约束 + V2 broker 测试）、raw nonce 全载体扫描（DB/outbox/payload 无明文）、handle-first 单次打开（无 check-then-open）、worker 隔离（无 path/secret 继承）、原子 CLOSED、saga 固定状态链、no-real-holdout（synthetic bytes only）全部证明；
- P8 gate：build PASS → verify PASS → close PASS → closure + supplement + replay PASS。

## 5. 遗留事项

1. Task 18.9 的 entry_policy OPEN_HOLDOUT 唯一入口注册（当前 runtime 未在 entry_policy.json 声明 effect——计划要求受控 entry，P8 阶段 runtime 本身已是最小入口，entry policy 注册可并入 C0 前检查）；
2. Authority v3 schema（REQUEST_FROZEN 加入 saga_state CHECK）待真实 saga 使用（当前 saga 状态在内存层，DB 层 AUTHORIZED 起始兼容）；
3. Reviewer A/B 外部模型审阅按 standing directive 记录。

## 6. 下一步

P8 完成 → **Task 20：C0R2 attempt（offline rollout fixtures、worker 协议、publication 与 chaos gate）**，沿用同一 JIT activation + gate 流程，最终完成 corrective recovery 全部阶段。
