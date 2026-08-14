# CR010-R01..R07 功能问题复现证据（2026-08-15）

复现基线：commit afb8ad2（CR-010 v3 最终 re-gate 完成后的 HEAD）
复核来源：`docs/superpowers/reviews/2026-08-15-v342-cr010-content-completion-review.md`
规则：先复现"现状失败"，再修改代码；所有修复配套行为测试。

## R01 [阻断] evaluate_v2 未传 outcome → AuthorityBroker.consume TypeError
- 复现命令：
  ```
  python -c "from tests.test_control_plane_final_evaluator import _request; ...; te.evaluate_v2(req, data_root=TrustedEvaluatorDataRoot(root='/tmp/x', holdout_refs=('holdout-final-1',)))"
  ```
- 实际输出：`TypeError: FakeBroker.consume() missing 1 required keyword-only argument: 'outcome'`
- 真实 `AuthorityBroker.consume()`（final_evaluator.py:503）要求 keyword-only `outcome`；`evaluate_v2()`（final_evaluator.py:1220）调用 `self._broker.consume(request)` 未传 → 生产入口必然 TypeError。
- 现状测试只断言 callable 名称/policy 元数据，未执行 evaluate_v2 真实行为。

## R02 [阻断] RESULT_STAGED 无 object/claim/hash 验证
- 复现：`python -m unittest tests.test_control_plane_final_eval_orchestrator.FinalEvalOrchestrationTests.test_orchestrator_advances_consumed_to_result_staged` → OK
- 该测试 sink 返回 `research_state/control_plane/p8/attempts/p8-attempt-003/evidence/worker_result.json`，**文件从未创建**（ls 确认不存在），绑定仍进入 RESULT_STAGED。
- stores.py:5047 `_stage_final_eval_result` 仅做路径格式 + sha 格式校验，不检查 blob 存在/committed/内容 hash/claim 唯一性/同卷。
- orchestrator（final_eval_orchestrator.py:145）：`result_object_ref == result_claim_ref`（同一路径），`result_object_sha256 = _document_sha256(result_document)`（内存文档 hash，非文件字节 hash）。

## R03 [阻断] recovery lease 非 durable、CLOSED→Authority finish 无独立边界、expected_version 未校验
- stores.py:5162 `_issue_final_eval_recovery_lease` 只读校验后返回内存 `FinalEvalRecoveryLease`，无 durable lease 行。
- stores.py:5230+ `_recover_final_eval_binding` 在**同一事务**内完成 RESULT_STAGED→CLOSED→AUTHORITY_TERMINAL + task_tickets 状态更新（Authority finish），无 CLOSED 后独立 crash 边界（CRASH_POINTS 无 CRASH_AFTER.CLOSED）。
- final_eval_orchestrator.py:57 `OrchestrationInputs.expected_version` 声明但从未校验（grep 仅构造处出现；所有 CAS 用 snapshot.saga_version）。

## R04 [高] FinalEvalRuntime 孤立内存 happy path
- final_eval_runtime.py:38-91：`_saga_state`/`_steps` 内存推进；不调用 orchestrator/reconciler/Authority store；返回 `evidence_ref=None`。
- grep：生产代码（research_automation/ 除自身模块外）无任何 FinalEvalRuntime 调用者。

## R05 [阻断] 生产 rollout 依赖 tests.* fixture；worker 固定 SUCCEEDED
- rollout_chaos.py:79-115 `_test_fixtures()` lazy import `tests.test_control_plane_campaign_freeze/_protocol_member`、`tests.test_control_plane_campaign_lease/_FakeProcessIdentityProvider`、`tests.test_control_plane_campaign_preflight/_scope`、`tests.test_control_plane_campaign_store`、`tests.test_control_plane_campaign_two_cycle`、`tests.test_control_plane_evidence_learning`、`tests.test_foundations_protocols/_protocol`。
- rollout_chaos_worker.py:208-245 `run_worker` 对所有合法 step 固定返回 `{"outcome": "SUCCEEDED", "completed_cycles": 0, "state_digest": None, ...}`，不执行 controller step，无真实 state/digest/evidence。

## R06 [阻断] 同进程模拟、无 durable pause/resume、invariant 未 fail-closed、AtomicPublisher 非 crash-safe
- 复现 invariant 缺口：
  ```
  python -c "from research_automation.control_plane import rollout_chaos; p = rollout_chaos.run_c0_simulation(seed=20260811, cycles=20).to_payload(); ..."
  ```
  实际：produced=14，EXACT=10；missing=`durable_pause_resume`/`fresh_process_identity`/`network_denied`；`pass` 字段仍为 True。
- `require_exact_invariant_set`（rollout_chaos.py:1732）在 official 路径从未被调用（grep 仅定义处）；且自身抛 `NameError: name 'Mapping' is not defined`（未导入 Mapping）。
- regate_driver stage3c（88ff213）直接调 `run_c0_simulation()` 同进程模拟；replay 同进程同 deterministic root；pause_after 仅写 marker。
- AtomicPublisher（rollout_chaos.py:1631+）直接 O_EXCL 写最终 object/claim：无同卷 temp staging、无 atomic rename、无父目录 durability barrier；写入中崩溃留 partial object 且后续被当 IDEMPOTENT_EXISTING。

## R07 [高] no-side-effect 仅看 Git 状态行
- regate_driver.py stage3c `c0_no_side_effect_receipt`：仅 `git status --porcelain` before/after + deterministic root 在 tempdir；未覆盖 Authority/Operational DB、data、KBase、config、strategy、provider registry、网络探针、保护文件。
