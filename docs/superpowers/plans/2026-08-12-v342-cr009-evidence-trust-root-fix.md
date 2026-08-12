# V3.4.2 Corrective Recovery — CR-009：Committed-Blob Evidence Trust Root 修复

> **状态：NON_AUTHORITATIVE_PREPARATION（草案，未获批准，不构成任何执行授权）**
> 起草日期：2026-08-12
> 前置：GPT 独立审阅 `2026-08-12-v342-deepseek-execution-review.md`（HOLD / F-01 至 F-05）
> 分支：`codex/v342-control-plane`

---

## 1. 目的

修复 GPT 审阅 F-01 指出的 committed-blob evidence 根合同失效：TaskReport 的 `evidence_sha256` 目前是**路径字符串的 SHA-256**，而非该 ref 指向的 committed blob raw bytes SHA-256；多个 `input_evidence_refs` 指向不存在或错误阶段目录的文件；Gate verifier 不解引用嵌套 evidence；task-level `required_test_receipt_ids / required_review_receipt_ids / required_evidence_ids` 为空导致缺 receipts 仍可 PASS。

本 CR 只修复 **evidence 根合同机制**（coordinator、TaskReport requirements、Gate verifier、负向测试），不重跑迁移、不调用真实模型、不修改任何旧 TaskReport/Gate/closure/Authority 行。

## 2. 不可变边界（add-only）

- 旧 P0/P6/P7/P8/C0 Gate、closure、TaskReport、evidence、Authority/Operational 行**全部保持原样**；
- 本 CR 产生的所有工件以新 commit 追加；不覆盖、不删除、不原地修补；
- 用户受保护文件（CHANGELOG.md / daily_run.py / daily_select.py / docs/b1_v3_results.md）不触碰；
- 不执行：真实 LLM/provider 调用、live store migration、C1 rerun、promotion、`set_param`/reset/rollback。

## 3. 技术变更范围（草案，批准后实施）

### 3.1 `activation_coordinator.py`
- TaskSpec 的 `input_evidence_refs` 必须引用**已提交的** committed activation manifest/envelope；
- `evidence_sha256` 改为 **`SHA-256(committed blob raw bytes)`**（先创建并提交真实 evidence 文件，再引用其 blob hash）；
- 禁止：自引用、未来引用、`SHA256(path text)`。

### 3.2 TaskReport requirements 强制
- task-level `required_test_receipt_ids / required_review_receipt_ids / required_evidence_ids` 不得为空（除非 phase 合同明确豁免）；
- 任一必需 receipt 缺失时 TaskReport 必须 BLOCKED/FAIL。

### 3.3 `gates.py` Gate verifier 解引用
- 对每个 required `input_evidence_ref`：从 locked commit 读取 regular blob，核对 ref、blob SHA、commit/phase/attempt binding；
- 缺失、wrong-phase、uncommitted、hash mismatch 全部 FAIL；
- Gate-level phase cumulative test/review/evidence requirements 独立于 source TaskReport 强制。

### 3.4 负向测试
新增：dangling ref、路径字符串 hash、错误 blob hash、错误阶段目录、仅工作树存在、空 mandatory requirements、后置 supplement 冒充前置证据。

## 4. 授权要求（Gate 0 硬门）

实施前需用户批准：
1. 本 CR 的精确范围与不可变边界；
2. （若适用）修改 full discovery `exit 0` 合同必须先有独立 change request；
3. Reviewer A/B 调用前单独批准 actor/provider/model 与披露范围；
4. （若需要）live store migration 前单独批准目标与维护窗口。

## 5. 验收标准（对齐 GPT HOLD 解除标准）

- [ ] Gate verifier 从 locked commit 解引用每个 required evidence blob；
- [ ] 所有 `evidence_sha256` == committed blob raw bytes SHA-256；
- [ ] 无 dangling / wrong-phase / 仅工作树存在的 required ref；
- [ ] task-level 与 phase-level requirements 分层机械强制；
- [ ] 负向测试全绿；
- [ ] 原样重跑 P0 full discovery 达 exit 0（或 CR 批准改合同）；
- [ ] 新 P0 Gate/closure 在修复后 trust root 上重建；
- [ ] 旧 evidence/Gate/closure/store 行未被改写。
