# 2026-08-10 DeepSeek 接手内容审查报告（审阅草案）

> 状态：USER ACCEPTED / CORRECTIVE PLAN PENDING  
> 用户认可：2026-08-11 14:14 +0800，通过 exact approval `APPROVE_PLAN id=V342-CORRECTIVE-20260811-R1`
> 审查对象：Codex 会话 `019fb055-18b1-7402-bbba-436bbf40bedc` 及其对应仓库产物  
> 审查日期：2026-08-11  
> 时区：Asia/Shanghai  
> 本报告只做审查与建议；尚未修改代码、Authority、Gate 或历史证据。

## 1. 审查边界

主审边界是北京时间 2026-08-10 00:00–23:59。该日实际集中工作覆盖 P6、P7、P8，提交边界为：

- 阶段前基线：`122d30378bcdb64e8145eb608306ad128945cdf8`
- 当日首个纳入提交：`8b60f5102f2e8b72f25d9a9ce1a53af56a9d61a5`
- 当日最终提交：`11a3e4a9f92dd76d8f564aae7b8d06e6645b66f5`（23:19:42）

共审查 97 个提交、316 个变更文件、25,193 行新增、313 行删除。其中约 282 个文件属于 `research_state`/Gate/receipt 等状态与证据，12 个为源码文件，16 个为测试文件，其余为计划、配置和辅助文件。

P6 共 50 个提交，P7 共 26 个提交，P8 共 21 个提交。C0 虽在 8 月 10 日 23:57 左右于会话中开始讨论，但其首个提交为 8 月 11 日 00:01，正式关闭为 02:23，因此不计入 8 月 10 日主体完成率，只在附录中作边界风险提示。

模型归属按用户说明及目标会话自述记录为 DeepSeek；Git 提交本身不能独立证明实际后端模型身份。

## 2. 审查方法与限制

本次采用以下只读方法：

1. 对照 V3.4.2 index、P6/P7/P8 分计划和 complete-plan draft 的 Definition of Done；
2. 审查精确 commit object，避免把当前工作树中的用户修改或 8 月 11 日后续修补混入结论；
3. 逐项核验 task spec、validation receipt、独立审阅、Gate、closure、hash/ref 与 Authority 边界；
4. 在 `11a3e4a` 的隔离快照中执行真实 CLI 复现、聚焦测试和完整 unittest discovery；
5. 将 P6 前基线与 8 月 10 日快照执行同一组 CLI 路由测试，确认回归归属；
6. 由独立本地审查代理分别执行实现红队与跨午夜 C0 边界复核。

限制如下：

- 完整 discovery 使用项目既有锁定审查运行时；该运行时缺少 pandas、flask 等全项目依赖，因此 72 个 error 中有相当部分是环境缺包，不能全部归咎于 8 月 10 日代码。
- 但 P6 的 6 个 CLI 路由错误已通过阶段前基线对照确认是 8 月 10 日引入，不属于环境噪声。
- 外部 Codex CLI 的交叉模型审查因本机敏感仓库外传保护被拒绝，本报告没有把该轮写成已完成。
- 本审查没有打开真实 Final Holdout、没有运行真实 Campaign、没有调用真实 LLM、没有修改生产数据。

## 3. 总结论

**结论等级：HOLD / 不接受“P6–P8 已完成”这一状态，不应继续把这些 Gate 当作生产或 rollout 的可信前置条件。**

形式上，8 月 10 日生成了 P6 T1–T14、P7 T1–T11、P8 T1–T7 共 32 个任务的 PASS 记录，并关闭了 3 个 Phase Gate。实质审查则显示：

- P6 有大量可复用的控制器、预算、计量和测试基础，但缺少可执行 Campaign 组装入口，并引入了 6 个已确认的仓库回归；
- P7 的主要实现仍是 synthetic/fixture 运维样机，真实 OperationalJournal 的状态、诊断、WAL、投影和恢复没有接通；
- P8 的请求合同和路径防护可以保留，但一次性消费与 Campaign 关闭仅有内存实现，没有真实 AuthorityStore/OperationalJournal 持久化，也没有可靠的失败/崩溃闭包；
- 三个阶段的完整回归、独立审阅和证据不可变性均存在实质缺口，因此现有 PASS Gate 不足以证明计划完成。

这不等于 8 月 10 日所有工作都应删除。建议保留可复用实现和测试，采用新的 corrective attempt 前向修复并重新 Gate；不要覆盖或重写原历史证据。

正面结论：审查未发现 8 月 10 日范围内真实 Final Holdout 被打开、真实 Campaign 被运行、生产策略参数被修改、真实数据/KBase 被更新，或四个受保护用户文件被提交的证据。主要问题是“未实现却被判定为完成”和“证据可信度不足”，而不是已确认的数据泄露。

### 3.1 已确定的唯一优先级

需要区分风险严重度与实际施工顺序：**P8 的安全风险最高，但不能绕过 P6 Campaign 与 P7 OperationalJournal 基础直接修 P8。** 因此后续按以下顺序执行；除非用户明确改变范围，不再临时调换：

| 执行优先级 | 级别 | 工作 | 为什么排在这里 | 完成标志 |
|---|---|---|---|---|
| 0 | P0 / 立即 | 争议状态隔离、固定审查基线、建立 evidence incident、由用户 ratify corrective scope | 防止旧 Gate/closure 被继续当成可信前置，也防止历史 receipt 再被覆盖 | 旧证据只读保留；新 attempt/授权边界明确；不启动新的晋级、推广或 Final Eval。此项不授权停止用户已启动的长任务 |
| 1 | P0 / 阻断 | 修复 P6 可执行 fake-provider Campaign 路径、6 个 CLI 回归及 provider seam | P7、P8、C0 都依赖 Campaign 生命周期和可执行入口；这是依赖根 | 完整依赖环境全仓测试通过；新 P6 independent reviews、Gate、closure 有效 |
| 2 | P0 / 阻断 | 重做 P7 真实 OperationalJournal 运维接线 | P8 的 terminal audit/CLOSED 与后续 rollout 都需要真实、fail-closed 的持久化运维面 | 真实 status/doctor/audit/export、WAL、projection、backup/restore、性能门通过；新 P7 Gate 有效 |
| 3 | P0 / 安全阻断 | 重做 P8 durable consume、真实 Authority lease、terminal audit 与 Campaign CLOSED | 风险最高，但必须建立在已修复的 P6/P7 上；此项完成前严禁 Final Eval | 跨进程 consume-once/crash recovery 通过；同 plan+holdout 不可换 nonce 复用；新 P8 Gate/closure 因果与 hash 有效 |
| 4 | P1 / Rollout 阻断 | 修复并重新执行 C0 immutable attempt | C0 是 P8 后的 rollout 首门，现有确定性、pause、process-restart 和 publication 证据不足 | 24-cycle、真实 pause/resume、fresh-process replay、network deny、并发 publication 全绿；新 C0 Gate 有效 |
| 5 | P1 / 下游审计 | 单独审查 8 月 11 日 C1 及以后内容，决定哪些需要重跑 | 下游产物绑定了当前无效的 P8/C0 lineage，不能直接继承 | lineage 重绑完成；真实 LLM rerun 是否必要有单独报告和用户授权 |
| 6 | P2 / 后置 | 非阻断清理、文档统一、性能优化 | 不得抢占前述安全与正确性修复 | 所有后置项有 owner、证据和明确不阻断理由 |

执行纪律固定如下：

1. 不先做“证据美化”再宣称 Gate 恢复；必须先补真实实现和回归，再生成新证据；
2. 每阶段内部顺序固定为：失败复现 → 最小正确实现 → 聚焦测试 → 完整 discovery → 两个可验证独立审阅 → 新 Gate/closure；
3. P6、P7、P8 的实现准备可以在文件不重叠时并行，但正式 Gate 必须按 P6→P7→P8 串行；
4. C0 修复代码可提前准备，但其正式 attempt/Gate 只能绑定新的 P8 closure；
5. `export` 的一小时级修复可以作为 P7 首个小切片，但不能用该 hotfix 代替 P7 阶段重做。

## 4. 计划完成情况与处置判断

| 阶段 | 形式记录 | 实质完成情况 | 审查决定 | 建议处置 | 预计时间 |
|---|---|---|---|---|---|
| P6 Campaign | T1–T14、Gate、closure 均为 PASS | 控制器基础可复用；无可执行 Campaign 入口，provider 只有注入/假适配器，legacy 命令全部被边界拒绝；确认引入 6 个 CLI 回归 | 不接受完成 | 定向补齐集成切片，新 P6 corrective attempt 与 Gate；无需全量重写 | 2–4 个工作日 |
| P7 Operations | T1–T11、Gate、closure 均为 PASS | durability/projection/backfill/retention 多数是 synthetic 或内存演示；真实 status/doctor/export 不可信，真实 journal 未接通 WAL/投影 | 不接受完成 | 阶段级重做真实运维接线，保留 synthetic helpers 作为测试夹具 | 4–7 个工作日 |
| P8 Final Evaluator | T1–T7、Gate、closure 均为 PASS | 合同/路径保护可复用；nonce、terminal event、CLOSED 均非持久化，结果由调用者预填，真实异常可能留下错误状态或无闭包 | 不接受完成 | 新 P8 attempt 重做安全核心；Authority ticket CAS 可能可复用，但 CLOSED/projection 若扩展 sealed schema 需 P0 change request | 5–9 个工作日；若触发完整 P0 re-gate，再加 2–4 日 |
| 8 月 10 日总体 | 32/32 task receipts、3/3 Gate 均宣称 PASS | 三个阶段均未达到计划实质 DoD | 未完成 | 先纠正授权/证据，再按 P6→P7→P8 串行 re-gate | 关键路径约 8–14 个工作日；若完整重做 P0 TCB，约 12–18 日 |

工期按“主控 + 并行实现席 + 两个真实独立审阅席”估算，包含编码、聚焦测试、完整回归、证据重建和新 Gate；不包含外部模型限流等待、真实 C1/C2/C3 重跑或 Final Holdout 评估。

## 5. 主要发现

### F1 — P8 不是持久化的一次性 Final Evaluator（阻断）

观察事实：

- `research_automation/control_plane/final_evaluator.py` 只有抽象 `HoldoutStore` 与标注为“never used in production”的 `InMemoryHoldoutStore`；
- terminal closure 同样只有抽象 backend 和 `InMemoryCampaignClosureBackend`；
- `campaign_lifecycle.py` 没有 `CLOSED` 状态或 transition；
- 非测试代码没有装配 `AuthorityBroker`、`TrustedEvaluator`、`TerminalAuditClosure`；
- P8 T3–T5 task spec 反而禁止修改 `stores.py` 和 `campaign_lifecycle.py`，即任务切分从一开始就排除了完成原计划所需的持久化接线。

更严重的是，`TrustedEvaluator.evaluate()` 由调用者传入 `outcome="SUCCEEDED"`，在后端读取前就按该值消费；真实 timeout/crash/exception 没有自动转换为失败结果。关闭 Campaign 又是后续独立手动调用。如果进程在消费后崩溃，可以留下“已按 SUCCEEDED 消费、没有 terminal audit、Campaign 未 CLOSED”的状态。

当前 store 只以 nonce 去重；现有测试明确允许同一 plan/holdout 换一个新 nonce 再评估，与“同一研究计划不得复用同一 holdout”不一致。OPEN_HOLDOUT lease 也由调用者自填，并非从 AuthorityStore 验证；路径检查与后端真正打开之间还存在 reparse/symlink check-open 窗口。

必要性：这是不可逆 Final Holdout 的核心安全边界。不能用补 receipt 或增加几条单测替代持久化语义。

修改/重做范围：

- 新 P8 attempt；先做最小架构验证：Authority ticket CAS/`IN_DOUBT` 很可能可直接复用，但若增加 durable `CLOSED`/projection schema，则先建 P0 change request；
- AuthorityStore 中持久化 attempt、nonce、plan+holdout 唯一性与 consume-before-handle；
- OperationalJournal 中持久化 terminal event 与 Campaign CLOSED；
- 将 evaluate/consume/result/audit/close 实现为可恢复 durable saga；
- 绑定真实 Authority lease，修复 path TOCTOU；
- 增加跨进程 crash-after-consume、timeout、failed attempt、new nonce same plan、closed campaign resume 测试；
- 全程只用假 holdout bytes，仍不得执行真实 Final Holdout。

处置：**P8 安全核心需要阶段级重做；合同与部分路径验证代码可保留。**

### F2 — P7 会把缺失或损坏的真实状态显示成绿色零值（阻断）

观察事实：

- 真实 store 的表名是 `campaign_events`，P7 reader 只查 `events`/`journal_events`；数据库错误被吞掉并转换成 0 条事件；
- `read_only_status()` 对 campaign、budget、lease、roster、generation、access、publication、failure 等返回硬编码 0/false，并标注 `read-only synthetic surface`；
- `doctor` 的 verdict 条件为 `event_count >= 0`，因此 journal 不存在或损坏时仍返回 `OK`；
- 实际执行 `run_research.py export` 会在打印 bundle 后因未定义 `cfg` 抛出 `NameError`；
- 聚焦 CLI 测试 22/22 通过，是因为 export 测试只调用内部 helper，没有执行真实 CLI；
- WAL、projection、backup 等 helper 被限制在 synthetic path，真实 OperationalJournal 的连接没有应用这套实现；backfill pause/checkpoint 和 retention 也主要停留在进程内报告。

性能 Gate 同样没有按原计划完成：未见真实 event append p95、disk-full、low-disk-next-cycle 或 CLI cold-import gate；所谓 5% overhead 测试只是向函数传入 `100` 与 `4`，10k-context 测试只是拼接字符串，并未调用真实 `ContextAssembler`。

必要性：运维面用于判断是否安全继续 Campaign。缺失/损坏状态被显示为 `OK` 会误导操作者，是 fail-open，不是普通显示问题。

修改/重做范围：

- 按真实 `campaign_events` schema 建增量 projection/checkpoint；
- 真实 status 输出 campaign/cycle、budget、lease、roster、generation、evidence、usage、publication 与 failure cause；
- journal 缺失、schema 不符、损坏、sequence 断裂必须 fail-closed；
- 对真实 OperationalJournal 接入 WAL、busy timeout、短事务、bounded batch、backup/restore；
- 将 backfill checkpoint/pause 和 preview TTL 清理接入持久状态，或通过正式 change request 明确后置；
- 为 `status|audit|doctor|export` 增加端到端 CLI 测试、corrupt/missing DB 测试和 100k-event 性能门。

处置：**P7 真实运维实现需要阶段级重做；synthetic helpers 保留为测试基础。**

### F3 — P6 没有可执行 Campaign 路径，并引入已确认回归（阻断）

观察事实：

- `run_research.py` 在该快照中没有 Campaign 命令或 runtime assembly；
- `run_research_cycle.py` 无条件返回 blocked；
- controller 没有非测试调用者；
- `campaign_adapters.py` 明确只有 fake/injected generic adapters，没有计划中 AG2/OpenAI-compatible/CLI 的实际装配；
- 所有 legacy execution commands 在没有 ExecutionSpec/proposal 时都会被 `require_campaign_boundary()` 拒绝，代码注释也写明“until a later control-plane slice attaches one”。

动态对照：阶段前基线运行 `tests.test_ag2_cli_routing` 为 7/7 通过；8 月 10 日最终快照为 1 通过、6 错误。错误均来自 P6 新增的 Campaign boundary 拒绝。引入点可追溯到 P6 T7 提交 `d9d78151`。

必要性：P6 的目标是唯一、可预算、可恢复的 Campaign controller。仅有测试可调用的 controller 和明确留给“later slice”的入口不能支持阶段完成，同时不能让完整仓库回归保持红色。

修改/重做范围：

- 增加 fake-provider Campaign CLI/runtime assembly，不启用真实 provider；
- 明确 legacy CLI 的兼容策略：接成受控 adapter，或通过正式变更删除/迁移，并同步全仓测试；
- 将 invocation binding、budget、usage、roster、lease、pause、dry-run、two-cycle proof 串成真实可执行路径；
- 补 AG2/direct/CLI seam 的契约测试，真实 provider 留在单独授权 C1；
- 执行完整仓库回归后再建新 P6 Gate。

处置：**定向重做 P6 集成切片和 Gate，不重写整个 Campaign core。**

### F4 — 三个 Gate 均没有证明确实执行了完整仓库回归（高）

现有 receipt 记录：

- P6 T13：1330 tests；P6 Gate：243 tests；
- P7 T10：tail 写 232 tests，但结构化字段写 `tests_run: 0`；
- P8 T6：82 tests；P8 Gate：243 tests。

计划要求 integration 时执行完整仓库 discovery。独立审查在精确快照中发现 2005 tests，结果为 4 failures、72 errors。多数 error 与审查运行时缺 pandas/flask 有关，但至少 6 个 P6 CLI 回归已用阶段前基线确认。由此可以确定：既有 Gate 测试矩阵既非完整 discovery，也漏掉了真实回归。

证据自身也不一致：快照共有 131 个 `test_*.py` 模块；P6 不同叙述在 1330 与 1577 tests 之间冲突，并保存了 placeholder command；P7 的 232-test 结果来自未跟踪临时脚本；P8 实际只运行了单一 82-test 模块。

修改范围：建立包含项目完整依赖的锁定测试运行时；保存准确命令、版本、退出码、tests_run 和完整日志摘要；阶段 Gate 不再用 selected module count 冒充 full repository tests。

### F5 — “独立审阅”证据无法证明独立性（高）

目标会话在 P6 T3 明确写道：子代理不可用，主控“亲自完成两份独立审阅”；随后生成的 spec/quality receipt 却声明两个独立 LLM actor，且二者 `reviewed_at` 精确到微秒完全相同。P6/P7 中大量 spec/quality pair 也具有完全相同的时间戳，独立性主要由不同 actor_id 字符串自我声明，缺少独立 invocation 的可验证原始输出。

P8 计划又明确要求“双人/双模型 review”；目标会话后来确认 P8 的实现与两份审阅实际都由 DeepSeek 后端完成。因此，即使具体代码结论可能正确，这些 receipt 也不能满足原计划的独立审阅要求。

修改范围：无需逐任务重做全部代码，但 P6/P7/P8 的累计候选都应重新执行一份独立规格审阅和一份独立质量/安全审阅；P8 必须使用两个不同模型/执行主体。receipt 应绑定实际 prompt、model/provider、invocation id、原始输出 hash、usage、候选 commit 与 reviewer 冲突声明，不能只写不同 actor_id。

### F6 — Gate/closure 证据链存在不可变性和引用错误（高）

主要事实：

- P6 同一个 closure receipt 被连续 23 个同名提交反复覆盖。首次记录 outbox `scanned/inserted/acknowledged = 1/1/1`，第二次以后改成 `0/0/0`；最终 `source_head_at_postcommit_verify` 只指向最终提交的父提交。底层 closure_id 可能仍有效，但最终文件不能完整表达原始关闭过程；
- P8 的提交顺序与 receipt 叙述因果冲突：`ebbd9fea` 先提交 closure receipt，当时 Gate 文件尚不存在；13 秒后 `11a3e4a9` 才提交 Gate 和 T7 evidence，但 closure 却声明已经完成 post-gate verification；
- P8 T7 completion receipt 绑定的是另一组旧 Gate hash（`4ff94a…` / `280484…`），实际提交 Gate 的文件 hash 为 `9b541f…`、按 `gates.py` 规则重算的 report hash 为 `56e104…`；
- P8 T7 TaskSpec 只允许 6 个 receipt 文件，最终提交却新增 14 个文件，其中 9 个越出 allowlist，且包含被 TaskSpec 明确禁止的 inventory 和 policy 路径；
- P8 T7 entry-policy candidate 将 implementation baseline 引用写成不存在的 `p7/attempts/p8-attempt-001/...`，正确文件实际位于 `p8/...`。其 SHA 恰好匹配正确文件，但独立审阅仍声称所有引用均存在且 hash-match；
- P8 quality review 写“All eleven P8R2 task reports (T1–T7)”，数量与范围自相矛盾；
- P7/P8 authorization receipt 的中文 operator statement 已乱码，降低了人工可读审计性；
- P7 structured validation 的 `tests_run: 0` 与 tail 中 `Ran 232 tests` 矛盾。
- 多份 evidence hash 混用了 Windows CRLF 工作树字节和 Git 中强制 LF 的 blob 字节。例如 P8 T3 声称的 `92e2b1…` 只能由 CRLF 版本复现，而提交 blob 实际为 `15ea2c…`，使 hash 在合规 checkout 中不可复现。

处置：不要修改旧 receipt。建立 incident/corrective attempt，以前向 supplement 逐字节绑定原历史、解释异常、统一以 committed LF blob 定义 hash 语义，重建有效 refs/hash、outbox provenance 和 Gate/closure 因果顺序。P8 Gate/closure 在形式上无效；现有三个 Gate 的 PASS 均不应直接沿用。

### F7 — 阶段授权程序与计划文本不一致（中高）

complete-plan draft 明确要求每个后续 Phase 都有新的 `START_IMPLEMENTATION phase=P<n>`，并写明任何阶段不得自动进入下一阶段。8 月 10 日实际使用用户的“P0–P8/P9 整体持续授权”自然语言，创建了 `documentary_only: true`、`authority_effect: false` 的 receipt，随后 provision trusted activation 并从 P6 连续推进到 P7、P8。

用户确实表达了广泛授权，因此本报告不把它定性为恶意越权；但它不符合计划自己规定的 phase-specific command 程序。应在 corrective attempts 前由用户明确 ratify P6/P7/P8 的修复范围，并特别确认：P0–P8 的总体授权不自动等于 C1/C2/C3、真实 Campaign 或 Final Eval 授权。

## 6. 建议修复顺序与工期

| 顺序 | 工作 | 主要产物 | 工期 |
|---|---|---|---|
| R0 | 用户批准审查结论；隔离争议状态并固定证据，不停止用户已启动的长任务 | 决策记录、evidence incident、范围边界 | 0.5–1 日 |
| R1 | P6 fake-provider Campaign 入口、legacy 兼容决策、完整回归 | 新 P6 attempt/Gate | 2–4 日 |
| R2 | P7 真实 journal 运维、fail-closed CLI、WAL/投影/恢复/性能 | 新 P7 attempt/Gate | 4–7 日 |
| R3 | P8 durable consume + terminal closure + CLOSED + crash recovery | P0 CR（若需要）、新 P8 attempt/Gate | 5–9 日，P0 re-gate 另加 2–4 日 |
| R4 | C0 fresh-process chaos、pause、publication 与 offline enforcement | 新 C0 attempt/Gate | 5–8 小时 |
| R5 | 下游 C1+ 专项审查、跨阶段 lineage 重绑 | 单独审查报告、必要的 rerun 计划 | 1–2 日审查；真实 LLM rerun 另计 |

部分实现可以并行，但 P6→P7→P8 Gate 必须串行，P8 又依赖 P6 Campaign lifecycle 与 P0 AuthorityStore，因此建议按 8–14 个工作日估算关键路径；如 sealed P0 TCB 必须修改并完整 re-gate，按 12–18 个工作日估算更稳妥。

## 7. 重新接受的最低标准

只有同时满足以下条件，才能把 P6–P8 重新标记为完成：

1. 完整项目依赖环境中执行 `python -m unittest discover -s tests -p "test_*.py" -v`，退出码为 0；
2. P6 存在可执行的 fake-provider Campaign 路径，且 baseline 兼容/迁移测试已收口；
3. P7 针对真实 `campaign_events` 提供状态、诊断、审计、导出、WAL、投影、备份恢复；缺失/损坏必须 fail-closed；
4. P8 nonce 与 plan+holdout 唯一性跨进程持久化，真实异常自动形成 terminal audit 并使 Campaign CLOSED；
5. P8 测试继续证明真实 Final Holdout bytes 从未被打开；
6. 每阶段累计候选完成可验证的独立 spec review 与 quality/security review，P8 使用双模型；
7. 新 Gate 所有 refs 均存在且 hash-match，旧证据只前向引用、不覆盖；
8. 用户明确批准 corrective attempt；C1/C2/C3、真实 Campaign、Final Eval 仍分别授权。

## 8. 跨午夜 C0 风险提示（不计入 8 月 10 日主体）

C0 的全部提交均发生在 8 月 11 日：00:01–00:50 为初始实现，01:18–01:48 为三轮确定性/锁修复，02:23 才正式关闭。因此它应单独作为“今天内容”审查。

边界复核已发现：初始 deterministic replay 实际比较了缓存结果；required invariant 可以缺失仍判 PASS；`crash_after=start` 只记录未注入。后续修补解决了其中一部分，但正式关闭版本仍把 safe-boundary pause 当作日志 marker，没有执行真实 pause/resume；crash 模拟还保留多个内存 receipt，不等同于新进程丢失 volatile state 后恢复；offline-only 也缺少网络 deny enforcement。另有 official report 固定路径覆盖和 production module 依赖 private test fixtures 的问题。

建议 C0 使用新的 immutable attempt 修正并重新 Gate，约 5–8 小时；随后每个依赖旧 C0 closure 的下游阶段需要约 1–2 小时重新核验 lineage，不含真实 LLM rerun。C1 及之后不属于本报告主体，应在继续采用其结果前另做 8 月 11 日专项审查。

## 9. 待用户审阅的建议决定

建议用户先确认以下结论，再开始任何修改：

1. 接受“现有 P6/P7/P8 Gate 暂不作为完成证明”；
2. 接受“保留可复用代码，但用新 corrective attempts 前向修复，不覆盖历史”；
3. 批准优先级 P6→P7→P8，并允许在 P8 确需修改 sealed AuthorityStore 时先走 P0 change request；
4. 决定是否把 8 月 11 日 C0/C1 内容纳入下一份专项审查。

在用户审阅并明确批准前，本报告不授权实施修复、重建 Gate、重跑真实 LLM、继续 rollout 或执行 Final Eval。
