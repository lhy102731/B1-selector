# V3.4.2 Lean Research Control Plane 完整实施计划

状态：`DRAFT_FOR_APPROVAL`  
变更请求：`P0-CR-002`  
实施基线：`V3.4.2-P0R2`  
日期：2026-07-26  

本计划在获得批准并生成新的 `plan_hash / scope_hash / instruction_policy_hash` 后，取代 V3.4.1-P0R1。P0R1 已领取的授权、T1/T2 原始报告和旧哈希只保留为不可变审计证据，不得复用为新授权。

## 1. 最终目标

在不修改生产策略、信号、参数和研究验证范围的前提下，建立一条可以连续运行多轮、每轮自动吸收上一轮有效经验、遇到污染或越权自动失败关闭、并能完整审计和恢复的研究控制链。

系统完成后必须做到：

1. 每次运行绑定冻结的协议、代码、数据 generation、特征、模型、阈值、roster 和配置身份。
2. 训练、验证、fold test、Final Holdout、Prompt 展示和派生结果全部可追踪，污染会沿 lineage 传播。
3. Runner 只生成 raw artifacts；科学结论由独立 EvidenceAdapter 机械判定。
4. 正面、负面、PARTIAL 和 anti-factor 结论均可按明确 scope 留存；失败用法不会误封整个机制。
5. Learning Packet create-only、content-addressed；Ledger、Memory、预算、coverage 和状态均为可重建 projection。
6. Campaign 完成一轮后，只有已提交且安全的 Learning 才进入下一轮；`NO_MATERIAL_FINDING` 只影响信息增益，不制造空结论。
7. 可运行数小时并在崩溃后幂等恢复；不会静默缩减 roster、改参数、改验证范围或杀死正在执行的任务。
8. Final Holdout 只在候选完全冻结后，经单独一次性授权打开一次；成功、失败、超时或崩溃后均永久消费。

## 2. 相对 V3.4.1 的必要修订

| 原设计 | V3.4.2 修订 |
|---|---|
| Access/Campaign 写“现有 control-plane SQLite” | P0 AuthorityStore 与 OperationalJournal 物理隔离，只共享底层事务 implementation |
| T2 先冻结入口 hash，T3-T7 后续又修改入口 | 代码冻结后再执行最终 inventory/policy 复核；最终扫描可以更新 policy，但 scanner 不能自批 |
| 每个模块自己实现 SQLite、锁、迁移、sequence | 一个 durable transaction kernel；不同领域保留独立状态语义，不建万能 FSM |
| 新写 GenerationPublisher | 从现有 KBase semantic/catalog 发布代码提取 ImmutableReleaseStore，KBase 与 Generation 各用 Adapter |
| 复制或长期保留旧 data generation | Campaign 持有共享读租约；日更可 staging/校验但发布等待，避免复制 84G |
| 新 Campaign 继续接旧 TaskQueue/resume | work item 写 OperationalJournal；TaskQueue、STOP、status.json、autorun 仅作 legacy Adapter |
| 多套 patch/apply/rollback | 统一走隔离 workspace、`git apply --check`、路径 allowlist、编译/测试和丢弃 workspace 回滚 |
| MemoryRouter 上再叠新 memory_packet | ContextProjection 成为新路径唯一事实输入，旧 MemoryRouter/project_state 只做一次性 legacy Adapter |
| 手写 retry、字符估 token、固定 3000 token | Tenacity + UsageEnvelope + tokenizer Adapter；未知保持 null/UNKNOWN，预算按预留上限结算 |
| dataclass、JSON Schema、task report 各自演化 | 版本化 ContractRegistry；Pydantic 模型生成 JSON Schema，语义交叉校验仍由项目代码负责 |
| 两个 CLI 都实现 Campaign/status | `run_research.py` 是唯一命令所有者；`run_research_cycle.py` 只翻译旧参数或提示迁移 |

## 3. 架构约束

### 3.1 两个信任域

```text
用户精确授权命令
        │
        ▼
AuthorityBroker
        │  只通过固定路径、单次 capability、BEGIN IMMEDIATE
        ▼
research_state/control_plane/authority/authority.sqlite3
        │
        │  capability reference；不暴露建库/发权能力
        ▼
EntryGuard / PhaseGrant / TaskTicket / SideEffectLease

普通研究运行
        │
        ▼
research_state/control_plane/operational/operational.sqlite3
        │
        ├── run/cycle/access/derivation/usage/budget/lease events
        ├── Learning commit events
        └── projections/checkpoints/audit export
```

- AuthorityStore 是固定、预置、fail-closed 的授权事实源。普通 Runner、Campaign、LLM 和 Web 入口不能取得其写连接。旧 `p0/control_plane.sqlite3` 若存在只按 legacy audit 读取，绝不原地升级为新 authority。
- OperationalJournal 是运行事实源。它不能签发 phase、task、campaign 或 Final Eval 权限。
- 两者可以复用连接、迁移、CAS、幂等和事务测试 implementation，但数据库文件、schema owner 和写入者必须分离，禁止 `ATTACH DATABASE` 和伪跨库事务。
- Authority 事务在同库写 `authority_outbox`；镜像到 OperationalJournal 时按 event_id 幂等。镜像失败不回滚已经消费的权限，也不得重做 authority action；pending outbox 会阻止 phase close，直至安全重放完成。
- 威胁模型防止自动化误用、未授权入口、重放、并发竞态和崩溃恢复错误；不声称能抵御同一 Windows 管理员账户下的恶意本地进程。更强隔离需要独立 OS 账户/ACL/沙箱，暂不纳入本轮。

名词必须拆开，禁止继续复用模糊的 `policy_hash`：

- `instruction_policy_hash`：本计划、AGENTS 和实现约束的权威 hash；
- `entry_policy_sha256`：某次 code freeze 后人工审阅的 executable/import/scheduler policy；
- AuthorizationEnvelope 绑定前者，Phase gate 同时绑定当期 final inventory 与后者。

### 3.2 唯一事实源

| 事实 | 唯一权威 | 其他形式 |
|---|---|---|
| phase/action/holdout 授权 | AuthorityStore | receipt、gate report 是审计引用 |
| run/cycle/access/usage/budget/lease | OperationalJournal | status、Ledger、Memory 是 projection |
| 科学 claim | content-addressed Learning Packet + commit event | factor library、摘要是 projection |
| 数据字节 | 生产 CSV source of truth | raw parquet/indicator/signal 均为 derived cache |
| 数据身份 | GenerationManifest + Campaign read lease | 历史 manifest 只审计，不能假装仍可读取旧字节 |
| 配置身份 | typed ProjectSettings + frozen manifest | Prompt 中的配置文字不是 enforcement |
| 合同 | ContractRegistry version | JSON Schema 由合同生成，不手写平行版本 |
| 历史 Runner 输出 | `legacy_unaudited` raw evidence | 不得直接进入 Ledger/Memory/Promotion |
| 控制台输出 | diagnostic logging | 不能充当 audit event 或科学证据 |

### 3.3 主数据流

```text
exact phase authorization
        │
        ▼
EntryGuard ──X── undeclared entry/effect/path
        │
        ▼
ProtocolCompiler + IdentityBundle + Generation read lease
        │
        ▼
ContextProjection → ContextAssembler → Invocation/Usage
        │                                  │
        └──────── proposal/work item ──────┘
                           │
                           ▼
MutationTransaction in disposable workspace
                           │
                           ▼
Runner raw artifacts + Access/Derivation events
                           │
                           ▼
RunnerArtifactAdapter → EvidenceAdapter
                           │
             ┌─────────────┼────────────────────┐
             ▼             ▼                    ▼
       invalid/audit   NO_MATERIAL_FINDING   valid scoped claim
             │             │                    │
             └──────► round event      Learning Packet → commit event
                                                  │
                                                  ▼
                                       Ledger/Memory projections
                                                  │
                                                  ▼
                                    next-cycle mechanical decision
```

## 4. 技术选型与依赖边界

### 4.1 复用

- SQLite：AuthorityStore、OperationalJournal、事务 CAS、幂等和增量 projection。
- `jsonschema`：兼容已有 KBase Draft 2020-12 合同。
- `pydantic` / `pydantic-settings`：typed contracts、JSON Schema、YAML/env settings、SecretStr。
- `tenacity`：同步/异步逻辑 retry、stop/wait/predicate/callback。
- `psutil`：PID + process create time 的 lease 身份，防 PID reuse。
- `tiktoken` 与 AG2 tokenizer：已知模型的 context 估算；未知模型明确 ESTIMATED。
- 现有 `semantic_index.py` / `catalog_builder.py` 发布流程、`lineage.py` ancestry、`RawParquetCache`、`git apply` 和隔离 workspace。

P1 必须生成并锁定最小 control-plane 依赖清单；“本机已安装”不能作为可复现保证。预计直接依赖包括 Pydantic、pydantic-settings、jsonschema、Tenacity、psutil、tiktoken，以及当前实际使用的 AG2/OpenAI SDK 版本。

### 4.2 不进入核心路径

- DVC：仅允许以后做独立 prototype，不接管日更 CSV。
- MLflow：以后最多是只写 export Adapter，不做第二事实源。
- Prefect、Temporal、Celery、RQ、APScheduler：不把单机科研循环变成通用平台。
- OpenLineage：以后最多导出，不承担 taint/authorization。
- Casbin、Oso、JWT、通用 FSM：不替代一次性 capability 与 SQLite CAS。
- SQLAlchemy/Alembic：当前小型、显式 schema 不值得增加抽象。
- LangChain/LlamaIndex：不接管 scoped memory、taint 或 Prompt enforcement。
- file-age lock、strategy 级独占锁、全仓 Typer/Click 迁移、Loguru/structlog 审计、orjson 权威 canonical hash。

## 5. 分阶段实施

任何阶段都不能自动进入下一阶段。每阶段均要求：RED 测试、最小 GREEN、重构检查、完整回归、文件 allowlist 差异、独立 review、gate report 和新的用户命令。因为 P1-P8 都会合法修改源码，所以每个阶段都必须执行“全部代码完成 → code freeze → final inventory → 人工审阅 entry-policy delta → immutable policy → generic gate”；P0 的 policy 不能被误当成永久有效。

### P0R2：重新闭合入口与授权根

目标：修复 P0R1 的结构性冲突，形成可信且可执行的 P0 gate。P0R1 的 T1/T2 代码仅作为 provisional diff 重新验证，不能直接继续 T3。

任务：

1. 生成 `P0-CR-002`、V3.4.2 plan/scope/instruction-policy hashes；冻结本计划、AGENTS、allowlist 和 threat model。
2. 定义统一 `control_plane.task_report.v2` JSON Schema；T1/T2 原报告保持不变，通过 hash-bound adoption/normalization report 接入。V2 至少固定 phase/task/attempt/authorization、identity binding、allowed/forbidden files、baseline、test receipts、review findings、changed/unexpected files、external invocations、side effects、computed outcome 和 self-hash；`changed_files` 永远是数组。P0 使用 sealed stdlib validator，不新增第三方依赖；P1 的 typed reader 必须兼容此冻结 schema，不能静默改权威语义。
3. 对 P0R1 T1/T2 做 provisional adoption：先校验原报告 hash，再重跑合同、inventory 和 import-safety 测试；T1 代码可复验接纳，T2 的 368 条 inventory 仅作 initial delta baseline，旧 policy 永不满足 final gate。
4. 定义可信 bootstrap/provision/claim 生命周期：固定 AuthorityStore 路径、一次初始化、secret 仅通过 stdin/匿名管道传递、原文不写文件/环境变量/日志。公共 hash 只能标识内容，不能授权；phase/actor/invocation/attempt/hash 不一致时拒绝且不消费。
5. 抽取最小 durable transaction implementation；Authority adapter 仍只以 `mode=rw` 打开固定预置库。P0R2 同时 provision 最小 OperationalJournal，用于 task/audit/outbox mirror；P3 只扩展 access/lineage schema，不再创建第三个 store。
6. 完成一次性的通用 PhaseGateBuilder/Verifier/Closer，不再为 P1-P8 各写 gate engine。调用方不能传入 PASS；TaskReport outcome 也由受控 builder 根据 receipts、ticket、review 和 scope delta 计算。
7. 完成 subprocess、patch、repair、KBase、Registry、Snapshot、Handoff、自动循环、Web 和 notification 的最深公共 sink guard。
8. 完成 import seam 与外部 Windows Scheduler inventory；不修改 scheduler/ACL/运行进程。
9. 所有 P0 代码冻结后，重新扫描完整入口面；scanner 只生成 candidate，独立 reviewer 发布 immutable policy 到 `research_state/control_plane/policies/<sha256>.json`。现有源码目录下的 `entry_policy.json` 只保留为 P0R1 provisional evidence，不再是 active authority。
10. 运行最终 gate，确认无开放/失败/IN_DOUBT ticket、无越界变更、`auto_advance=false`。

P0R2 必须一次性预定义 P0-P8、CAMPAIGN、FINAL_EVAL 和已知 SideEffect 的 authority 语义。Authority bootstrap/broker/store/schema/gate verifier/durable transaction core 在 P0 PASS 后成为 sealed TCB（可信计算基）；P1-P8 不得修改这些文件。若后来发现必须修改，必须走 `P0-CR`、新 P0 attempt 和完整 P0 gate，不能只刷新当前 phase policy。

P0R2 验收：

- 三次独立冷启动导入，每次不超过 5 秒且无 side effect。
- 公共 hash、任意 dataclass、调用方选择 DB 路径、旧 token、未知 schema 均不能产生 authority。
- 并发 claim 只有一个成功；崩溃后状态为可审计 `IN_DOUBT`，不能静默重试。
- 最终 inventory 在代码冻结之后生成，所有 policy-bound 文件 hash 精确匹配。
- 外部开发模型每个批次均有 roster manifest、provider/base-url/model/config/prompt hash 和 nullable usage；同一调用不能在多个 task report 重复计数。usage 固定为 REPORTED/ESTIMATED/UNKNOWN，缺失字段为 null。
- `authority_outbox` 清空；Journal mirror 失败不能通过重复消费 capability 修复。
- PASS 后仍必须是 `P1_AUTHORIZED=false / CAMPAIGN_AUTHORIZED=false / FINAL_EVAL_AUTHORIZED=false / AUTO_ADVANCE=false`。

通用 gate CLI 固定为：

```text
python -m research_automation.control_plane.cli gate preflight --phase P0 --attempt-id <id>
python -m research_automation.control_plane.cli gate build --phase P0 --attempt-id <id> --freeze-manifest <path> --inventory <path> --entry-policy <path> --scheduler-inventory <path> --task-report-id <id> --output <path>
python -m research_automation.control_plane.cli gate verify --phase P0 --attempt-id <id> --report <path> --read-only
python -m research_automation.control_plane.cli gate close --phase P0 --attempt-id <id> --report <path> --capability-stdin
```

退出码：`0=verified PASS`、`2=valid computed FAIL`、`3=malformed/corrupt evidence`、`4=authority/identity mismatch`、`5=store unavailable/IN_DOUBT`。`preflight` 可重复且只读；正式 FAIL 后必须创建新的 attempt，不能覆盖报告。

回滚：只允许 scoped reverse patch 或删除本阶段新建的临时测试/报告；不得 `git reset --hard`、不得清除已消费授权、不得修改用户已有工作树变更。

### P1：合同、设置、身份和冻结协议

目标：让后续模块共享一个版本化合同和配置源，不再各写 schema/hash/config。

任务：

1. 在不修改 sealed authority contracts 的前提下建立 Research ContractRegistry v2；Pydantic 模型使用 strict/frozen/extra-forbid 并生成 Draft 2020-12 JSON Schema，未知字段和未知未来版本 fail-closed，旧 P0/KBase 合同通过显式 legacy Adapter 读取。TaskReport 的 typed reader 必须兼容 P0 冻结 schema，但不能替换 Authority validator。
2. 深化 `ResearchConfig` 为 typed ProjectSettings：YAML + env、provider identity、transport、base URL、request/response model、retry、pricing version、tokenizer、路径和预算统一验证；secret 使用 SecretStr 且禁止序列化。导入配置不能自动 `load_dotenv`、修改 `os.environ` 或改全局 logging；只读 inspect 可在缺 secret 时工作，真实 invocation 前 fail。
3. 统一 ArtifactIdentity：将 locator 与 content identity 分开。path/mtime/root 只属于 ArtifactLocator；content SHA-256、schema、producer、generation 和 logical role 构成 identity。相同内容换路径不改 artifact_id，内容变化必须改 ID；历史 KBase hash 使用 legacy profile，不改历史 ID。
4. ProtocolCompiler 生成冻结 ExecutionSpec：label/horizon/exit、train/validation/fold-test、rolling folds、purge/embargo、universe/calendar、特征边界、参数/模型/阈值、runner/code、GPU backend、输出 schema 和允许 side effects。
5. MaterialChangeClassifier 对 label、fold、gate、runner、特征、参数、数据 role 等变动返回 `IDENTICAL / IMMATERIAL_ALLOWLISTED / APPROVED_AMENDMENT / MATERIAL_UNAPPROVED`；自生成 preregistration hash 不能自证批准。
6. 冻结最小 dependency lock 和 provider capability manifest；运行时不得自动 pip install。

P1 gate：canonical/hash/schema/property tests、unknown-version fail-closed、SecretStr 泄漏测试、provider drift、协议歧义、非有限浮点、Windows path/junction、旧 hash 兼容和 ExecutionSpec 稳定性全部通过。

### P2：Immutable Release、Data Generation 与 Cache Manifest

目标：在不复制 84G 数据的前提下，保证运行期间数据不变、日更发布不出现半状态、研究/生产 cache 不串用。

任务：

1. 从 KBase `semantic_index.py` / `catalog_builder.py` 提取 ImmutableReleaseStore：stage、manifest/hash validation、current/previous、atomic promote、recovery、rollback。KBase 和 Generation 各用 Adapter。
2. GenerationManifest 绑定 CSV cutoff、calendar、point-in-time universe、adjustment scheme、missing policy 和 CacheManifest。
3. 运行获得 generation 共享读租约；日更获得发布写租约。读租约存在时，日更可以下载、staging 和校验，但不能替换 live CSV 或移动 CURRENT；状态明确为 `PUBLISH_PENDING`。
4. Campaign 全程绑定一个 generation。若日更 pending，当前 cycle 完成后暂停，不再启动下一 cycle；释放租约并发布后，如需继续必须创建明确的 campaign revision/new campaign，不能静默换 generation。
5. 统一 CacheManifest Interface：generation、source identity、feature contract、calendar/universe、adjustment、research/production namespace。Parquet、indicator、signal pickle 保留各自格式。
6. pinned path 只验证实际触达 artifact；禁止 routine `rglob` 和全量 rehash。legacy unpinned path 可暂时保留 mtime 行为，但不能产可信 evidence。
7. 明确缺失日语义：`PRESENT / NO_BAR_CONFIRMED / UNKNOWN_NO_BAR / FETCH_FAILED`。只有来源确认才标 SUSPENDED；任何 no-bar/fetch-failed 行均不能产生 signal、model、entry 或 exit 特征。组合估值允许单独 `STALE_VALUATION` 字段并标 stale，不能回流模型。
8. 保留 GBK、CSV 倒序、raw parquet 升序、checkpoint retention、生产/研究 cache 分离和现有 no_today_bar 合法性。

P2 gate：staging crash、租约竞争、pending publish、uncontrolled mutation、cache identity、research isolation、suspended/no-bar、fetch failure、disk full、CURRENT recovery 和旧 release hash 兼容测试通过。若修改 `backtest_optimized.py`，先取得用户对精确验证命令/参数/日期/输出路径的批准，再运行 AGENTS 要求的 known-good backtest。

### P3：OperationalJournal、Access、Lineage 与 Taint

目标：建立与 AuthorityStore 隔离的运行事实源，并追踪所有可能影响科学结论或 Prompt 的数据访问。

任务：

1. 在 P0R2 已 provision 的固定 OperationalJournal 上执行版本化 schema migration，增加 access/derivation/projection 表；仍由同一 owner 管理 sequence、idempotency、actor/invocation 和 checkpoint。
2. 各领域保留独立枚举/迁移表；复用 expected-state CAS、append event、IN_DOUBT 和 terminal enforcement，不建立巨型 FSM。
3. 数据 role 固定为 `TRAIN / VALIDATION / FOLD_TEST / FINAL_HOLDOUT / LIVE_FORWARD`。
4. 记录 `READ / MATERIALIZE / DERIVE / DISPLAY / CONSUME / EXPORT`；事件只存 bounded metadata/ref/hash，禁止 DataFrame、raw log 和 secret。
5. 将现有 `lineage.py` 的 ancestry/cycle/root 算法接入统一 derivation edge；TaintGraph 是 edge/event 的 projection，不建立第二套持久图。
6. taint 至少包含 `CLEAN / TEST_LABEL / TEST_DERIVED / FINAL_HOLDOUT / INVALID`；DISPLAY/CONSUME 会把 taint 传播到 Prompt、Memory 和下游 claim。
7. 每个冻结 candidate/protocol/fold 的 FOLD_TEST attempt 在读取前写入唯一消费记录；测试结果不得用于同一 fold 的阈值、模型或变体选择。
8. Final Holdout 不在 P3 开放；其 nonce/attempt 只由 P8 AuthorityBroker 管理。

P3 gate：并发 append、sequence、reopen/replay、lineage cycle、test-derived 指标、Prompt exposure、fold-test replay、数据库损坏、projection rebuild 和 AuthorityStore 物理隔离测试通过。

### P4：Mutation、Runner Artifact、Evidence 与 Learning Commit

目标：只让冻结协议下、隔离 workspace 中产生并经独立核验的证据成为 Learning。

任务：

1. 深化现有 PatchExecutor 为 MutationTransaction：
   - GitDiffAdapter 使用 `git apply --check` 后应用，禁止 `--unsafe-path`；
   - StructuredAstAdapter 保留确定性常量修改；
   - 共用路径 containment、文件 allowlist、before/after hash、compile/selected tests 和 actor audit；
   - 失败通过丢弃隔离 workspace 回滚，不逐文件猜测恢复；
   - 删除仅验证脚本使用的自写 hunk fallback。
2. 每个已知 runner 建立严格、版本化 RunnerArtifactAdapter；现有宽松 stdout/Markdown/CSV parser 只输出 legacy raw evidence。
3. EvidenceAdapter 独立验证 artifact hash/schema、label/exit/horizon、日期覆盖、fold role、purge/embargo、model artifact、feature set、threshold、generation、runner/code、行数/缺失率、access/taint 和 executed-vs-approved spec。
4. Runner 自报 `PASS`、`promotion_gate_passed` 或 scientific outcome 永远不是权威。
5. Evidence 采用正交字段：protocol conformance、evidence grade、scientific outcome、promotion eligibility、commit eligibility 和 invalidation codes；`NO_MATERIAL_FINDING` 是 round outcome，不是空 claim。
6. Learning Packet 支持 POSITIVE、NEGATIVE、PARTIAL、anti-factor 和 failed-usage-mode，必须携带 scope、证据 refs、audit grade、taint、parent lineage、reopen predicate 和 future-usage suggestion。
7. Packet 先以 content hash exclusive-create + fsync，再 append idempotent commit event；崩溃留下的 orphan packet不进入 Ledger，由 reconciler 报告/收养，不能双计。Ledger 按 last_sequence 增量投影。
8. 为 2026-07-23/24 历史结果生成 hash-bound audit addendum；不改原文件、不宣称 unseen/OOS、不批量导入旧 Runner 结果。

P4 gate：生产目录 byte-identical、unsafe path、compile/test failure、workspace discard、unknown runner、semantic mismatch、tainted metric、false PASS、packet tamper、crash window、duplicate commit、projection rebuild、negative/PARTIAL claim 和 raw-log exclusion全部通过。

### P5：Scoped LearningGate 与唯一 Context Projection

目标：下一轮只读取安全、相关、可解释的经验，并正确利用负面结论而不是全局封杀。

任务：

1. ClaimScope 至少包含 mechanism、usage mode、market regime、time window、universe、liquidity bucket、label/protocol family 和 data generation family。
2. LearningGate 机械计算 `EXACT / SUBSET / OVERLAP / DISJOINT`：
   - exact execution identity 可 hard-block；
   - semantic similarity 只 warning；
   - PARTIAL 只对 scope 交集生效；
   - disjoint 不阻塞。
3. 冲突分级：`REPRODUCIBILITY_FAILURE / SCOPE_OR_PROTOCOL_CONFLICT / DATA_DRIFT_CONFLICT / LEGACY_EVIDENCE_CONFLICT`，记录 resolution owner 和 actor event。
4. `universal_factor_rejection` 默认 false，只能由多 scope 严格证据机械派生，手工 true 直接拒绝。
5. reopen 由 predicate evaluator 判定；允许新机制、不同 usage、不同 regime/time/universe/liquidity、数据漂移、新证据等级或已声明 research gap，禁止人工布尔绕过。
6. ContextProjection 是 V3.4 新路径唯一输入；MemoryRouter、compile_project_state、recent-file/rglob 只允许一次性 legacy Adapter，不得成为热路径。
7. ContextAssembler 生成角色化结构，保留 claim、scope、grade、taint、invalidation、reopen 和 lineage；按相关性/信息增益确定性压缩。每模型每 cycle：Memory ≤1500 tokens，控制元数据 ≤500 tokens。超限返回 `CONTEXT_BUDGET_EXCEEDED`，不得静默截掉关键字段。
8. 已知 tokenizer 使用 tiktoken/AG2 Adapter；未知 tokenizer 标 ESTIMATED 并保守计数，不使用 `len(text)`。
9. KBase/raw source 作为 data 引用和结构化字段进入 Prompt，不能携带授权/控制指令；加入 prompt-injection fixture，模型文本永远不能生成 capability。
10. 负面 learning 可以产生 avoid、soft-penalty、anti-factor、regime-conditional 或 future-usage 建议；未经严格 forward validation 不得升级为 production hard gate。

P5 gate：scope 交并、PARTIAL、conflict/reopen、parent invalidation、prompt injection、taint exclusion、large Ledger、token overflow、确定性输出、无 recent-file scan 和 negative-learning 建议测试通过。

### P6：Invocation/Usage 与唯一 Campaign Controller

目标：提供可预算、可恢复、自动吸收上一轮结果的多轮循环，不引入通用工作流平台。

任务：

1. 建立 ModelInvocation Module，提供 AG2 Classic、OpenAI-compatible direct call 和 CLI Adapter。
2. SDK 内部 retry 设为 0；Tenacity 是唯一逻辑 retry owner。每次 attempt 在解析文本/JSON 前先落 UsageEnvelope，非法 JSON、空响应、fallback、timeout 和 exception 都必须有事件。
3. UsageEnvelope 记录实际 provider/profile/request model/response model、call/attempt、cache/reasoning tokens、reported/estimated cost、outcome、stream/fallback 状态和 raw-usage hash。数值缺失用 null + `REPORTED / ESTIMATED / UNKNOWN`，严禁 0 伪装未知。
4. 预算在调用前按最大输入/输出和价格上限原子 reserve；调用后 settle。usage UNKNOWN 时保留全部 reservation，保证不会穿透预算。streaming 在完成中断/partial-usage 测试前保持禁用。
5. Campaign work item、cycle、budget、roster、lease、resume 和 pause 全部写 OperationalJournal；不新增 queue.json。ExperimentTask 仅作输入 DTO。
6. Campaign/Cycle 状态显式：`DRAFT → FROZEN → AUTHORIZED → RUNNING → PAUSE_REQUESTED/PAUSED/BLOCKED/COMPLETED → CLOSED`。AG2 stage/source-item checkpoint 保留在 Adapter 内，Campaign 不理解其内部步骤。
7. 每个 cycle 固定顺序：reserve → build safe context → freeze proposal/spec/roster/generation → execute → evidence → learning commit → settle → information-gain decision → next cycle。
8. roster manifest 绑定 provider/profile/model/role/prompt/config/capability hash。科学 roster 缺少 required member 或发生 model drift 时 BLOCKED，不静默缩表/换模型。
9. budget 覆盖 token、API cost、wall time、cycle count、tool attempts、data exposure、disk growth；CapitalTracker 只保留 channel allocation/ROI/information-gain projection，不再计算固定 3000-token 或独立硬预算。
10. cycle lock 使用 campaign_id/cycle_id + fencing token；heartbeat 使用 host、PID、process create time、monotonic sequence。stale 不能只看 mtime。共享 projection 只用短事务。
11. pause/STOP 变成 `PauseRequested` Adapter，只在安全 cycle boundary 生效；不得停止当前 subprocess 或用户已启动的长任务。
12. dry-run 使用独立 namespace/预算，但运行同一 protocol/scope/duplicate/conflict 预检；不写正式 Packet/Ledger/Registry，多次 preview 不污染正式事实源。
13. `run_research.py` 成为唯一 CLI；`run_research_cycle.py` 删除 return 后不可达实现，只保留兼容参数翻译/迁移提示。AutonomousRunner、AutomationController、EvolutionLoop、sqnav autorun/backlog、TaskQueue/status/STOP 均为 `legacy_unaudited` Adapter。

P6 gate：usage success/missing/fallback/timeout/invalid JSON、double-retry、unknown cost、budget concurrency、illegal transition、duplicate cycle、roster drift、lease fencing、crash resume、pause boundary、dry-run isolation、legacy quarantine 和离线双轮“上一轮 Packet 进入下一轮 Context”集成测试通过。实施期间不调用真实 LLM、不运行真实 Campaign。

### P7：长跑运维、审计、留存与 canary 准备

目标：证明长期运行不会逐轮变慢、膨胀、双计或失去可观测性。

任务：

1. OperationalJournal 使用 WAL/显式 busy timeout/单 writer 短事务；AuthorityStore 保持低频、fail-closed 配置。
2. projection 按 last_sequence 增量更新；全量 rebuild 只用于离线恢复，并有进度、阈值和 integrity check。
3. 使用 SQLite backup API 生成一致性 operational backup；AuthorityStore 损坏时 fail-closed，禁止自动重建/清空。
4. `run_research.py status|audit|doctor|export` 只读：显示 campaign/cycle、budget、lease、roster、generation、evidence grade、access、usage status、pending data publish 和失败原因；不能启动任务或打开 holdout。
5. 生成确定性 audit bundle（manifest + hashes + refs），排除 secret、raw labels、Final Holdout 和无关大文件。
6. 历史 backfill 只实现 rate-limited、分片、低优先级、可暂停 Adapter；本阶段不实际批量回填。
7. Learning Packet/commit 增加 retention_class、last_referenced_at、archive_eligible；只实现标记和报告，不自动移动科学证据。临时 preview/staging 按明确 TTL 清理，真实 packet 不按年龄删除。
8. 冲突/阻塞状态输出人类可读解释和直接 evidence refs。
9. 建立性能基线与回归门禁。

P7 gate：100k synthetic events 增量 status/projection、crash/backup/restore、disk-full、low-disk-next-cycle block、lease false-reap、audit export redaction、backfill throttle/pause、retention safety 和 CLI read-only 测试通过。仍不自动启动真实 Campaign。

### P8：Trusted Evaluator 与不可逆关闭

目标：在完全冻结后一次性评估 Final Holdout，并阻止任何基于它的继续调参或自动循环。

任务：

1. FinalEvalRequest 绑定 campaign、candidate set、code、ExecutionSpec、feature/model/threshold、roster、generation、holdout 和 actor hashes。
2. `AUTHORIZE_FINAL_EVAL` nonce 存 AuthorityStore；AuthorityBroker 在返回任何 handle 前先永久消费 attempt。失败、超时、崩溃都不能重试。
3. TrustedEvaluator 使用独立低权限 data-root Adapter；Research Runner、AG2、Prompt 和 Memory 永远得不到 raw path、labels 或可重构数据。
4. result 只包含结构化指标和 evidence refs；不进入同一研究计划的 proposal selection 或 Memory。
5. 成功或失败均追加 terminal audit 并使 Campaign CLOSED；所有自动 cycle 请求被拒绝。
6. live-forward 是新的未推广 run，和 Final Holdout 结果分离。生产 promotion 永远需要人工审批。

P8 gate：候选未冻结、wrong hash、wrong actor、path traversal、nonce replay、crash-after-consume、failed-attempt consumed、closed-campaign resume、Prompt/LLM access、manual-only promotion 和 audit export 测试通过。实现 gate 明确证明没有读取真实 Final Holdout。

## 6. 跨阶段测试矩阵

```text
授权 receipt
  ├── forged/public-hash/replay/concurrent/crash                 [P0]
  ▼
协议 + identity + settings
  ├── schema drift/path/time/float/secret/provider drift         [P1]
  ▼
generation + cache
  ├── pending publish/mutation/no-bar/research isolation         [P2]
  ▼
access + lineage + taint
  ├── fold-test once/display/consume/derived contamination       [P3]
  ▼
workspace mutation + runner artifacts + evidence
  ├── traversal/compile fail/false PASS/unknown schema           [P4]
  ▼
packet + journal + projection
  ├── crash window/tamper/duplicate/rebuild                       [P4]
  ▼
scope gate + context
  ├── PARTIAL/conflict/reopen/prompt injection/token overflow     [P5]
  ▼
invocation + campaign
  ├── usage missing/retry/fallback/budget/roster/lease/resume     [P6]
  ▼
ops
  ├── incremental rebuild/backup/disk/audit/backfill/retention    [P7]
  ▼
Final Holdout
  └── consume-before-access/replay/crash/CLOSED/manual promotion  [P8]
```

每个 Phase 的测试层次：

1. pure contract/property tests；
2. SQLite/file crash-injection tests；
3. Adapter fixture tests；
4. phase integration tests；
5. 全部 `python -m unittest discover -s tests -p "test_*.py" -v`；
6. allowlist filesystem diff 和 gate self-verification。

控制面改动不得擅自运行真实回测。若确实修改 backtest/signal/parameter handling，必须先把精确参数、日期、范围、cache/output 路径提交用户批准；随后执行 known-good backtest 和两值 sanity check，验证不同参数结果不同。

## 7. 关键失败模式与处理

| 失败 | 机械处理 | 用户可见结果 |
|---|---|---|
| 普通 writer 尝试写 AuthorityStore | fixed-path/adapter deny | `AUTHORITY_WRITE_DENIED` |
| P0 代码后改导致 inventory stale | 最终 inventory 必须在 code freeze 后重建 | gate FAIL，不进入 P1 |
| Packet 已写、commit event 未写时崩溃 | orphan 不投影，reconciler 报告/幂等收养 | `ORPHAN_PACKET_PENDING` |
| budget reserve 后调用结果未知 | 保留全部 reservation | usage=UNKNOWN，预算不会穿透 |
| Provider/response model 漂移 | roster identity mismatch | Campaign BLOCKED，不换模型 |
| active Campaign 遇到日更 | 日更 staging；当前 cycle 完成；下一 cycle 暂停 | `DATA_PUBLISH_PENDING` |
| 外部程序原地改数据 | touched-artifact hash/size/mtime mismatch | `GENERATION_MUTATED`，证据无效 |
| 停牌或当日无 bar | 不生成 signal/entry/exit；估值字段单独 stale | 明确 missing_reason |
| stale lease 遇 PID reuse | PID + create time + fencing 校验 | 不误回收活进程 |
| Context 超预算 | 确定性压缩失败后显式返回 | `CONTEXT_BUDGET_EXCEEDED` |
| KBase/source prompt injection | 结构化 data、无 capability、sink gate | 指令被视为数据，不能授权 |
| Runner 自报有效 | EvidenceAdapter 忽略布尔值并重算 | `EVIDENCE_INVALID` 或真实 verdict |
| dry-run 重复 | 独立 namespace，不写正式事实 | preview 可重复，正式去重不污染 |
| OperationalJournal 损坏 | integrity check + backup restore；无法恢复则 fail-closed | `OPERATIONAL_STORE_UNAVAILABLE` |
| disk 低于 reserve | 阻止下一 cycle，不杀当前任务 | Campaign PAUSED/BLOCKED |
| Final Eval 读取后崩溃 | attempt 已永久消费并关闭 | `FINAL_EVAL_CONSUMED_FAILED` |

## 8. 性能与 token 预算

1. steady-state 禁止全仓 `rglob`、全量历史 JSON 重写和 84G routine rehash；只处理本轮事件与 touched artifacts。
2. projection 更新复杂度为 O(new events)；status 查询使用 projection/checkpoint，不扫描 Packet 全目录。
3. SQLite 每个逻辑事件一次短事务；高频 access/usage 可 bounded batch，不按 token/row 写事件。
4. 每角色每 cycle 的 Learning Memory ≤1500 tokens，控制元数据 ≤500 tokens；raw log/report/data 不进入 Prompt。
5. reserve-before-call 保证预算上限；UNKNOWN 不会导致低估。
6. P7 记录并门禁：event append p95、10k Packet context build、100k event status/rebuild、CLI cold import 和 campaign bookkeeping 比例。控制面 steady-state wall-time overhead 目标不超过 cycle 总耗时 5%；超过即 gate FAIL 或要求 change request。
7. 无真实 load 数据时使用可重复 synthetic benchmark，保存机器/磁盘/Python/SQLite 版本，避免跨机器伪比较。
8. complexity budget：禁止再增加第三个权威数据库、第二个 gate engine、第二个 Campaign queue、第二个 usage ledger 或新的 file-lock lease。任何新持久 artifact 必须同时声明 owner、schema、retention、rebuild/repair 和 deletion test，否则不得进入计划。
9. 新 Module 必须满足二者之一：拥有不可替代的领域规则，或至少有两个真实 Adapter 证明 Seam 存在；单一调用方的 pass-through Module 应删除/内联。

## 9. 并发开发与主控复核

阶段 gate 仍串行；阶段内部可并发：

| Phase | 可并发 lane | 必须串行 |
|---|---|---|
| P0R2 | sink guards、Web guards、report-schema tests | durable authority contract → code freeze → final inventory → gate |
| P1 | settings、protocol fixtures、legacy hash inventory | ContractRegistry/identity owner 最终合并 |
| P2 | release-store extraction、data semantics tests、cache Adapter tests | interface freeze → daily publication integration |
| P3 | journal/access tests、lineage Adapter tests | journal schema freeze → taint projection |
| P4 | MutationTransaction、RunnerArtifactAdapter/Evidence | Evidence verdict → Learning commit integration |
| P5 | scope/conflict rules、context fixtures | claim schema freeze → final ContextAssembler |
| P6 | Invocation/usage、Campaign core、legacy CLI Adapter | usage schema + budget semantics → integration |
| P7 | status/audit/export、backup/rebuild、benchmarks | final ops gate |
| P8 | 不并发安全核心 | 全部顺序、双人/双模型 review |

外部火山模型池（排除 DeepSeek）与 DeepSeek 官方 API 可在不共享文件的 lane 中并行给出实现 proposal/patch。约束：

- 每个 batch 先冻结 development roster manifest；
- 外部模型不持有 Authority secret、不运行 research/backtest/data update、不直接改主 workspace；
- 每个 patch 由主 Codex 逐行 review、在隔离 workspace 应用、跑 RED/GREEN/完整回归后才能合并；
- 相同文件不得并行编辑；共享合同 owner 单写；
- quota 用尽时，按用户既有授权由主 Codex接手，记录 `takeover_reason=QUOTA_EXHAUSTED`，不得偷偷缩小范围；
- 科学 Campaign roster 与开发 roster 完全分离。开发时可接手，不代表科学 roundtable 可以静默替换 required member。

## 10. 防漂移执行约束

1. 每个 TaskTicket 绑定 plan/scope/instruction-policy hash、active entry-policy digest、allowed files、allowed effects、expected tests、rollback 和 evidence path。
2. 用户原始参数、日期、样本、step、阈值、validation design 是 spec；任何改变先停在 task boundary，提交 Change Request。
3. 不修改 `set_param/reset/rollback`，除非用户单独明确授权。
4. 不改变生产策略、信号、模型、参数、production config、data bytes、KBase content、Registry、Snapshot、Handoff 或 Memory，除非对应后续 phase ticket 明确列出。
5. 不自动 phase advance；失败 phase 的 retry 需要新命令和 failed_attempt id。
6. 不停止用户已运行任务；预算/磁盘/更新请求只阻止下一 cycle。
7. 外部 LLM 结论只作 proposal；主 Codex 对设计、代码、测试和 gate 负责。
8. 不使用 Git reset/checkout 清理用户变更；不 stage/commit/push，除非用户另外授权。
9. Python 源码不加入 Unicode emoji。
10. P0 sealed TCB 后续只读；任何触碰都自动使当前 phase gate无效并要求重新打开 P0。

## 11. 明确不在本轮范围

- 重构整个项目目录、迁移/删除 84G 数据、改变 checkpoint retention 参数。
- 多租户、远程服务、容器化、Web UI/dashboard、移动端。
- 把 daily production scheduler 纳入 Campaign。
- 全仓迁移 Typer/Click、SQLAlchemy/Alembic、统一 logging 风格。
- 自动冷存储迁移；只做 eligibility 标记。
- 实际批量历史回填；只做 rate-limited 工具和测试。
- LLM streaming；完成 usage/中断合成测试前保持关闭。
- 自动 production promotion。
- 防御同一管理员账户下的恶意本地代码；需要 OS 级隔离的二期方案。

## 12. 授权与 rollout

批准后先物化以下计划文档，尚不实施代码：

```text
docs/superpowers/plans/2026-07-26-v342-index.md
docs/superpowers/plans/2026-07-26-v342-00-p0r2.md
docs/superpowers/plans/2026-07-26-v342-01-foundations.md
docs/superpowers/plans/2026-07-26-v342-02-data-generation.md
docs/superpowers/plans/2026-07-26-v342-03-access-lineage.md
docs/superpowers/plans/2026-07-26-v342-04-evidence-learning.md
docs/superpowers/plans/2026-07-26-v342-05-memory.md
docs/superpowers/plans/2026-07-26-v342-06-campaign.md
docs/superpowers/plans/2026-07-26-v342-07-operations.md
docs/superpowers/plans/2026-07-26-v342-08-final-evaluator.md
docs/superpowers/plans/2026-07-26-v342-p0-change-request-002.md
```

manifest 必须给出每个文件 hash、全局 plan/scope/instruction-policy hash、覆盖关系和旧 P0R1 superseded 状态。

### 12.1 计划/Phase 授权

1. 用户批准变更请求：

   `APPROVE_CHANGE_REQUEST id=P0-CR-002`

2. 主控把本草案物化为仓库计划文件，生成并展示新 plan/scope/policy hashes 与 Authority receipt。
3. 用户启动 P0R2：

   `START_IMPLEMENTATION phase=P0 plan_version=V3.4.2-P0R2 plan_hash=<shown_hash>`

4. 每个后续 Phase 都需要新的 `START_IMPLEMENTATION phase=P<n> plan_hash=<same_or_amended_hash>`。
5. 失败重试必须使用：

   `RETRY_IMPLEMENTATION phase=P<n> failed_attempt=<id> plan_hash=<hash>`

### 12.2 Campaign rollout

P6/P7 代码 gate 通过不等于允许真实 Campaign。

1. C0：离线 fixtures + fake clock/PID/provider，至少 20 cycle chaos simulation。
2. C1：单独授权 real-LLM dry-run，只验证 roster/usage/context/预算，不 Commit 科学 Learning。
3. C2：单独授权 2-cycle canary，无 Final Holdout；检查第二轮是否只吸收第一轮有效 Learning。
4. C3：审计 canary 后，用户可授权多小时 Campaign：

   `AUTHORIZE_CAMPAIGN campaign_id=<id> max_cycles=<n> max_wall_time_s=<n> budget_id=<id> authorization_nonce=<nonce>`

5. 长 Campaign 遇日更 pending 时在 cycle boundary 暂停；不能静默切 data generation。
6. 候选冻结并停止研究后，才允许单独 Final Eval：

   `AUTHORIZE_FINAL_EVAL holdout_id=<id> authorization_nonce=<one_time_nonce>`

## 13. Definition of Done

计划实现完成需要同时满足：

1. P0R2-P8 每个 gate 都有 self-hashed PASS report，且无 unresolved P0/P1 安全问题。
2. AuthorityStore 与 OperationalJournal 物理隔离并通过越权测试。
3. 旧 Runner、TaskQueue、autorun、status、MemoryRouter 不再是 V3.4 权威热路径。
4. Packet/journal/projection crash matrix 无丢失、无双计、无 raw-log/taint 泄漏。
5. pinned generation 期间日更不能发布；未控制 mutation 会使 evidence fail-closed。
6. 使用量可以是 UNKNOWN，但不能丢事件、伪装为 0 或穿透预算。
7. 离线双轮测试证明上一轮 scoped Learning 会进入下一轮，而 invalid、NO_MATERIAL_FINDING、Final Holdout 和 tainted 结果不会进入。
8. 2-cycle canary 经单独授权、审计通过后，才宣称“多轮闭环可运行”；仅测试通过不能宣称科学有效。
9. Final Eval 仍需独立一次性授权；任何 PASS 都不自动修改生产策略。

## 14. 当前正确下一步

本计划尚未授权实施。当前不要继续 P0R1-T3，也不要重用旧 `START_IMPLEMENTATION phase=P0`。

如果用户认可本计划，下一条控制命令应为：

`APPROVE_CHANGE_REQUEST id=P0-CR-002`

随后主控只执行“物化计划、生成 hashes、建立新 authorization receipt”这一步；仍不会自动开始代码实施。
