# Recursive Data Contract Evolution Registry

递归数据契约演进注册服务。契约以 JSON 描述，支持具名 `record` / `enum`、带标签
`union`、容器 `array` / `map`、原始类型 `int` / `long` / `string` / `bytes`，
以及**本地递归引用**。发布内容先**规范化（canonicalize）**再取 **SHA-256 指纹**，
以 **CAS（内容寻址存储）** 分配**不可变版本**；注册时按 subject 的兼容性策略做
reader/writer 校验，失败返回历史版本、规则和最短字段路径。

此外支持**跨 subject 联动发布**：把多个 subject 的目标版本与 producer→consumer
数据流边打包成不可变发布候选，服务在同一历史快照下校验并生成确定性分批升级计划，
确认时以**全局 CAS** 保证所有 subject 原子前进。候选指纹同时纳入各参与 subject
的**快照信息**（最新版本号、该版本指纹、兼容模式），基线漂移后重提同一请求会
生成**新候选与新计划**，旧候选保留可查但只能按 CAS 失败。

还支持**兼容策略迁移**：为一个或多个 subject 提交目标兼容模式，服务在**同一
历史快照**中按版本顺序**重放**每个 subject 的演进 —— 普通模式逐版核对其**前一
最新版**，`*_TRANSITIVE` 模式逐版核对**全部旧版**，找出切换到目标模式会**新增
冲突**的版本对（有序、带方向、规则与最短路径），并保存为**不可变迁移候选**：
重放无冲突的候选为 `planned`（可确认），存在冲突的候选为 `blocked`（永久保存、
可查询但不可确认）。确认时按 subject id 顺序锁定，以基线版本、基线指纹与原模式
做 **CAS**：任一漂移即整体返回 409 且不改任何配置；否则在单事务内原子切换全部
模式，重复确认幂等。策略切换会使此前按旧快照规划的联动发布候选按其原有 CAS
语义失效（`compatibility_drift`）。

此外还有**消费者绑定与漂移影响**：客户端用幂等键把「producer subject + 基线
版本 + reader 契约」登记为**不可变绑定**（服务在同一历史快照校验 reader 必须
能读取基线版本写出的数据）。之后可随时查询漂移影响：服务把 reader 契约按版本
顺序**重放**到基线之后的每一个 producer 版本上，返回读不了的版本（规则 + 最短
字段路径）与**首个失败版本**，并把绑定状态置为 `impacted`。每次评估落盘为
**不可变的报告修订**（快照未变则复用最新修订），删除或修改 subject 不会改写
历史报告；修订号可作为 CAS 令牌防止并发评估互相覆盖。

所有写入口（版本注册、配置变更、联动发布确认、策略迁移确认、消费者绑定与影响
报告修订）都在**同一数据库事务**内向**不可变事件日志**追加事件：事件带**单调
全局序号**、类型、subject、版本或指纹摘要与**规范化 payload**（canonical JSON）。
客户端用 `POST /event-subscriptions` 登记 subject / 事件类型过滤，按**游标分页**
并以 `snapshot_seq` 截止读取，提交 `checkpoint` 后可**断点同步**或把游标归零
**重放重建本地索引**。全局序号由单行计数器在事务内 `UPDATE ... RETURNING` 分配，
行锁持有到提交 —— 序号即提交顺序、无空洞无重复，并发写入不漏不重，事务回滚
不会留下事件；事件按 subject **名字**引用而非外键，删除或重建 subject 仍保留
完整历史。

技术栈：Python 3.12 · FastAPI · PostgreSQL（SQLAlchemy 2 + psycopg 3）。

---

## 一键启动（Docker Compose）

```bash
docker compose up --build
```

- API: <http://localhost:8000>
- 交互式文档（Swagger UI）: <http://localhost:8000/docs>
- PostgreSQL: `localhost:5432`（用户/密码/库均为 `contracts`，数据持久化在 `pgdata` volume）

Compose 会等待 Postgres 健康检查通过后再启动 API；API 启动时自动建表（含事件
日志与订阅表，并初始化全局序号计数器）。启动后即可：

1. 通过既有写入口产生变更（注册版本、改配置、确认联动发布/迁移、建绑定、评估影响）；
2. `POST /event-subscriptions` 登记订阅（subject / 事件类型过滤，空为全部）；
3. `GET /event-subscriptions/{id}/events` 按游标分页重放事件（`snapshot_seq` 截止），
   `POST /event-subscriptions/{id}/checkpoint` 提交断点 —— 详见下文
   「契约变更事件订阅与断点重放」。

## 本地开发

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
export DATABASE_URL="postgresql+psycopg://contracts:contracts@localhost:5432/contracts"
uvicorn app.main:app --reload
```

运行测试（测试自动使用临时 SQLite，无需外部依赖）：

```bash
pytest
```

迁移并发确认的回归测试（`tests/test_migration_concurrency_pg.py`，以及暂存库
归属协议的边界测试 `tests/test_pg_scratch_ownership.py`）需要真实的
PostgreSQL 行级锁（`SELECT ... FOR UPDATE`）与集群级对象语义，不可达
PostgreSQL 时自动跳过。要运行它们，先用 Compose 起数据库，再用
`TEST_DATABASE_URL` 指向**数据库服务器**（默认值就是 Compose 的连接串，
因此不设该变量直接运行效果相同）：

```bash
docker compose up -d db
TEST_DATABASE_URL="postgresql+psycopg://contracts:contracts@localhost:5432/contracts" \
    pytest tests/test_migration_concurrency_pg.py tests/test_pg_scratch_ownership.py
```

**回归不会触碰 `TEST_DATABASE_URL` 指向的库。** 该连接串只用于定位服务器
（登录后可连接任意库，如 `contracts` 或 `postgres`）；测试在其上自建专用
暂存库 `contracts_concurrency_test`，走可重复的「建库 → 清理 → 销毁」流程。
所有建表与清表都只发生在这个暂存库内 —— 因此默认配置、或把
`TEST_DATABASE_URL` 误指到 Compose 业务库 `contracts`，都不会对业务库建表
或清表。注意不要把 `TEST_DATABASE_URL` 直接指向暂存库名本身（会被明确
拒绝）。登录角色需要 CREATE DATABASE 权限（Compose 的 `contracts` 用户是
超级用户，开箱即用）。

### 暂存库的归属核验（为什么同名库不会被误删）

**同名不等于归属**：服务器上可能已存在别人的 `contracts_concurrency_test`，
其中甚至可能有同名为 `subjects` 的表，但行并不属于本套件。因此套件**不会
只凭表名**就 DROP 或 TRUNCATE。归属由 `tests/pg_scratch.py` 中的三重、可
独立核验的凭证证明，接管判定全部发生在一把**集群级咨询锁**
（`pg_advisory_lock`，按暂存库名取键，运行期间由专用连接持有）之下：

1. **数据库对象归属注释**（`COMMENT ON DATABASE`）：`CREATE DATABASE` 之后
   立刻写入，专门覆盖「建库成功、归属标记尚未写入即崩溃」的中断窗口；
2. **库内归属标记表** `_contracts_suite_owner`：单行 JSONB，记录套件标识、
   每次运行唯一的 `run_id`、单调递增的**运行代际 `generation`**、持有进程的
   后端 PID 与心跳时间；后台线程在运行期间持续刷新心跳（标记表不在用例间
   TRUNCATE 范围内，因此跨用例始终可核验）；
3. **集群咨询锁 + 活跃心跳/PID**：并行启动在咨询锁上串行化；锁在有界等待内
   拿不到，即明确报「仍被其他运行占用」并失败，绝不接管。

接管同名库的判定规则（任一条不满足都在任何破坏性语句**之前**明确失败，原数据
保留）：

| 同名库现状 | 处理 |
| --- | --- |
| 无归属注释（哪怕表名/表结构与本套件完全一致、且含数据） | **失败，保留**：无法证明归属 |
| 有本套件注释，但含非本套件的表，或标记中的套件标识不匹配 | **失败，保留**：归属冲突 |
| 有本套件标记，但心跳新鲜或标记中的后端 PID 仍存活 | **失败，保留**：其他运行正在使用 |
| 有本套件标记，心跳已陈旧 **且** 后端 PID 已消失（上次运行残留） | 安全接管：DROP 后重建，`generation` +1 |
| 有本套件注释但无标记（建库与写标记之间崩溃留下的空壳） | 安全接管：DROP 后重建 |
| 库不存在 | 全新创建 |

TRUNCATE（用例间清理）与会话结束的 DROP 之前还会再次核验「当前连接库名 +
套件标识 + 本次 `run_id`」；若核验时库已不属于本次运行，则放弃破坏性操作并
保留数据。确属本套件、可安全接管的隔离库照常支持**重复运行、用例间清理与
结束销毁**。

### 暂存库的退出销毁（外部连接绝不被强制断开）

会话结束（`lease.release()` / `with` 退出）时，销毁条件比「仍归本运行所有」
更严格，依次为：

1. 先关闭**本运行自己的连接**（应用引擎连接池与 owner/心跳后端），但继续持有
   集群级咨询锁；
2. 在咨询锁下重新核验归属（数据库注释 + 库内 marker 的 `run_id` 仍指向本次
   运行），核验失败则明确报错并保留库与数据；
3. 检查暂存库里是否还有**外部客户端会话**（或绑定到该库的 prepared
   transaction）。检查本身失败、或仍有任何外部会话占用，都**明确报错并保留库
   及全部数据**——套件从不使用 `DROP DATABASE WITH (FORCE)`，因此外部客户端
   不会被强制踢断、正在使用的库不会被删除；
4. 确认无人占用后，只执行**非强制**的普通 `DROP DATABASE`。这样「检查 →
   DROP」之间的竞争窗口里若恰好有新客户端连入，PostgreSQL 自身会以 55006
   拒绝 DROP，套件把该失败转为明确报错并保留库与数据。

咨询锁一直持有到销毁完成或拒绝完成之后才释放；因此销毁失败留下的状态（注释与
marker 的 `run_id` 仍有效、锁已释放）可由**下次运行安全判定**：外部连接还在时
接管会被同样拒绝；连接全部离开且心跳陈旧后，即按上一节的陈旧残留规则代际 +1
接管。测试覆盖了真实外部客户端占用退出、正常空闲销毁、检查→DROP 竞争窗口、
拒绝后下次运行的判定，并保留既有的同名归属保护与并行租约有界等待。

这些用例用屏障（barrier）精确控制两个并发 confirm 的交错，覆盖：同一候选的
并发确认收敛到同一已提交结果（双方均 200 且响应一致）、不同候选争用时恰好一个
提交（另一个 409 且整体回滚），以及多 subject 候选在竞争下的原子性（败者不在
任何 subject 上留下切换）。归属协议的边界测试另覆盖：同名外来库（含完整本套件
表结构与外来行）在拒绝后行与表结构原样保留、建库/写标记之间的中断、陈旧残留的
代际递增接管、并行启动的有界等待失败与数据保留、用例间清理不破坏标记、心跳刷新、
结束销毁，以及**退出时外部客户端仍占用暂存库（含检查→DROP 竞争窗口）绝不强制
断连、明确报错并保留库与数据，空闲库正常非强制销毁，拒绝后由下次运行安全判定**。

## 契约语言

类型可以是：

| 形式 | 说明 |
| --- | --- |
| `"int"` `"long"` `"string"` `"bytes"` | 原始类型（也可写 `{"type": "int"}`） |
| `{"type": "record", "name": N, "fields": [...]}` | 具名记录；字段 `{id, name, type, default?}` |
| `{"type": "enum", "name": N, "symbols": [...]}` | 具名枚举 |
| `{"type": "union", "branches": [{"tag": T, "type": ...}]}` | 带标签联合，tag 在联合内唯一 |
| `{"type": "array", "items": T}` / `{"type": "map", "values": T}` | 容器 |
| `{"type": "ref", "name": N}` 或简写 `"N"` | 引用本文档内定义的具名类型（支持递归） |

要点：

- **字段按稳定 `id` 对齐**，不按名字或顺序；因此字段重命名是兼容变更。
- 字段**无 `default` 即为必读（required）**。
- 具名类型在文档内全局唯一；`ref` 实现本地递归（如链表、树）。
- 默认值按类型校验：record 默认值为按字段名索引的对象；union 默认值为
  单键对象 `{<tag>: <value>}`。

递归契约示例（链表）：

```json
{
  "type": "record",
  "name": "Node",
  "fields": [
    {"id": 1, "name": "value", "type": "int"},
    {"id": 2, "name": "next", "type": {"type": "ref", "name": "Node"}}
  ]
}
```

## 规范化、指纹与 CAS

发布内容先规范化再取指纹，语义相同 ⇒ 指纹相同 ⇒ 同一版本：

- 对象键序、record 字段顺序（按 id 排序）、enum 符号顺序、union 分支顺序均被归一；
- 简写形式归一：`"int"` ≡ `{"type": "int"}`，`"Node"` ≡ `{"type": "ref", "name": "Node"}`；
- 具名类型在确定性遍历中首次出现处内联，之后以 `ref` 表示（递归文档因此有限）。

指纹 = 规范化文档（UTF-8）的 SHA-256。重复发布相同内容返回已存在的版本
（HTTP 200, `"created": false`），不会新增版本 —— 注册是幂等的。

## 兼容性规则

`check(reader, writer)` 判定「writer 写出的数据能否被 reader 读取」：

| 规则 | 说明 | 违反时的 rule |
| --- | --- | --- |
| 原始类型提升 | 仅允许 writer `int` → reader `long` | `primitive_type_mismatch` |
| 字段对齐 | 按稳定 id 对齐；reader 可忽略 writer 多余字段 | — |
| 新增必读字段 | reader 有而 writer 无的字段必须带默认值 | `required_field_missing_default` |
| 枚举覆盖 | reader 符号集必须覆盖 writer 全部符号 | `enum_symbols_not_covered` |
| union 映射 | 每个 writer 分支按 tag **唯一**映射到兼容的 reader 分支 | `union_branch_missing` |
| 容器递归 | array 元素、map 值递归校验 | （内层规则） |
| 类型种类 | record/enum/union/array/map/原始类型种类须一致 | `type_kind_mismatch` |

递归校验以**已访问 (reader, writer) 类型对**终止循环（余归纳：再次到达同一
类型对即视为兼容）。

### 模式

| 模式 | reader | writer | 核对范围 |
| --- | --- | --- | --- |
| `BACKWARD`（默认） | 新版本 | 历史版本 | 仅最新版 |
| `FORWARD` | 历史版本 | 新版本 | 仅最新版 |
| `FULL` | 双向 | 双向 | 仅最新版 |
| `*_TRANSITIVE` | 同上 | 同上 | **全部历史版本** |

### 失败响应

注册冲突返回 HTTP 409，`detail.errors` 按历史版本逐条给出：冲突版本号、
方向、规则、**最短字段路径**（路径长度并列时按字典序取最小）及人类可读信息：

```json
{
  "detail": {
    "message": "schema is not compatible under the subject's BACKWARD_TRANSITIVE policy",
    "errors": [
      {
        "version": 1,
        "direction": "backward",
        "rule": "required_field_missing_default",
        "path": ["owner"],
        "path_string": "$.owner",
        "message": "reader field 'owner' (id 4) is required but absent from the writer contract and declares no default"
      }
    ]
  }
}
```

### 并发语义

版本号按 subject 单调递增；`(subject, version)` 与 `(subject, fingerprint)`
均有唯一约束。**同一基线上的并发注册只有一项成功**：失败者收到 409，需基于
新的最新版本重新校验后重试。并发发布相同内容时，CAS 使双方幂等收敛到同一版本。

联动发布的确认是**全局 CAS**：候选快照之后任一参与 subject 的最新版本（及其指纹）
或兼容模式发生变化，confirm 即整体拒绝且不写入任何版本；并发 confirm 同一候选时
双方幂等收敛到同一结果。并发创建候选时，同一快照上的重复提交由唯一指纹约束幂等
收敛；若创建期间快照被推进，服务在有界重试内重新读取快照并重新校验、重新规划，
不会返回绑定旧基线的过期候选。

兼容策略迁移确认采用同一套**全局 CAS**：确认事务先以 `SELECT ... FOR UPDATE`
锁定**候选行**并在锁下重读其状态，再按 subject id 顺序锁定 subject 行做基线校验。
快照之后任一 subject 的最新版本、指纹或模式漂移，即整体 409 且不切换任何模式。
并发确认**同一**迁移候选时，后到者阻塞在候选行锁上，待胜者提交后读到已提交状态，
幂等复用同一确认结果 —— 不会把对方已完成的模式切换误判为 `compatibility_drift`；
并发确认**不同**迁移候选时仍按基线 CAS 判定漂移，恰好一个提交，另一个整体回滚并
收到 409。确认提交的模式切换以**带条件的 UPDATE**（仅要求当前模式等于快照模式；
不因目标模式已相同就推定本候选已提交）执行，因此在不提供行锁的数据库（SQLite）
上也不会漏掉并发改写。

## API 一览

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /subjects` | 创建 subject（`{name, compatibility}`，默认 `BACKWARD`） |
| `GET /subjects` / `GET /subjects/{name}` | 列表 / 详情（含版本） |
| `PUT /subjects/{name}/config` | 修改兼容性模式 |
| `DELETE /subjects/{name}` | 删除 subject 及其版本 |
| `POST /subjects/{name}/versions` | 注册新版本（201 新建 / 200 CAS 命中；409 不兼容或并发冲突；422 契约非法） |
| `GET /subjects/{name}/versions` | 版本列表 |
| `GET /subjects/{name}/versions/{v}` | 版本详情（`v` 可为 `latest`），含原始内容、规范化文档与指纹 |
| `POST /subjects/{name}/check` | 干跑校验：只检查不注册 |
| `POST /releases` | 创建联动发布候选（201 新建 / 200 内容哈希在同一快照上命中；基线漂移后重提生成新候选；404 subject 不存在；409 无推进、策略冲突、边不兼容或依赖环；422 请求非法） |
| `GET /releases` / `GET /releases/{fp}` | 候选列表 / 详情（批次、每批边、前置条件：基线版本+基线指纹+兼容模式、确定性计划；旧候选在基线漂移后仍可查询） |
| `POST /releases/{fp}/confirm` | 全局 CAS 确认发布（200 提交或幂等重放；409 版本/指纹/兼容模式漂移，未写入任何内容） |
| `POST /migrations` | 创建兼容策略迁移候选：一个或多个 subject 的目标模式（201 新建 / 200 同一快照内容哈希命中；`status` 为 `planned` 或 `blocked`；404 subject 不存在；409 目标模式与现状相同（无推进）；422 请求非法） |
| `GET /migrations` / `GET /migrations/{fp}` | 迁移候选列表 / 详情（重放冲突对、快照前置条件、状态；旧候选在基线漂移后仍可查询） |
| `POST /migrations/{fp}/confirm` | 全局 CAS 确认迁移（200 原子切换全部模式或幂等重放；409 候选 `blocked`，或版本/指纹/模式漂移，未改任何配置） |
| `POST /consumer-bindings` | 创建消费者绑定：`{producer_subject, baseline_version, reader, idempotency_key}`（201 新建 / 200 同键同内容幂等命中；404 producer 或基线版本不存在；409 幂等键冲突或 reader 与基线不兼容；422 reader 契约非法） |
| `GET /consumer-bindings/{binding_id}` | 绑定详情（reader 契约、基线指纹、状态 `active`/`impacted`、报告修订数） |
| `GET /consumer-bindings/{binding_id}/impact` | 漂移影响：按版本顺序重放 reader 契约，返回受影响版本（规则+最短路径）与首个失败版本；快照移动时生成新报告修订，否则复用（`created: false`）；`?expected_revision=N` 为乐观 CAS 令牌（409 不匹配）；producer 被删除/替换后返回历史报告（`producer_intact: false`），无历史报告则 409 |
| `POST /event-subscriptions` | 登记事件订阅：`{subjects: [], event_types: []}`（空列表表示不过滤；事件类型见下），201 返回 `subscription_id` 与 `checkpoint: 0` |
| `GET /event-subscriptions/{id}` | 订阅详情（过滤条件、当前 checkpoint、创建时间） |
| `GET /event-subscriptions/{id}/events` | 游标分页读取：`?after_seq=N`（缺省用订阅 checkpoint）、`?limit=M`（默认 100，上限 1000）、`?upto_seq=S`（钉住快照截止）；返回 `snapshot_seq`、`events[]`、`next_after_seq`、`has_more` |
| `POST /event-subscriptions/{id}/checkpoint` | 提交断点 `{seq: N}`（`0` 表示从头重放以重建本地索引；超出当前 `snapshot_seq` 返回 409 `checkpoint_beyond_snapshot`） |
| `GET /health` | 健康检查 |

## 兼容性示例（curl）

```bash
B=http://localhost:8000

# 1. 创建 subject（传递式向后兼容）
curl -s -X POST $B/subjects -H 'Content-Type: application/json' \
  -d '{"name": "users", "compatibility": "BACKWARD_TRANSITIVE"}'

# 2. 注册 v1：递归链表契约
curl -s -X POST $B/subjects/users/versions -H 'Content-Type: application/json' -d '{
  "content": {
    "type": "record", "name": "Node",
    "fields": [
      {"id": 1, "name": "value", "type": "int"},
      {"id": 2, "name": "next", "type": {"type": "ref", "name": "Node"}}
    ]
  }
}'
# => {"subject":"users","version":1,"fingerprint":"a600...","created":true}   201

# 3. 幂等重发：字段顺序打乱、简写展开，语义相同 -> 命中 CAS，仍是 v1
curl -s -X POST $B/subjects/users/versions -H 'Content-Type: application/json' -d '{
  "content": {
    "type": "record", "name": "Node",
    "fields": [
      {"id": 2, "name": "next", "type": "Node"},
      {"id": 1, "name": "value", "type": {"type": "int"}}
    ]
  }
}'
# => {"version":1,"created":false}   200

# 4. 兼容演进：int 提升为 long；新增带默认值的可选字段 -> v2
curl -s -X POST $B/subjects/users/versions -H 'Content-Type: application/json' -d '{
  "content": {
    "type": "record", "name": "Node",
    "fields": [
      {"id": 1, "name": "value", "type": "long"},
      {"id": 2, "name": "next", "type": {"type": "ref", "name": "Node"}},
      {"id": 3, "name": "label", "type": "string", "default": ""}
    ]
  }
}'
# => {"version":2,"created":true}   201

# 5. 不兼容：新增必读字段 owner 无默认值 -> 409，逐历史版本给出规则与最短路径
curl -s -X POST $B/subjects/users/versions -H 'Content-Type: application/json' -d '{
  "content": {
    "type": "record", "name": "Node",
    "fields": [
      {"id": 1, "name": "value", "type": "long"},
      {"id": 2, "name": "next", "type": {"type": "ref", "name": "Node"}},
      {"id": 3, "name": "label", "type": "string", "default": ""},
      {"id": 4, "name": "owner", "type": "string"}
    ]
  }
}'
# => 409  errors: [{version:1, rule:"required_field_missing_default", path:["owner"]},
#                  {version:2, ...}]            （TRANSITIVE 核对全部历史版本）

# 6. 干跑校验（不注册）
curl -s -X POST $B/subjects/users/check -H 'Content-Type: application/json' -d '{
  "content": {"type": "record", "name": "Node", "fields": [
    {"id": 1, "name": "value", "type": "string"},
    {"id": 2, "name": "next", "type": {"type": "ref", "name": "Node"}}]}
}'
# => {"is_compatible":false,"errors":[{version:1, rule:"primitive_type_mismatch", ...},
#                                     {version:2, ...}]}
```

union 与 enum 示例：

```bash
curl -s -X POST $B/subjects -H 'Content-Type: application/json' \
  -d '{"name": "payments", "compatibility": "FULL"}'

# v1：带标签 union
curl -s -X POST $B/subjects/payments/versions -H 'Content-Type: application/json' \
  -d '{"content": {"type": "union", "branches": [
        {"tag": "card", "type": "string"},
        {"tag": "cash", "type": "int"}]}}'

# 删除 card 分支 -> 409 union_branch_missing（每个 writer 分支须唯一映射到 reader 分支）
curl -s -X POST $B/subjects/payments/versions -H 'Content-Type: application/json' \
  -d '{"content": {"type": "union", "branches": [{"tag": "cash", "type": "long"}]}}'

# enum：reader 必须覆盖 writer 全部符号；收缩符号集会报 enum_symbols_not_covered
curl -s -X POST $B/subjects -H 'Content-Type: application/json' -d '{"name": "colors"}'
curl -s -X POST $B/subjects/colors/versions -H 'Content-Type: application/json' \
  -d '{"content": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN"]}}'
curl -s -X POST $B/subjects/colors/versions -H 'Content-Type: application/json' \
  -d '{"content": {"type": "enum", "name": "Color", "symbols": ["RED"]}}'   # 409
```

## 跨 subject 联动发布

当一次升级涉及多个 subject 时，可以把「多个 subject 的目标版本 + producer→consumer
数据流边」打包成一个**不可变发布候选**，由注册服务统一规划、原子发布：

1. `POST /releases` 提交候选。服务在**同一历史快照**下：
   - 用各 subject 自己的兼容策略校验其目标契约（与单 subject 注册同一套规则）；
   - 校验每条数据流边：**consumer 的目标契约必须能读取 producer 目标契约写出的数据**；
   - 由边构建有向依赖图，生成**确定性升级计划**：批次数量最少（同批 subject 可同时
     切换），批内按 `(subject, version)` 字典序排列。
2. 候选以**内容哈希 + 历史快照**幂等保存：指纹除覆盖目标契约与边集外，还纳入
   每个参与 subject 在规划时刻的快照 —— 最新版本号、该版本的内容指纹及兼容模式。
   - 快照未变时重提语义相同的请求，返回同一候选（HTTP 200, `"created": false`），
     计划随候选固化；
   - 任一参与 subject **注册了新版本**或**修改了兼容模式**后，用相同目标契约和
     producer→consumer 边重提，会得到一个**新候选（新指纹）与绑定新基线的新计划**
     （HTTP 201, `"created": true`）。旧候选不会被覆盖或删除，仍可 `GET` 查询，
     但其 confirm 只能按全局 CAS 返回 409（见下文「基线漂移后的重放」）。
3. `POST /releases/{fingerprint}/confirm` 确认发布，采用**全局 CAS**：候选快照之后
   **任一**参与 subject 的最新版本（或该版本指纹）、兼容模式发生变化，即整体拒绝
   （409）且**不写入任何版本**；否则所有 subject 在**一个事务**内同时前进到目标版本。
   重复 confirm 幂等返回同一结果。
4. `GET /releases/{fingerprint}` 查询计划：每批切换的 subject、该批满足的数据流边、
   以及 confirm 所需满足的前置条件（各 subject 的基线版本）。

校验失败均返回 409 且不落库：

| 情形 | `detail` 关键字段 |
| --- | --- |
| 目标契约违反 subject 自身策略 | `subjects[].errors[]`（版本、方向、规则、最短路径） |
| 数据流边不兼容 | `edges[]`：producer/consumer 及各自目标版本、规则、最短路径 |
| 依赖图有环 | `rule: "cyclic_dependency"` + `cycle[]`：**最短冲突环**（含每个节点的目标版本） |
| 某 subject 内容已注册（无推进） | `subjects[].existing_version` |
| confirm 时版本漂移 | `kind: "version_drift"` + `conflicts[]`：`expected_baseline` vs `actual_latest` |
| confirm 时基线指纹被替换 | `kind: "fingerprint_drift"`：`expected_fingerprint` vs `actual_fingerprint` |
| confirm 时兼容模式被修改 | `kind: "compatibility_drift"`：`expected_compatibility` vs `actual_compatibility` |

现有单 subject API 完全保留，两种方式可以混用；单 subject 注册或模式修改导致联动
候选失效时，重提请求会基于新基线生成新候选，旧候选的 confirm 返回 409 及冲突明细。

### 基线漂移后的重放（修复后的行为）

```bash
B=http://localhost:8000

# 1. 成功创建候选（orders/payments 都在 v1 基线上，目标 v2）
curl -s -X POST $B/releases -H 'Content-Type: application/json' -d '{
  "subjects": {
    "orders":   {"content": {"type": "record", "name": "Order", "fields": [
      {"id": 1, "name": "order_id", "type": "int"},
      {"id": 2, "name": "amount", "type": "long"},
      {"id": 5, "name": "currency", "type": "string", "default": "USD"}]}},
    "payments": {"content": {"type": "record", "name": "Payment", "fields": [
      {"id": 1, "name": "payment_id", "type": "int"},
      {"id": 2, "name": "amount", "type": "long"},
      {"id": 5, "name": "currency", "type": "string", "default": "USD"}]}}
  },
  "edges": [{"producer": "orders", "consumer": "payments"}]
}'
# => 201  fingerprint: fp-old

# 2. 任一参与 subject 发生基线变化：注册新版本，或修改兼容模式
curl -s -X POST $B/subjects/orders/versions -H 'Content-Type: application/json' -d '{
  "content": {"type": "record", "name": "Order", "fields": [
    {"id": 1, "name": "order_id", "type": "int"},
    {"id": 2, "name": "amount", "type": "long"}]}}'
# （等价地：PUT /subjects/orders/config 把 BACKWARD 改成其他模式也会改变快照）

# 3. 用完全相同的目标契约和边重提 -> 新候选、新计划（绑定新基线）
curl -s -X POST $B/releases -H 'Content-Type: application/json' -d '{ ...同上... }'
# => 201  created: true   fingerprint: fp-new（与 fp-old 不同）
#    plan.preconditions 中 orders 的 baseline_version 已变为 2，
#    并带有 baseline_fingerprint；orders 的 target_version 变为 3

# 4. 旧候选保留可查，但其确认只能失败（全局 CAS）
curl -s $B/releases/fp-old                       # => 200，status 仍为 planned
curl -s -X POST $B/releases/fp-old/confirm
# => 409 {"detail": {"message": "release candidate is stale: ...",
#                    "conflicts": [{"subject": "orders",
#                                   "kind": "version_drift",
#                                   "expected_baseline": 1,
#                                   "actual_latest": 2}]}}

# 5. 新候选在新基线上确认成功；重复确认幂等
curl -s -X POST $B/releases/fp-new/confirm       # => 200 committed
```

并发说明：多个 `POST /releases` 在同一快照上竞争时由候选唯一约束保证幂等收敛；
若创建过程中基线恰好被其他请求推进，服务会重新读取快照并重算计划（有界重试），
不会返回绑定旧基线的过期候选。

### 联动发布示例（curl）

```bash
B=http://localhost:8000

# 1. 两个 subject：orders 是 payments 的上游（payments 消费 orders 的数据）
curl -s -X POST $B/subjects -H 'Content-Type: application/json' -d '{"name": "orders"}'
curl -s -X POST $B/subjects -H 'Content-Type: application/json' -d '{"name": "payments"}'
curl -s -X POST $B/subjects/orders/versions -H 'Content-Type: application/json' -d '{
  "content": {"type": "record", "name": "Order", "fields": [
    {"id": 1, "name": "order_id", "type": "int"},
    {"id": 2, "name": "amount", "type": "int"}]}}'
curl -s -X POST $B/subjects/payments/versions -H 'Content-Type: application/json' -d '{
  "content": {"type": "record", "name": "Payment", "fields": [
    {"id": 1, "name": "payment_id", "type": "int"},
    {"id": 2, "name": "amount", "type": "int"}]}}'

# 2. 提交联动发布候选：amount 一起拓宽为 long，并声明数据流边 orders -> payments
curl -s -X POST $B/releases -H 'Content-Type: application/json' -d '{
  "subjects": {
    "orders":   {"content": {"type": "record", "name": "Order", "fields": [
      {"id": 1, "name": "order_id", "type": "int"},
      {"id": 2, "name": "amount", "type": "long"},
      {"id": 5, "name": "currency", "type": "string", "default": "USD"}]}},
    "payments": {"content": {"type": "record", "name": "Payment", "fields": [
      {"id": 1, "name": "payment_id", "type": "int"},
      {"id": 2, "name": "amount", "type": "long"},
      {"id": 5, "name": "currency", "type": "string", "default": "USD"}]}}
  },
  "edges": [{"producer": "orders", "consumer": "payments"}]
}'
# => 201
# {
#   "fingerprint": "9c1e…",
#   "status": "planned",
#   "created": true,
#   "plan": {
#     "batches": [
#       {"batch": 0, "subjects": [{"subject": "orders", "target_version": 2}], "edges": []},
#       {"batch": 1, "subjects": [{"subject": "payments", "target_version": 2}],
#        "edges": [{"producer": "orders", "consumer": "payments",
#                   "producer_version": 2, "consumer_version": 2}]}
#     ],
#     "preconditions": [
#       {"subject": "orders",   "baseline_version": 1,
#        "baseline_fingerprint": "a600…", "compatibility": "BACKWARD"},
#       {"subject": "payments", "baseline_version": 1,
#        "baseline_fingerprint": "7b12…", "compatibility": "BACKWARD"}
#     ],
#     "subjects": [...], "edges": [...]
#   }
# }

# 3. 查看计划（每批边、前置条件、确定性升级顺序）
curl -s $B/releases/9c1e…

# 4. 确认发布：全局 CAS —— 若 orders/payments 任一最新版本已变化则整体 409
curl -s -X POST $B/releases/9c1e…/confirm
# => {"fingerprint": "9c1e…", "status": "committed",
#     "versions": [{"subject": "orders",   "version": 2, "fingerprint": "…"},
#                  {"subject": "payments", "version": 2, "fingerprint": "…"}]}

# 5. 若第 2 步之后有人通过单 subject API 抢先注册了 orders v2：
#    confirm 返回 409，且 payments 不会被写入任何版本
# => {"detail": {"message": "release candidate is stale: ...",
#                "conflicts": [{"subject": "orders",
#                               "kind": "version_drift",
#                               "expected_baseline": 1, "actual_latest": 2}]}}
#    重新 POST /releases（相同请求体）会得到绑定 v2 基线的新候选，可正常确认。
```

## 兼容策略迁移（policy migration）

当需要把一个或多个 subject 的兼容策略收紧（例如从 `BACKWARD` 切换到
`BACKWARD_TRANSITIVE` 或 `FULL`）时，可以提交一个**迁移候选**，由服务重放历史、
评估影响并原子切换：

1. `POST /migrations` 提交每个 subject 的目标模式（`to_compatibility`）。服务在
   **同一历史快照**（所有参与 subject 共用一个快照）中：
   - 按版本号从小到大**重放**每个 subject 的演进；
   - 普通模式（`BACKWARD` / `FORWARD` / `FULL`）下，每一版只与其**前一最新版**
     （发布时的最新版）核对；`*_TRANSITIVE` 模式下，每一版与其**全部旧版**核对；
   - 找出切换到目标模式会**新增冲突**的有序**版本对** `(from_version,
     to_version)`，逐条给出方向（`backward`：新版读旧版数据；`forward`：旧版读
     新版数据）、违反规则与**最短字段路径**（并列取字典序最小）。首个版本没有
     前驱，永远不会出现在冲突对中。
2. 候选以**内容哈希 + 历史快照**不可变保存（与联动发布相同的快照语义）：
   - 指纹覆盖每个 subject 的 from/to 模式与快照（最新版本号、该版本指纹、当前
     模式）。快照未变时重提语义相同的请求返回同一候选（HTTP 200,
     `"created": false`）；任一 subject 注册了新版本或修改了模式后重提，会得到
     **新候选**（新指纹），旧候选保留可查但其 confirm 只能按 CAS 失败；
   - 重放**无冲突**的候选状态为 `planned`，可以确认；
   - 重放**存在冲突**的候选仍会保存，状态为 `blocked`，作为「哪些历史版本对在
     目标策略下不合法」的不可变审计记录。`blocked` 候选**不得确认**（409 返回
     其冲突对），也无法通过后续演进变为可确认 —— 历史不可变；
   - 目标模式与当前模式相同（无推进）时返回 409 且不落库。
3. `POST /migrations/{fingerprint}/confirm` 确认迁移，采用**全局 CAS**：先锁定
   候选行并在锁下重读其状态（并发确认同一候选时在候选行上串行化，后到者复用已
   提交结果），再按 subject id 顺序锁定，每个参与 subject 的最新版本（及其指纹）
   和当前模式都必须仍等于快照前置条件。**任一**漂移即整体拒绝（409，返回全部漂移
   明细）且**不修改任何配置**；否则所有 subject 在**一个事务**内原子切换到目标
   模式。重复 confirm 幂等返回同一结果。
4. 迁移提交只切换模式、不改动任何版本；切换后新注册的版本即按新模式校验。已有的
   **联动发布候选**不会被删除，但其快照包含旧的兼容模式，因此其 confirm 会按原有
   的快照语义返回 409（`compatibility_drift`），重提该发布会基于新模式重新规划。

冲突与失败响应一览：

| 情形 | HTTP | `detail` 关键字段 |
| --- | --- | --- |
| 重放发现冲突（候选已存为 `blocked`） | `POST` 201 | `plan.conflicts[]`：`subject`、`from_version`、`to_version`、`direction`、`rule`、`path`、`path_string` |
| 确认 `blocked` 候选 | 409 | `rule: "history_replay_conflicts"` + `conflicts[]`（同上） |
| 目标模式与当前模式相同 | 409 | `subjects[]`：`subject`、`compatibility` |
| confirm 时版本漂移 | 409 | `conflicts[]`：`kind: "version_drift"`，`expected_baseline` vs `actual_latest` |
| confirm 时基线指纹被替换 | 409 | `kind: "fingerprint_drift"`：`expected_fingerprint` vs `actual_fingerprint` |
| confirm 时模式被修改 | 409 | `kind: "compatibility_drift"`：`expected_compatibility` vs `actual_compatibility` |

### 策略迁移示例（curl）

```bash
B=http://localhost:8000

# 1. subject 默认 BACKWARD；注册三段历史：
#    v1 只有 a；v2 新增带默认值的 b；v3 保留 b 但去掉默认值。
#    每一步都向后兼容（v3 读取 v2 数据时 v2 仍有 b），所以 BACKWARD 下全部接受。
curl -s -X POST $B/subjects -H 'Content-Type: application/json' \
  -d '{"name": "users", "compatibility": "BACKWARD"}'

curl -s -X POST $B/subjects/users/versions -H 'Content-Type: application/json' -d '{
  "content": {"type": "record", "name": "R",
              "fields": [{"id": 1, "name": "a", "type": "int"}]}}'

curl -s -X POST $B/subjects/users/versions -H 'Content-Type: application/json' -d '{
  "content": {"type": "record", "name": "R", "fields": [
    {"id": 1, "name": "a", "type": "int"},
    {"id": 2, "name": "b", "type": "int", "default": 0}]}}'

curl -s -X POST $B/subjects/users/versions -H 'Content-Type: application/json' -d '{
  "content": {"type": "record", "name": "R", "fields": [
    {"id": 1, "name": "a", "type": "int"},
    {"id": 2, "name": "b", "type": "int"}]}}'

# 2. 申请迁移到 BACKWARD_TRANSITIVE：重放全部旧版 -> v3 无法读取 v1 数据
#    （v3 的 b 为必读且无默认值，v1 中不存在该字段）。候选被保存为 blocked。
curl -s -X POST $B/migrations -H 'Content-Type: application/json' -d '{
  "subjects": {"users": {"to_compatibility": "BACKWARD_TRANSITIVE"}}}'
# => 201
# {
#   "fingerprint": "4f2a…",
#   "status": "blocked",
#   "created": true,
#   "plan": {
#     "subjects": [{"subject": "users", "baseline_version": 3,
#                   "baseline_fingerprint": "…",
#                   "from_compatibility": "BACKWARD",
#                   "to_compatibility": "BACKWARD_TRANSITIVE"}],
#     "conflicts": [
#       {"subject": "users", "from_version": 1, "to_version": 3,
#        "direction": "backward",
#        "rule": "required_field_missing_default",
#        "path": ["b"], "path_string": "$.b",
#        "message": "reader field 'b' (id 2) is required but absent …"}
#     ],
#     "preconditions": [
#       {"subject": "users", "baseline_version": 3,
#        "baseline_fingerprint": "…", "compatibility": "BACKWARD"}]
#   }
# }

# 3. blocked 候选不得确认：409 原样返回冲突对，模式不变
curl -s -X POST $B/migrations/4f2a…/confirm
# => 409 {"detail": {"message": "migration candidate is blocked: …",
#                    "rule": "history_replay_conflicts",
#                    "conflicts": [{"from_version": 1, "to_version": 3, …}]}}
curl -s $B/subjects/users          # compatibility 仍是 "BACKWARD"

# 4. 改申请一个不引入冲突的目标模式（FULL 只核对最新版，v3 vs v2 双向兼容）。
#    相同请求提交两次：第一次 201 created=true，第二次命中 CAS（200, created=false）
curl -s -X POST $B/migrations -H 'Content-Type: application/json' -d '{
  "subjects": {"users": {"to_compatibility": "FULL"}}}'
FP=$(curl -s -X POST $B/migrations -H 'Content-Type: application/json' -d '{
  "subjects": {"users": {"to_compatibility": "FULL"}}}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["fingerprint"])')

# 5. 确认：全局 CAS —— 快照之后若 users 注册了新版本或改过模式，则整体 409
curl -s -X POST $B/migrations/$FP/confirm
# => {"fingerprint": "…", "status": "committed",
#     "subjects": [{"subject": "users", …, "to_compatibility": "FULL"}]}
# 再确认一次幂等返回同一结果；此后注册新版本按 FULL 校验
curl -s $B/subjects/users          # compatibility 已变为 "FULL"
```

多 subject 原子迁移（同一快照、同一事务）与 CAS 漂移示例：

```bash
# 一次提交多个 subject 的目标模式；全部重放无冲突 -> status: planned
curl -s -X POST $B/migrations -H 'Content-Type: application/json' -d '{
  "subjects": {
    "orders":   {"to_compatibility": "FORWARD"},
    "payments": {"to_compatibility": "FULL_TRANSITIVE"}
  }}'

curl -s -X POST $B/migrations/$FP/confirm
# => 200：orders 与 payments 在一个事务内同时切换
#
# 若确认前 payments 抢先注册了新版本（或有人 PUT 改了任一 subject 的模式）：
# => 409 {"detail": {"message": "migration candidate is stale: …",
#                    "conflicts": [{"subject": "payments",
#                                   "kind": "version_drift",
#                                   "expected_baseline": 2,
#                                   "actual_latest": 3}]}}
#    orders 也不会被切换；重提请求会基于新快照生成新候选（新指纹）。
```

## 消费者绑定与漂移影响（consumer bindings & drift impact）

下游消费者可以把「我读哪个 producer subject、从哪个基线版本读、用哪份 reader
契约」登记为一个**不可变绑定**，之后随时查询 producer 的演进是否影响自己：

1. `POST /consumer-bindings` 提交 `{producer_subject, baseline_version, reader,
   idempotency_key}`。服务在**同一历史快照**中校验并保存：
   - producer subject 与基线版本必须存在（否则 404）；
   - **reader 契约必须能读取基线版本写出的数据**（与注册同一套 reader/writer
     规则），否则 409 且不落库，`detail.errors[]` 给出规则与最短字段路径；
   - 绑定不可变：`binding_id` 是幂等键、producer、基线版本（含指纹）与规范化
     reader 契约的内容哈希；producer 只按**名字**引用，不用外键 —— 删除或重建
     subject 不会级联改写绑定或其报告。
2. **幂等键语义**：`idempotency_key` 全局唯一。
   - 同键同内容重提 → 返回已存绑定（HTTP 200, `"created": false`），即使
     producer 此后已演进、subject 已被删除，响应也不变；
   - 同键**异**内容（换了 producer、基线版本或 reader 契约）→ 409
     `idempotency_conflict`，不落库；
   - 不同键总是创建不同绑定。
3. `GET /consumer-bindings/{binding_id}/impact` 评估漂移影响：把绑定的 reader
   契约**按版本顺序重放**到基线之后的每一个 producer 版本（reader = 绑定契约，
   writer = producer 版本），返回：
   - `checked_versions`：本次重放覆盖的版本（基线之后的全部版本，有序）；
   - `affected_versions[]`：reader 读不了的版本，各带违反规则与**最短字段
     路径**（并列取字典序最小，与注册失败响应同一规则）；
   - `first_impacted_version`：首个失败版本；
   - 只要存在受影响版本，绑定状态即置为 `impacted`（黏性：历史只增不改，
     过去的失败不会消失）。
4. 每次评估落盘为**不可变的报告修订**（revision 从 1 单调递增）：
   - producer 最新快照（最新版本号 + 指纹）未变 → 复用最新修订
     （`"created": false`），不新增修订；
   - 快照已移动 → 基于当前快照生成**新修订**（`"created": true`），旧修订
     永久保留、绝不改写；
   - `?expected_revision=N` 是乐观 **CAS 令牌**（`0` 表示「还没有任何报告」）：
     与绑定当前最新修订号不一致即 409 `report_revision_conflict`，什么都不写；
     并发评估由绑定行锁（PostgreSQL）与 `(binding_id, revision)` 唯一约束
     （SQLite 下同样收敛）保证双方收敛到同一修订，不会互相覆盖。
5. **删除或修改 subject 不改写历史报告**：
   - `PUT /subjects/{name}/config` 改兼容模式不影响绑定与报告（影响快照只含
     版本号与内容指纹，重放结果与模式无关）；
   - `DELETE /subjects/{name}` 后绑定与全部修订照常可查；此时 `GET .../impact`
     返回**最新已存修订**作为历史记录（`"created": false,
     "producer_intact": false`），不再生成新修订；若绑定还从未生成过报告，
     则返回 409 `producer_missing`；
   - subject 被删除后以同名重建、且基线版本的内容指纹对不上（历史被替换）
     时，按同样的「历史记录」语义处理。

失败响应一览：

| 情形 | HTTP | `detail` 关键字段 |
| --- | --- | --- |
| 幂等键已用于不同内容 | 409 | `kind: "idempotency_conflict"` + `binding_id` |
| reader 与基线版本不兼容 | 409 | `kind: "baseline_incompatible"` + `errors[]`（版本、规则、最短路径） |
| producer subject / 基线版本不存在 | 404 | 消息文本 |
| reader 契约非法 | 422 | 消息文本 |
| `expected_revision` 与最新修订不符 | 409 | `kind: "report_revision_conflict"` + `expected_revision` vs `actual_revision` |
| producer 已删除/历史被替换且无历史报告 | 409 | `kind: "producer_missing"` |

### 消费者绑定示例（curl）

```bash
B=http://localhost:8000

# 1. producer subject：orders，注册 v1
curl -s -X POST $B/subjects -H 'Content-Type: application/json' -d '{"name": "orders"}'
curl -s -X POST $B/subjects/orders/versions -H 'Content-Type: application/json' -d '{
  "content": {"type": "record", "name": "Order",
              "fields": [{"id": 1, "name": "amount", "type": "int"}]}}'

# 2. 消费者绑定：从 v1 读起，reader 契约要求 amount 是 int
curl -s -X POST $B/consumer-bindings -H 'Content-Type: application/json' -d '{
  "producer_subject": "orders",
  "baseline_version": 1,
  "reader": {"type": "record", "name": "OrderView",
             "fields": [{"id": 1, "name": "amount", "type": "int"}]},
  "idempotency_key": "billing-svc-orders-v1"
}'
# => 201 {"binding_id": "7d3f…", "status": "active", "created": true, …}

# 幂等重提（同键同内容）→ 200，同一 binding_id；换内容复用同一键 → 409
curl -s -X POST $B/consumer-bindings -H 'Content-Type: application/json' -d '{ …同上… }'

# 3. 首次影响评估：还没有基线之后的版本
curl -s $B/consumer-bindings/7d3f…/impact
# => {"revision": 1, "created": true, "status": "active",
#     "result": {"checked_versions": [], "affected_versions": [],
#                "first_impacted_version": null}, …}
# 快照未变时再查 → 同一修订，"created": false

# 4. producer 演进到 v2：amount 拓宽为 long（BACKWARD 允许；int reader 读不了）
curl -s -X POST $B/subjects/orders/versions -H 'Content-Type: application/json' -d '{
  "content": {"type": "record", "name": "Order",
              "fields": [{"id": 1, "name": "amount", "type": "long"}]}}'

curl -s $B/consumer-bindings/7d3f…/impact
# => {"revision": 2, "created": true, "status": "impacted",
#     "result": {"checked_versions": [2],
#                "affected_versions": [{"version": 2,
#                   "rule": "primitive_type_mismatch",
#                   "path": ["amount"], "path_string": "$.amount", …}],
#                "first_impacted_version": 2}}

curl -s $B/consumer-bindings/7d3f…        # status 已是 "impacted"

# 5. CAS 重试：客户端基于 revision 2 重新评估；若已被别人推进则 409
curl -s "$B/consumer-bindings/7d3f…/impact?expected_revision=2"
curl -s "$B/consumer-bindings/7d3f…/impact?expected_revision=1"   # 409

# 6. 删除 producer subject：绑定与报告原样保留，impact 返回历史修订
curl -s -X DELETE $B/subjects/orders                       # 204
curl -s $B/consumer-bindings/7d3f…/impact
# => {"revision": 2, "created": false, "producer_intact": false, …}
```

## 契约变更事件订阅与断点重放（event subscriptions）

每一个真正改变状态的写入口，都会在**同一数据库事务**内追加**不可变事件**：
事件与业务数据一起提交或一起回滚。事件带**单调全局序号**（自 1 起、连续）、
事件类型、subject（名字）、版本号或指纹摘要、**规范化 payload**（canonical
JSON：键排序、紧凑分隔符）与落库时间。

### 事件类型

| `type` | 触发时机 | `subject` | `version` | `fingerprint` |
| --- | --- | --- | --- | --- |
| `version_registered` | 新版本注册（CAS 命中**不**产生事件） | subject 名 | 新版本号 | 内容指纹 |
| `compatibility_changed` | `PUT config` 真的改了模式（无变化**不**产生事件） | subject 名 | `null` | `null` |
| `release_committed` | 联动发布确认提交（**每个参与 subject 一条**） | subject 名 | 该 subject 的目标版本 | 候选指纹 |
| `migration_committed` | 策略迁移确认提交（**每个参与 subject 一条**） | subject 名 | `null` | 候选指纹 |
| `consumer_binding_created` | 消费者绑定落库（幂等重放**不**产生事件） | producer 名 | 基线版本 | binding id |
| `impact_report_created` | 生成**新**影响报告修订（复用修订**不**产生事件） | producer 名 | producer 最新版本 | binding id |

校验失败（409/422）、过期的联动发布/迁移确认、`blocked` 迁移候选等回滚路径
**不留下任何事件**；幂等重放（CAS 命中、重复 confirm、同键绑定、未移动快照的
影响评估）也不产生事件 —— 事件数等于真实状态变更数。

### 顺序、可见性与一致性

- **单调全局序号**：序号由单行计数器（`event_counter`，唯一行）在业务事务内
  `UPDATE ... RETURNING` 分配。该行的行锁持有到事务提交，因此序号按**提交顺序**
  发放：序号小的事务一定先提交，读到计数器当前值（`snapshot_seq`）时，所有
  `seq <= snapshot_seq` 的事件都已提交可见。
- **不漏不重**：事务回滚时计数器增量一并回滚，因此序号连续、无空洞；并发写入
  在计数器行上串行化，每个状态变更恰好产生其事件。
- **事务原子性**：事件与版本/配置/绑定/报告在同一事务内 —— 提交则都在，回滚
  则都不在。
- **字节一致**：事件不可变，payload 以规范化 JSON 原样存储；同一游标范围重复
  读取返回**逐字节一致**的响应（HTTP body 相同）。
- **历史不随 subject 删除而改写**：事件只按 subject **名字**引用、无外键。
  `DELETE /subjects/{name}` 后历史事件照常可查；同名重建后新事件继续在同一全局
  序号上追加（subject 自身的版本号重新计数）。

### 订阅、分页与 checkpoint

`POST /event-subscriptions` 登记订阅，请求体：

```json
{"subjects": ["orders"], "event_types": ["version_registered", "release_committed"]}
```

两个字段均可省略或为空列表（空 = 不过滤，订阅全部）。订阅同样按名字匹配
subject，subject 被删除后过滤仍能读出它的历史。响应含 `subscription_id`（随机
hex）与初始 `checkpoint: 0`。

读取：`GET /event-subscriptions/{id}/events?after_seq=N&limit=M&upto_seq=S`

- 每页返回满足 `after_seq < seq <= snapshot_seq` 的事件（按 `seq` 升序），并给出
  `snapshot_seq`（读取时的已提交序号）、`next_after_seq`（下一页游标）与
  `has_more`；
- `after_seq` 缺省时使用订阅存储的 `checkpoint` —— 这就是**断点同步**；
- `upto_seq` 可把截止钉在某个 `snapshot_seq`：多页同步或后来再读时，页内容
  逐字节不变（并发提交不会挤进固定快照）。

同步协议：逐页读到 `has_more: false`，把该页的 `snapshot_seq` 提交为 checkpoint，
再循环直到 `snapshot_seq` 不再增长。

`POST /event-subscriptions/{id}/checkpoint` 提交 `{"seq": N}`：

- 只能确认**已存在**的序号；`N` 超过当前 `snapshot_seq` 返回 409
  `checkpoint_beyond_snapshot`（什么都不改）；
- 提交 `0` 即把游标倒回开头，客户端可**重放全部事件重建本地索引**（如
  subject → 最新版本 / 当前兼容模式的映射），`version_registered` 与
  `release_committed` 都携带 subject、版本与指纹，足以重建。

### 示例（curl）

```bash
B=http://localhost:8000

# 1. 登记订阅：只看 orders 的版本注册与联动发布事件
SID=$(curl -s -X POST $B/event-subscriptions -H 'Content-Type: application/json' -d '{
  "subjects": ["orders"],
  "event_types": ["version_registered", "release_committed"]
}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["subscription_id"])')

# 2. 产生一些变更
curl -s -X POST $B/subjects -H 'Content-Type: application/json' -d '{"name":"orders"}'
curl -s -X POST $B/subjects/orders/versions -H 'Content-Type: application/json' -d '{
  "content": {"type": "record", "name": "Order",
              "fields": [{"id": 1, "name": "amount", "type": "int"}]}}'

# 3. 首页（也可显式传 after_seq=0&limit=100）
curl -s "$B/event-subscriptions/$SID/events"
# {
#   "subscription_id": "…", "after_seq": 0, "snapshot_seq": 1,
#   "events": [
#     {"seq": 1, "type": "version_registered", "subject": "orders",
#      "version": 1, "fingerprint": "67db…",
#      "payload": {"subject":"orders", "version":1, "fingerprint":"67db…",
#                  "compatibility":"BACKWARD", "canonical":"…"},
#      "recorded_at": "2026-…"}
#   ],
#   "next_after_seq": 1, "has_more": false
# }

# 4. 提交断点；之后读取默认从 checkpoint 继续
curl -s -X POST $B/event-subscriptions/$SID/checkpoint \
  -H 'Content-Type: application/json' -d '{"seq": 1}'
curl -s "$B/event-subscriptions/$SID/events"        # after_seq=1，无新事件时为空页

# 5. 重建本地索引：checkpoint 归零，分页重放全部历史
curl -s -X POST $B/event-subscriptions/$SID/checkpoint \
  -H 'Content-Type: application/json' -d '{"seq": 0}'
curl -s "$B/event-subscriptions/$SID/events?limit=100&upto_seq=1"

# 6. 订阅全部事件（不过滤）：删除 subject 后历史仍可查
ALL=$(curl -s -X POST $B/event-subscriptions -H 'Content-Type: application/json' -d '{}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["subscription_id"])')
curl -s -X DELETE $B/subjects/orders
curl -s "$B/event-subscriptions/$ALL/events"         # 事件原样保留
```

并发保证在真实 PostgreSQL 行级锁下有专门回归：并发确认同一迁移候选的两个请求
都成功时，每个 subject 的 `migration_committed` 事件恰好写入一次，全局序号连续
（`tests/test_migration_concurrency_pg.py`，不可达 PostgreSQL 时自动跳过）。

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql+psycopg://contracts:contracts@localhost:5432/contracts` | SQLAlchemy 连接串；`sqlite:///...` 可用于本地开发/测试 |
| `TEST_DATABASE_URL` | `postgresql+psycopg://contracts:contracts@localhost:5432/contracts` | 仅测试：PostgreSQL 并发回归用的**服务器**连接串（只用于定位服务器，指向的库不被改动）；不可达时这些用例自动跳过 |

## 项目结构

```
app/
  contract/          # 契约内核（纯函数，无 IO）
    types.py         #   类型节点定义
    parser.py        #   校验 + 构建（两趟：先收集具名类型，支持前向/递归引用）
    canonical.py     #   规范化 + SHA-256 指纹
    compat.py        #   reader/writer 兼容性引擎（visited 类型对终止循环）
  service.py         # 注册编排：CAS、模式分派、并发冲突
  release.py         # 联动发布：快照校验、依赖图批次计划、最短冲突环、全局 CAS 确认
  migration.py       # 策略迁移：版本顺序重放、冲突版本对、不可变候选、全局 CAS 切换
  binding.py         # 消费者绑定：幂等键、基线校验、漂移重放、不可变报告修订与 CAS
  events.py          # 统一事件器：同事务追加不可变事件、全局序号分配、订阅与断点重放
  routers/           # FastAPI 路由（subjects / versions / releases / migrations / bindings / event-subscriptions）
  models.py db.py    # SQLAlchemy 模型与引擎
tests/               # 解析、规范化、兼容性、API、并发、联动发布、策略迁移、消费者绑定、事件订阅测试
  pg_scratch.py      #   PG 暂存库归属租约：咨询锁 + 归属注释 + 标记表/心跳/代际
  test_migration_concurrency_pg.py   # 真实 PG：迁移确认行锁竞争 + 事件恰好一次回归
  test_pg_scratch_ownership.py       # 真实 PG：暂存库归属/接管边界
```
