# 本地轻量工作流调度服务

一个**本地可运行**的轻量工作流调度服务:用本地 JSON 文件作为状态存储,模拟任务的调度与执行。
目标是让 **重复提交、并发执行、进程重启** 三种情形下的调度结论保持一致且可解释。

- 纯 Python 3.14 + FastAPI,无外部数据库 / 消息队列依赖
- 状态原子落盘(临时文件 + `os.replace` + fsync),进程崩溃不会留下半截状态
- 后台线程池调度,全局并发上限可控
- 支持**工作流组**:一次提交一批工作流,组级防重、组级终态收敛、组级取消/终止
- 每个状态转移都记录时间戳、来源状态、目标状态与**判定依据**,并打到日志

---

## 1. 快速开始

```bash
uv sync
uv run uvicorn main:app --host 127.0.0.1 --port 8000
# 可选环境变量
#   WF_STATE_PATH=workflow_state.json   状态文件路径
#   WF_MAX_CONCURRENCY=4                全局并发上限
```

健康检查与 OpenAPI 文档:

```bash
curl http://127.0.0.1:8000/health
# 交互式文档: http://127.0.0.1:8000/docs
```

### 提交一个工作流

```bash
curl -X POST http://127.0.0.1:8000/workflows \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-123' \
  -d '{
    "workflow_id": "wf-1",
    "tasks": [
      {"id": "fetch",  "action": "pass"},
      {"id": "wobbly", "action": "flaky", "params": {"fail_times": 2},
       "retry_policy": {"max_retries": 3, "base_delay_seconds": 0.1, "backoff_multiplier": 2}},
      {"id": "report", "action": "pass", "dependencies": ["fetch", "wobbly"]}
    ]
  }'
```

### 提交一组工作流

```bash
curl -X POST http://127.0.0.1:8000/groups \
  -H 'Content-Type: application/json' \
  -H 'Group-Key: batch-2026-10-03' \
  -d '{
    "group_id": "batch-1",
    "workflows": [
      {"workflow_id": "etl-users",  "tasks": [{"id": "extract", "action": "pass"},
                                              {"id": "load", "action": "pass", "dependencies": ["extract"]}]},
      {"workflow_id": "etl-orders", "tasks": [{"id": "extract", "action": "pass"},
                                              {"id": "load", "action": "pass", "dependencies": ["extract"]}]}
    ]
  }'

# 按组查询组内各工作流当前状态
curl http://127.0.0.1:8000/groups/batch-1
# 组级取消 / 终止(不可逆、幂等)
curl -X POST http://127.0.0.1:8000/groups/batch-1/cancel
curl -X POST http://127.0.0.1:8000/groups/batch-1/terminate
```

### 模拟动作(action)

执行器是本地确定性模拟,便于复现调度行为:

| action | 参数 | 行为 |
|---|---|---|
| `pass` | `output`(可选) | 始终成功 |
| `fail` | `kind`: `retryable`/`non_retryable`,`message` | 始终以指定类型失败 |
| `flaky` | `fail_times` | 前 N 次**可重试**失败,第 N+1 次起成功 |
| `sleep` | `seconds` | 睡眠后成功;终止时可被中断(用于并发/取消测试) |

---

## 2. 调度规则

### 2.1 依赖推进

- 任务只有在其**全部依赖都为 `SUCCEEDED`** 时才会被派发执行。
- 依赖关系在提交时静态校验,以下情况**整次提交被拒绝**,不产生任何持久化与执行,
  且失败原因可通过不同的 `error_code` 区分:

  | 情形 | error_code | HTTP |
  |---|---|---|
  | 任务列表为空 | `EMPTY_WORKFLOW` | 422 |
  | 同次提交任务 id 重复 | `DUPLICATE_TASK_ID` | 422 |
  | 依赖指向不存在的任务 | `MISSING_DEPENDENCY`(返回 `task_id` 与 `missing_dependency`) | 422 |
  | 依赖图存在环(含自依赖) | `CYCLE_DETECTED`(返回环上节点序列) | 422 |

  环检测使用 DFS 三色法。

### 2.2 依赖失败的确定传播(不静默跳过)

下游 `PENDING` 任务若发现任一依赖进入 **`FAILED` / `SKIPPED` / `CANCELLED`**,
会被**确定性地**标记为 `SKIPPED`(不动点传播,支持多级下游),并记录:

- `skip_reason` 与 `skipped_by`:由哪些依赖导致跳过;
- 一条 `... -> SKIPPED` 的状态转移,reason 写明 `dependency never succeeded: <deps>`。

因此不会出现"任务悄悄没跑"的情况,任何非成功终态都能解释。无关节点不受影响,正常执行。

### 2.3 重试与退避

- 一次执行的失败结论分两类:
  - **可重试(retryable)**:回到 `PENDING` 并设置 `next_run_at`,按指数退避等待后重新调度;
  - **不可重试(non-retryable)**:立即进入 `FAILED` 终态,只执行一次。
- 退避公式:`delay(n) = min(max_delay, base_delay * backoff_multiplier^(n-1))`
  (第 1 次失败等待 `base_delay`,之后逐次乘以倍数,封顶 `max_delay`)。
- 最大执行次数 `max_attempts = 1 + max_retries`(任务定义里 `retryable=false` 时强制为 1)。
- 达到上限仍失败 -> `FAILED` 终态,reason 记录 `retry budget exhausted after N attempt(s)`。
- 注意:可重试失败**不经过 `FAILED`**,而是 `RUNNING -> PENDING(等待退避)`。
  否则下游会把"重试中的暂时性失败"误判成终态而提前 SKIPPED。`FAILED` 只表示确定失败的终态。

### 2.4 并发控制

- 全局在执行任务数**绝不超过** `max_concurrency`(线程池大小 + 派发前槽位检查双重保证)。
- 同一 `(工作流, 任务)` 在任意时刻最多只有一个在执行实例:在执行任务登记在调度器的
  `_running` 表中,完成回调只接受 `status=RUNNING` 且 `attempt` 匹配的结论,
  过期/重复的完成通知会被忽略。
- 派发按任务 id 排序,行为确定。

---

## 3. 状态语义

### 3.1 任务状态机

```
PENDING ──► RUNNING ──► SUCCEEDED         (终态:成功,重启不重跑)
   │          │  │
   │          │  ├─(可重试失败)─► PENDING(next_run_at 退避) ─► RUNNING ...
   │          │  └─(不可重试/达到上限)─► FAILED              (终态)
   │          │
   │          └─(进程崩溃)─恢复► PENDING(重新调度)
   ├─(依赖 FAILED/SKIPPED/CANCELLED)─► SKIPPED   (终态,确定性传播)
   └─(取消/终止)─────────────────────► CANCELLED (终态)
```

终态集合:`SUCCEEDED / FAILED / SKIPPED / CANCELLED`。终态不可逆。

### 3.2 工作流状态机

```
PENDING -> RUNNING -> SUCCEEDED          (所有任务 SUCCEEDED)
                   |  FAILED             (有任务 FAILED,或只剩 SKIPPED;reason 含失败明细与被跳过的下游)
                   |  CANCELLED          (收到取消/终止)
```

后三者均为不可逆终态。工作流终态的 `reason` 汇总失败任务的 `[error_type] message`
以及被跳过的下游任务,保证结论可解释。

### 3.3 取消 vs 终止

| 操作 | 语义 |
|---|---|
| `POST /workflows/{id}/cancel` | **优雅取消(不可逆)**:置持久化标志 `cancel_requested`,不再派发新任务;所有尚未开始的任务立即 `CANCELLED`;**执行中的任务自然跑完**,之后工作流收敛为 `CANCELLED` |
| `POST /workflows/{id}/terminate` | **立即终止(不可逆)**:未开始任务立即 `CANCELLED`,给执行中的任务发中断信号(`sleep` 类动作会立即醒来),其执行结果**一律按 `CANCELLED` 收敛(结果丢弃)**;工作流立即进入 `CANCELLED` 终态 |

- 取消/终止通过**持久化标志**表达,即使进程重启,标志仍在,恢复后继续被遵守。
- 对已 `CANCELLED` 的工作流重复 cancel/terminate 是幂等的,返回同一终态,不会报错、不会复活;
  对 `SUCCEEDED/FAILED` 等其他终态调用 cancel/terminate 返回 `TERMINAL_STATE` / 409。
- **先取消、后终止**:优雅取消已被接受、工作流尚未终态时再调 terminate,会**升级**为
  立即终止(工作流马上进入 `CANCELLED`,退避/未派发的 `PENDING` 任务立即 `CANCELLED`)。

#### 3.3.1 执行中并发取消/终止的语义(不会再卡死)

取消/终止是控制面操作,可以与任务执行、完成回调**任意交错**,保证:

1. **不互相等待、不死锁**:所有状态变更路径统一锁序「工作流锁 → 存储锁」
   (调度扫描、任务完成回调、取消/终止均如此)。此前取消路径在存储事务内反向获取
   工作流锁,与完成回调构成 AB-BA——任务完成的瞬间并发取消即可让后台推进和取消
   接口一起永久挂起;该问题已修复。
2. **及时返回、立即生效**:cancel/terminate 只做持久化的状态变更并通知调度器,
   绝不等待任何任务执行结束;terminate 调用返回时工作流(及组)已在终态,
   在执行任务靠中断信号尽快醒来,其结论按 `CANCELLED` 收敛。
3. **幂等、无中间态**:多线程同时取消、反复取消/终止、对已取消对象重复操作,
   结果都相同——标志至多置位一次、终态至多进入一次,不会出现"一部分生效、
   另一部分永久挂起"。
4. **成功内容不丢不乱、不重跑**:已 `SUCCEEDED` 的任务与工作流不受取消影响;
   每个任务的 `RUNNING` 派发至多一次。终态结论(组/工作流/任务)先落盘再处理
   回调,进程重启后从状态文件恢复出的仍是同一结论。

**本地复现与验证**:

```bash
uv run pytest tests/test_cancel_concurrency.py -v   # 并发交错回归用例(带 20s 超时)
```

用例覆盖:任务边跑边取消/终止、6 线程反复交错 cancel+terminate、组 + 成员被
多线程同时终止、先取消后终止升级、成功任务跨并发取消与重启不重跑、
终止落盘后"崩溃"的残留任务恢复为 `CANCELLED`。每个用例中所有控制面线程都带
join 超时,`pyproject.toml` 配置了 `timeout = 20`,单个用例超过合理时间即判失败。
`log_cli` 实时日志逐条打印每个组/工作流/任务的状态转移,可看出判定顺序。

也可用内置动作手工压测(提交若干 `sleep` 长任务后并发打 cancel/terminate 端点),
修复前服务会在数秒内整体卡住,修复后所有请求立即返回并确定性收敛。

### 3.4 进程重启恢复

启动时(`Scheduler.recover()`)从状态文件加载全部工作流:

- `SUCCEEDED / FAILED / SKIPPED / CANCELLED` 等终态**原样保留,成功任务绝不重跑**;
- 崩溃时停在 `RUNNING` 的任务,由于其结论尚未持久化,回退为 `PENDING` 并**重新调度**,
  转移记录写明 `crash recovery: RUNNING->PENDING, no durable conclusion, safe to rerun`;
- 退避中的 `PENDING`(带 `next_run_at`)继续按退避时间重试;
- 已持久化的取消请求继续生效;
- 终止已落盘、但在执行任务的完成回调落盘前崩溃时,`CANCELLED` 终态工作流上可能残留
  `RUNNING/PENDING` 任务:终态不可逆,恢复时确定性补齐为 `CANCELLED`(**不回退、不重跑**),
  组与工作流结论保持与崩溃前一致。

持久化采用"整快照 + 临时文件 fsync + `os.replace` 原子替换",任何时刻读到的都是完整状态。

### 3.5 幂等提交

- 请求体 `idempotency_key`(或 `Idempotency-Key` 头)相同:
  - 同一 `workflow_id` 重复提交 -> 返回**既有工作流对象**,不产生重复执行(HTTP 201);
  - 不同 `workflow_id` 复用同一 key -> 拒绝,`IDEMPOTENCY_KEY_CONFLICT` / HTTP 409,
    响应体给出 `existing_workflow_id`,调用方可据此取回原工作流。

---

## 3A. 工作流组

一组**一次提交、共同收尾**的工作流。每个成员工作流内部仍是原来的任务图结构,
**成员的单工作流语义(依赖推进、失败传播、重试、取消)不因放进组里而改变**;
组只是在其之上加一层防重、汇总结论与组级控制。

### 3A.1 组级防重复提交

- 组携带防重键 `group_key`(请求体或 `Group-Key` 头)。提交内容的规范化指纹
  (`spec_hash`,成员工作流定义的 SHA-256,与成员顺序无关)随组一起持久化。
- **同键同内容**(无论并发多少次、用哪个 `group_id` 重试):幂等返回**已落地的组**,
  组下的工作流只建一次、只执行一次,不会出现同一批提交被重复建出来重复执行。
- **同键不同内容**:拒绝,`GROUP_KEY_CONFLICT` / HTTP 409,响应体带 `existing_group_id`
  指明这次提交最终落到了哪个已有组。该错误码与校验失败(`EMPTY_GROUP` /
  `DUPLICATE_WORKFLOW_ID` / `CYCLE_DETECTED` 等 422)明确可区分。
- **全有或全无**:组 + 全部成员工作流在一次存储事务内落盘;任一成员校验失败或
  成员 `workflow_id` 与已有工作流冲突(`WORKFLOW_ID_CONFLICT` / 409)时整组拒绝,
  不会留下"一半存在一半丢失"的状态。

### 3A.2 组级终态语义

组只有在**全部成员工作流到达终态**后才收敛,确定规则:

| 条件 | 组终态 |
|---|---|
| 全部成员 `SUCCEEDED` | `SUCCEEDED` |
| 组级取消/终止已生效 | `CANCELLED` |
| 其余(有成员 `FAILED`/`CANCELLED`) | `FAILED`,`reason` 列出导致结论的成员及其原因 |

- 成员失败**不会**让组内其他成员被悄悄跳过:未开始的成员照常推进到自己的终态,
  失败成员内部的下游任务按原有规则确定性 `SKIPPED`(带 `skip_reason` / `skipped_by`)。
- 组的 `reason` 汇总未成功成员的 `workflow_id=STATUS(成员reason)`,可逐级下钻到
  具体任务的错误或跳过依据。

### 3A.3 组级取消 / 终止

- `POST /groups/{id}/cancel`:组级优雅取消(不可逆)。置持久化 `cancel_requested`,
  作用到组内**所有尚未完成**的成员(等价于对每个成员执行单工作流取消);
  已 `SUCCEEDED` 的成员原样保留、绝不重跑。组在全部成员终态后收敛为 `CANCELLED`。
- `POST /groups/{id}/terminate`:组级立即终止(不可逆)。未完成成员立即进入
  `CANCELLED` 终态,在执行任务收到中断信号、结果被丢弃;组立即进入 `CANCELLED` 终态。
- 两者都**幂等**:重复调用返回同一终态;组标志与成员取消在同一持久化快照内生效,
  进程重启后由恢复逻辑继续遵守(若有成员因崩溃未应用取消,调度器会确定性补齐)。
- 先组级 cancel 后组级 terminate 同样会**升级**为立即终止:组马上进入 `CANCELLED`,
  尚未终态的成员立即终态,其在执行任务收到中断信号。
- 组操作不取成员工作流锁、成员终止后再按成员逐个加锁补发中断信号,锁序始终为
  「工作流锁 → 存储锁」,组级与成员级取消/终止并发交错也不会死锁。

### 3A.4 与单工作流接口的兼容范围

- 单工作流的全部接口(`POST/GET /workflows...`、取消/终止、幂等键)行为不变;
  组内成员也可以用单工作流接口查询。
- 状态文件升级为 `version: 2`(新增 `groups` 字段),**兼容读取**旧的 version 1 文件。
- 全局并发上限对组内所有成员统一生效:组内工作流再多,同时在执行的任务数也不越过
  `max_concurrency`。

---

## 4. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET  | `/health` | 健康检查,返回当前在执行任务数 |
| POST | `/workflows` | 提交工作流(支持幂等键) |
| GET  | `/workflows` | 列出工作流(id + status) |
| GET  | `/workflows/{id}` | 查询工作流:全部任务状态、错误、跳过依据、转移历史 |
| GET  | `/workflows/{id}/tasks/{task_id}` | 查询单个任务 |
| POST | `/workflows/{id}/cancel` | 优雅取消(不可逆) |
| POST | `/workflows/{id}/terminate` | 立即终止(不可逆) |
| POST | `/groups` | 一次提交一组工作流(支持 `Group-Key` 头防重) |
| GET  | `/groups` | 列出工作流组(id + status) |
| GET  | `/groups/{id}` | 查询组:组状态、reason、转移历史、组内各工作流当前状态 |
| POST | `/groups/{id}/cancel` | 组级优雅取消(不可逆,幂等) |
| POST | `/groups/{id}/terminate` | 组级立即终止(不可逆,幂等) |

每个任务 / 工作流对象都带 `transitions` 数组,元素形如:

```json
{"ts": 1790000000.12, "from": "RUNNING", "to": "PENDING",
 "reason": "retryable failure [FLAKY_FAILURE] flaky failure 1/2; backoff 0.100s then retry 2/4"}
```

---

## 5. 本地验证

### 运行单元测试

```bash
uv run pytest                      # 全量,日志会实时打印状态转移
uv run pytest -q                   # 安静模式
uv run pytest tests/test_recovery.py -v
```

测试覆盖(共 62 个):

| 文件 | 场景 |
|---|---|
| `tests/test_idempotency.py` | 幂等重复提交不重复执行、幂等键冲突 409、空/重复 id/缺依赖/环(含自环/长环)拒绝原因可区分 |
| `tests/test_dependencies.py` | 全部依赖成功才执行、不可重试立即终止、依赖失败确定性 SKIPPED 多级传播、取消与终止的终态/幂等语义 |
| `tests/test_retry_concurrency.py` | 指数退避重试后成功、重试预算耗尽收敛 FAILED、并发上限不被突破、同一任务只执行一次 |
| `tests/test_recovery.py` | 重启后成功任务不重跑、终态与错误原因不丢失、RUNNING 崩溃回退重调度、取消请求跨重启生效 |
| `tests/test_groups.py` | 同一组键并发重复提交只落地一个组、组键冲突与校验失败可区分、整组全有或全无、成员失败时组级收敛且结论可解释、组级取消/终止幂等且已成功成员不重跑、重启后组终态与成员结论不丢失、组内并发不越上限 |
| `tests/test_cancel_concurrency.py` | 执行中取消/终止及时返回并收敛、多线程反复交错 cancel+terminate 不卡死(原 AB-BA 死锁回归)、重复/并发取消幂等无中间态、对已成功对象取消报 409 且不重跑、先取消后终止升级、组与成员被多线程同时终止、成功内容跨并发取消与重启不丢、终止后崩溃恢复同一结论 |
| `tests/test_api.py` | HTTP 端到端:提交/查询/422/409/404/终止不可逆,含组端点(防重冲突 409、按组查询成员状态、组级终止幂等) |

测试日志通过 `log_cli` 实时输出,统一格式:

```
INFO [orchestrator.scheduler] task=wobbly transition RUNNING->PENDING reason=retryable failure ...
INFO [orchestrator.scheduler] workflow=wf-1 transition RUNNING->SUCCEEDED reason=all 3 task(s) succeeded
INFO [orchestrator.scheduler] group=batch-1 transition RUNNING->FAILED reason=member workflow(s) not succeeded: etl-users=FAILED(...)
```

### 端到端手动验证

见第 1 节;也可直接启动服务后:提交 -> `GET /workflows/{id}` 观察 `transitions` ->
`Ctrl+C` 杀掉进程 -> 用同一 `WF_STATE_PATH` 重新启动 -> 再次查询,验证终态与结论一致。

---

## 6. 代码结构

```
orchestrator/
  models.py        状态枚举/任务、工作流与工作流组模型/退避策略/序列化
  errors.py        可区分的失败原因(稳定 error_code,含组键冲突/组不存在)
  store.py         本地 JSON 原子持久化 + RLock + 多实体事务(整组全有或全无)
  executor.py      本地确定性模拟执行器(pass/fail/flaky/sleep)
  scheduler.py     调度循环:依赖推进/退避重试/并发上限/取消终止/组级收敛/崩溃恢复
  service.py       门面:提交校验、幂等去重(含组键防重)、查询、取消与终止
  api.py           FastAPI 路由与错误码映射(含 /groups 端点)
  logging_setup.py 统一日志(任务标识/状态转移/判定依据)
tests/             按场景组织的 62 个用例(单例超时 20s,挂起即失败)
main.py            uvicorn 入口
```
