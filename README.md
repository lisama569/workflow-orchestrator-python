# 本地轻量工作流调度服务

一个**本地可运行**的轻量工作流调度服务:用本地 JSON 文件作为状态存储,模拟任务的调度与执行。
目标是让 **重复提交、并发执行、进程重启** 三种情形下的调度结论保持一致且可解释。

- 纯 Python 3.14 + FastAPI,无外部数据库 / 消息队列依赖
- 状态原子落盘(临时文件 + `os.replace` + fsync),进程崩溃不会留下半截状态
- 后台线程池调度,全局并发上限可控
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
- 对已终态的工作流重复 cancel/terminate 是幂等的,返回同一终态,不会报错、不会复活。

### 3.4 进程重启恢复

启动时(`Scheduler.recover()`)从状态文件加载全部工作流:

- `SUCCEEDED / FAILED / SKIPPED / CANCELLED` 等终态**原样保留,成功任务绝不重跑**;
- 崩溃时停在 `RUNNING` 的任务,由于其结论尚未持久化,回退为 `PENDING` 并**重新调度**,
  转移记录写明 `crash recovery: RUNNING->PENDING, no durable conclusion, safe to rerun`;
- 退避中的 `PENDING`(带 `next_run_at`)继续按退避时间重试;
- 已持久化的取消请求继续生效。

持久化采用"整快照 + 临时文件 fsync + `os.replace` 原子替换",任何时刻读到的都是完整状态。

### 3.5 幂等提交

- 请求体 `idempotency_key`(或 `Idempotency-Key` 头)相同:
  - 同一 `workflow_id` 重复提交 -> 返回**既有工作流对象**,不产生重复执行(HTTP 201);
  - 不同 `workflow_id` 复用同一 key -> 拒绝,`IDEMPOTENCY_KEY_CONFLICT` / HTTP 409,
    响应体给出 `existing_workflow_id`,调用方可据此取回原工作流。

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

测试覆盖(共 31 个):

| 文件 | 场景 |
|---|---|
| `tests/test_idempotency.py` | 幂等重复提交不重复执行、幂等键冲突 409、空/重复 id/缺依赖/环(含自环/长环)拒绝原因可区分 |
| `tests/test_dependencies.py` | 全部依赖成功才执行、不可重试立即终止、依赖失败确定性 SKIPPED 多级传播、取消与终止的终态/幂等语义 |
| `tests/test_retry_concurrency.py` | 指数退避重试后成功、重试预算耗尽收敛 FAILED、并发上限不被突破、同一任务只执行一次 |
| `tests/test_recovery.py` | 重启后成功任务不重跑、终态与错误原因不丢失、RUNNING 崩溃回退重调度、取消请求跨重启生效 |
| `tests/test_api.py` | HTTP 端到端:提交/查询/422/409/404/终止不可逆 |

测试日志通过 `log_cli` 实时输出,统一格式:

```
INFO [orchestrator.scheduler] task=wobbly transition RUNNING->PENDING reason=retryable failure ...
INFO [orchestrator.scheduler] workflow=wf-1 transition RUNNING->SUCCEEDED reason=all 3 task(s) succeeded
```

### 端到端手动验证

见第 1 节;也可直接启动服务后:提交 -> `GET /workflows/{id}` 观察 `transitions` ->
`Ctrl+C` 杀掉进程 -> 用同一 `WF_STATE_PATH` 重新启动 -> 再次查询,验证终态与结论一致。

---

## 6. 代码结构

```
orchestrator/
  models.py        状态枚举/任务与工作流模型/退避策略/序列化
  errors.py        可区分的失败原因(稳定 error_code)
  store.py         本地 JSON 原子持久化 + RLock
  executor.py      本地确定性模拟执行器(pass/fail/flaky/sleep)
  scheduler.py     调度循环:依赖推进/退避重试/并发上限/取消终止/崩溃恢复
  service.py       门面:提交校验、幂等去重、查询、取消与终止
  api.py           FastAPI 路由与错误码映射
  logging_setup.py 统一日志(任务标识/状态转移/判定依据)
tests/             按场景组织的 31 个用例
main.py            uvicorn 入口
```
