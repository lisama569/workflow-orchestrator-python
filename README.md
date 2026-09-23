# 本地轻量工作流调度服务

一个**本地可运行**的轻量工作流调度服务：用本地 JSON 文件存储模拟任务调度与
执行，保证重复提交、并发执行与进程重启后的结论**一致且可解释**。

- 提交期：任务名唯一、依赖必须存在、依赖图必须无环，拒绝原因可区分
- 幂等：相同幂等键 + 相同定义的重复提交绝不产生重复执行
- 依赖：任务在全部依赖成功后才可执行；上游失败/取消按**确定规则**级联，不静默跳过
- 重试：可重试失败按指数退避重试到上限；不可重试失败立即终止
- 并发：全局上限 + 每工作流上限双重约束；同一任务任意时刻不重复执行
- 恢复：进程重启后已成功任务不重跑，未完成状态不丢，RUNNING 残留重放
- 取消：明确且不可逆的终态语义
- 每次状态转移都持久化到事件日志，单测输出打印任务标识、转移与判定依据

---

## 1. 快速开始

```bash
uv sync

# 运行测试（-s 可看到任务状态转移日志）
uv run pytest
uv run pytest -s tests/test_recovery.py

# 命令行端到端演示（依赖链/重试/失败级联/幂等/崩溃恢复/取消）
uv run python demo.py

# 启动 HTTP 服务
uv run uvicorn main:app --reload
# 或 fastapi[standard] 自带命令
uv run fastapi dev main.py
```

服务默认把状态写入当前目录下的 `.state/`（每个工作流一个 JSON 文件，
原子写）。可用环境变量调整：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WF_STATE_DIR` | `.state` | 状态文件目录 |
| `WF_TICK_INTERVAL` | `0.05` | 后台调度轮询间隔（秒） |

---

## 2. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/workflows` | 提交工作流（201） |
| `GET` | `/workflows` | 列出全部工作流 |
| `GET` | `/workflows/{id}` | 查询工作流（含任务状态、失败原因、事件轨迹） |
| `GET` | `/workflows/idempotency/{key}` | 按幂等键查询 |
| `POST` | `/workflows/{id}/cancel` | 取消整个工作流 |
| `POST` | `/workflows/{id}/tasks/{name}/cancel` | 取消单个任务 |
| `GET` | `/health` | 健康检查 |

### 提交示例

```bash
curl -s -X POST http://127.0.0.1:8000/workflows -H 'Content-Type: application/json' -d '{
  "idempotency_key": "order-123",
  "max_concurrency": 4,
  "tasks": [
    {"name": "reserve",  "handler": "success"},
    {"name": "charge",   "handler": "flaky", "payload": {"fail_times": 2},
     "retry_policy": {"max_retries": 3, "base_delay": 1.0,
                      "backoff_factor": 2.0, "max_delay": 30.0},
     "deps": ["reserve"]},
    {"name": "ship", "handler": "success", "deps": ["charge"]}
  ]
}' | python -m json.tool
```

### 内置处理器

| handler | 行为 |
| --- | --- |
| `success` | 恒成功，回显 payload |
| `fail_always` | 恒抛可重试异常（`TaskExecutionError`） |
| `fail_non_retryable` | 恒抛不可重试异常（`NonRetryableTaskError`） |
| `flaky` | 前 `payload.fail_times` 次可重试失败，之后成功 |
| `gate` | 阻塞在命名闸门，直到 release（成功）/ 取消 / 内部 abort（模拟进程崩溃），测试使用 |

自定义处理器可向 `BuiltinRegistry.register(name, fn)` 注册，签名为
`(TaskContext) -> dict`；长任务应轮询 `ctx.cancel_event` 以支持协作式取消。

### 错误码（响应体 `error` 字段，可程序化区分）

| HTTP | error | 触发场景 |
| --- | --- | --- |
| 400 | `CYCLIC_DEPENDENCY` | 依赖图存在环（含自依赖） |
| 400 | `UNKNOWN_DEPENDENCY` | 依赖指向不存在的任务 |
| 400 | `DUPLICATE_TASK_NAME` | 工作流内任务名重复 |
| 409 | `IDEMPOTENCY_CONFLICT` | 幂等键被不同定义复用 |
| 400 | `NON_RETRYABLE` | 任务抛出不可重试异常 |
| 200 | `RETRY_EXHAUSTED`* | 可重试失败达到重试上限（任务终态原因） |
| 200 | `DEPENDENCY_FAILED`* | 上游 FAILED（或因失败被 SKIPPED） |
| 200 | `DEPENDENCY_CANCELLED`* | 上游 CANCELLED |
| 409 | `ALREADY_TERMINAL` | 对终态工作流/任务再次取消 |
| 404 | `NOT_FOUND` | 工作流/任务不存在 |

\* 这些是任务终态的 `failure_reason`（200 响应体内），不是 HTTP 错误。

---

## 3. 状态语义

### 任务状态机

```
                ┌────────── submit ──────────┐
                ▼                            │
            PENDING ──依赖全部成功──▶ READY ──┼── START ──▶ RUNNING
               │  ▲                      ▲   │             │  │
               │  └──────────────────────┘   │     成功────┘  ├─ 不可重试/重试耗尽
        上游坏 │（退避到期）           退避到期│             ▼  ▼
               │                    RETRYING ◀── 可重试失败 FAILED（终态）
               ▼                                             
            SKIPPED（终态）          取消信号: PENDING/READY/RETRYING ─▶ CANCELLED（终态）
                                   RUNNING 协作式取消（执行返回后）──▶ CANCELLED（终态）
```

终态（不可逆）：`SUCCEEDED`、`FAILED`、`SKIPPED`、`CANCELLED`。

- **PENDING**：已提交，尚有依赖未成功。
- **READY**：全部依赖成功，等待调度（含崩溃恢复后的重放）。
- **RUNNING**：正在执行。进入该状态前**先同步落盘**，因此崩溃后磁盘可见残留。
- **RETRYING**：可重试失败，记录 `scheduled_at`（指数退避），到期转 READY。
- **SUCCEEDED**：执行成功，结果已持久化，重启后**不会重跑**。
- **FAILED**：不可重试失败（`NON_RETRYABLE`，attempts=1）或重试耗尽
  （`RETRY_EXHAUSTED`，attempts = 1 + max_retries）。
- **SKIPPED**：上游进入坏状态，本任务**从未执行**（attempts=0），携带
  `DEPENDENCY_FAILED` 或 `DEPENDENCY_CANCELLED` 及阻断依赖名。
- **CANCELLED**：用户取消。未开始任务收到取消立即终态；RUNNING 任务通过
  `cancel_event` 协作式收敛，即使处理器随后返回成功也不会复活。

### 工作流聚合状态

每个调度 tick 内反复推进任务直到不动点，然后聚合：

1. 任一任务 `FAILED` → 工作流 `FAILED`；
2. 全部任务终态，且存在 `CANCELLED/SKIPPED` → `CANCELLED`；
3. 全部任务 `SUCCEEDED` → `SUCCEEDED`；
4. 否则 `RUNNING`。

---

## 4. 调度与重试规则

### 依赖判定（确定性，与线程时序无关）

每轮按 `(workflow_id, task_name)` 排序评估，依赖阻断的优先级为：

```
FAILED（最高） > CANCELLED > SKIPPED > 未完成（阻塞）
```

- 因失败而 `SKIPPED` 的中间节点对下游仍计为“失败”，失败原因沿链传播为
  `DEPENDENCY_FAILED`；只有真正由取消引发的路径才得到
  `DEPENDENCY_CANCELLED`。下游**绝不静默跳过**：一定落为 SKIPPED 终态，
  `failure_detail` 给出首个阻断依赖（如 `upstream 'x' is FAILED`）。
- 显式取消优先于依赖级联：已请求取消的未开始任务直接 CANCELLED。

### 退避重试

第 n 次可重试失败后的等待：

```
delay(n) = min(base_delay * backoff_factor ** (n - 1), max_delay)
```

- `max_retries=0` 表示完全不重试；总执行次数上限 = `1 + max_retries`。
- 处理器抛 `NonRetryableTaskError` → 立即 `FAILED/NON_RETRYABLE`。
- 其他任何异常 → 可重试；`attempts > max_retries` 时
  `FAILED/RETRY_EXHAUSTED`。
- RETRYING 期间任务不可被启动，因此同一任务任意时刻最多一个执行体。

### 并发控制

- 全局上限（默认 16，`Scheduler(max_global_concurrency=...)`）与每工作流
  `max_concurrency`（默认 4）双重约束；候选任务按确定顺序填充槽位。
- 启动前在锁内检查状态必须为 READY，并登记 worker 记录，保证同一任务
  不会被并发双跑。
- 任务置 RUNNING 后**先 `fsync` 落盘再执行业务代码**。

### 幂等提交

- 提交先做定义校验（重名 → 未知依赖 → 环，顺序固定）。
- 幂等键已存在时：比较定义的**规范化指纹**（任务按名排序）。
  - 相同：直接返回原工作流（无论它在 RUNNING 还是终态），**不重复执行**；
  - 不同：`409 IDEMPOTENCY_CONFLICT`。
- 提交在存储互斥锁内完成读改写，并发重复提交只会创建一个工作流。

### 取消语义（不可逆）

- 工作流取消：所有未终态任务请求取消（`WORKFLOW_CANCELLED`）；RUNNING 任务
  协作式取消；工作流终态为 CANCELLED。
- 单任务取消：仅该任务 `TASK_CANCELLED`，下游确定性
  `SKIPPED/DEPENDENCY_CANCELLED`。
- 对 `SUCCEEDED/FAILED/CANCELLED` 的工作流或任一任务终态再次取消：
  一律 `409 ALREADY_TERMINAL`。取消不“排队”、不可撤销。

---

## 5. 进程重启与恢复

存储为每工作流一个 JSON，写入走“临时文件 + `os.replace`”原子替换，
崩溃最多丢失最后一次未落盘转移，绝不会出现半截文件。

启动时 `Scheduler.recover()` 扫描磁盘并执行：

| 磁盘状态 | 恢复动作 |
| --- | --- |
| `SUCCEEDED / FAILED / SKIPPED / CANCELLED` | 原样加载，结论不变 |
| `PENDING / READY` | 原样加载，按依赖继续推进 |
| `RETRYING` | `scheduled_at` 清零，立即获得一次调度机会（`attempts` 不变） |
| `RUNNING`（崩溃残留） | 转为 READY 立即重放，记录 `CRASH_RECOVERY` 事件，`attempts` 保留 |

语义为 **at-least-once**：崩溃时正在执行的任务重启后可能再执行一次，
但已 SUCCEEDED 的任务绝不重跑。重放不消耗重试预算（崩溃不算是任务失败）。
崩溃模拟通过 `Scheduler.stop(drain_workers=False)` 实现：置位 worker 的内部
abort 信号（与业务 cancel 信号严格分离），在途执行立即放弃落盘，磁盘因此
保留 RUNNING 残留；随后对同一状态目录新建 Scheduler 并调用 `recover()`。

---

## 6. 可解释性：事件轨迹与日志

每个工作流持久化 `events`（带全局递增 `seq`），每条转移包含
`task / from_state / to_state / reason / detail / at`。调度器同时以
`orchestrator` logger 输出，例如：

```
[wf-xxx/b] READY -> RUNNING | reason=START | attempt 2 started
[wf-xxx/b] RUNNING -> RETRYING | reason=RETRY_SCHEDULED | retryable FlakyError on attempt 2; retry #2 in 0.050s
[wf-xxx/y] PENDING -> SKIPPED | reason=DEPENDENCY_FAILED | upstream 'x' is FAILED
[wf-xxx/b] RUNNING -> READY | reason=CRASH_RECOVERY | RUNNING residue after process restart; replayed without consuming retry budget
```

测试默认开启实时日志（`pytest.ini` 的 `log_cli` + conftest 的 StreamHandler），
运行 `uv run pytest` 即可看到任务标识、状态转移与判定依据。

---

## 7. 测试覆盖

| 文件 | 覆盖点 |
| --- | --- |
| `tests/test_validation.py` | 环、自依赖、未知依赖、重名（错误原因可区分） |
| `tests/test_idempotency.py` | 同键重复提交、并发重复提交、键复用冲突、运行中重复提交 |
| `tests/test_failures.py` | 不可重试立即终止、重试耗尽、失败级联（含隔代）、取消级联、失败/取消优先级、并发上限、同任务不双跑 |
| `tests/test_recovery.py` | 成功任务不重跑、未完成状态保留、重放不耗预算、终态不可逆、重启后幂等 |
| `tests/test_api.py` | HTTP 提交/查询、可区分错误码、取消终态不可逆 |

本地验证：

```bash
uv run pytest                      # 全量
uv run pytest -s -k recovery       # 只看重启恢复并观察转移日志
uv run python demo.py              # 不依赖 HTTP 的完整演示
```

## 8. 代码结构

```
main.py                 uvicorn 入口
demo.py                 本地端到端演示
app/
  models.py             状态枚举、定义/实体模型、FailureReason
  errors.py             SubmissionError / 不可重试 & 可重试异常
  validation.py         重名 / 未知依赖 / 环检测（Kahn）
  retry.py              指数退避计算
  store.py              JSON 原子存储 + 互斥事务
  handlers/base.py      TaskContext / handler 协议
  handlers/builtin.py   success/fail/flaky/gate 等内置处理器
  scheduler.py          状态推进、依赖级联、并发、worker、崩溃恢复
  service.py            提交幂等、查询、取消
  api.py                FastAPI 路由与错误码映射
tests/                  单测（夹具含崩溃-重建辅助）
```
