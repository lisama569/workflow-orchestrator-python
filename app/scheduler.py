"""调度器：状态推进、依赖级联、并发控制、退避重试与 worker 生命周期。

确定性规则汇总：
- 推进顺序：按 workflow_id、task_name 排序，依赖级联在单个 tick 内
  反复收敛到不动点，因此结论与提交顺序/线程时序无关。
- 依赖判定优先级：FAILED > CANCELLED > SKIPPED > 未完成。
  下游不静默跳过，而是转为 SKIPPED，并携带 DEPENDENCY_FAILED 或
  DEPENDENCY_CANCELLED 原因及首个阻断依赖名。
- 显式取消优先于依赖级联（未开始的任务收到取消后立即 CANCELLED）。
- 并发：全局信号量 + 每工作流 max_concurrency 双重上限；同一任务
  RUNNING 期间不会被二次调度（状态守卫 + worker 登记表）。
- 重试：可重试失败转 RETRYING 并按指数退避调度；不可重试失败立即
  FAILED/NON_RETRYABLE；超过上限 FAILED/RETRY_EXHAUSTED。
- 崩溃恢复：重启时残留 RUNNING 任务回到 READY 立即重放（at-least-once，
  SUCCEEDED 已固化不会重跑），RETRYING 任务退避清零立即获得一次机会，
  均不消耗重试预算（attempts 不变）。
"""
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

from app.handlers.base import HandlerRegistry, TaskContext
from app.models import (
    TASK_TERMINAL_STATES,
    WORKFLOW_TERMINAL_STATES,
    FailureReason,
    TaskEntity,
    TaskState,
    Transition,
    WorkflowEntity,
    WorkflowState,
    utcnow_iso,
)
from app.retry import backoff_delay
from app.store import JsonStateStore

logger = logging.getLogger("orchestrator")

# 非失败类的转移原因（FailureReason 枚举之外的判定依据说明）
REASON_SUBMITTED = "SUBMITTED"
REASON_DEPS_MET = "DEPS_MET"
REASON_START = "START"
REASON_RETRY_SCHEDULED = "RETRY_SCHEDULED"
REASON_CRASH_RECOVERY = "CRASH_RECOVERY"


class Scheduler:
    def __init__(
        self,
        store: JsonStateStore,
        registry: HandlerRegistry,
        tick_interval: float = 0.05,
        max_global_concurrency: int = 16,
        clock: Callable[[], float] | None = None,
    ):
        self._store = store
        self._registry = registry
        self._tick_interval = tick_interval
        self._max_global = max_global_concurrency
        self._clock = clock or time.monotonic
        # condition 绑定存储锁，所有状态读写都在这把锁上串行化
        self._cond = threading.Condition(store.lock())
        self._threads: dict[tuple[str, str], dict] = {}
        self._thread: threading.Thread | None = None
        self._stopped = True

    # ---------- 生命周期 ----------

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stopped = False
        self._thread = threading.Thread(
            target=self._run, name="scheduler", daemon=True
        )
        self._thread.start()

    def stop(self, drain_workers: bool = True, timeout: float = 10.0) -> None:
        """停止调度循环。

        drain_workers=True : 等待在途 worker 正常完成并落盘（优雅停机）。
        drain_workers=False: 通知在途 worker 放弃落盘立即退出，用于模拟
                             进程崩溃：磁盘保留最后的 RUNNING 快照。
        """
        with self._cond:
            self._stopped = True
            self._cond.notify_all()
            if not drain_workers:
                # 崩溃模拟：只设置内部 abort 信号，绝不设置业务 cancel_event，
                # 避免 worker 把“崩溃”误记为“用户取消”。
                for info in self._threads.values():
                    info["aborted"] = True
                    info["abort_event"].set()

        deadline = self._clock() + timeout
        if drain_workers:
            with self._cond:
                while self._threads and self._clock() < deadline:
                    self._cond.wait(timeout=0.05)
        else:
            for info in list(self._threads.values()):
                remaining = max(0.0, deadline - self._clock())
                info["thread"].join(timeout=remaining)

        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def notify(self) -> None:
        """提交/取消后唤醒调度循环。"""
        with self._cond:
            self._cond.notify_all()

    def request_cancel(
        self, wf_id: str, task_name: str | None, reason: FailureReason
    ) -> None:
        """投递取消信号：未开始任务置标记（tick 立即终结），
        RUNNING 任务额外置位 cancel_event（协作式取消）。调用前应已持存储锁。"""
        data = self._store.load_all()
        wf = data.get(wf_id)
        if wf is None:
            return
        targets = (
            list(wf.tasks.values()) if task_name is None else [wf.tasks.get(task_name)]
        )
        for task in targets:
            if task is None or task.state in TASK_TERMINAL_STATES:
                continue
            task.cancel_requested = True
            if task.cancel_reason is None:
                task.cancel_reason = reason
            if task.state == TaskState.RUNNING:
                info = self._threads.get((wf_id, task.name))
                if info is not None:
                    info["ctx"].cancel_event.set()

    def run_once(self) -> None:
        """同步执行一次调度推进（测试与提交后即时可见性用）。"""
        self._tick()
        with self._cond:
            self._cond.notify_all()

    def wait_for_terminal(
        self, workflow_id: str, timeout: float = 10.0
    ) -> WorkflowEntity | None:
        """阻塞等待某工作流进入终态（测试/本地演示用）。"""
        deadline = self._clock() + timeout
        with self._cond:
            while self._clock() < deadline:
                data = self._store.load_all()
                wf = data.get(workflow_id)
                if wf is not None and wf.state in WORKFLOW_TERMINAL_STATES:
                    return wf
                self._cond.wait(timeout=0.05)
            return data.get(workflow_id) if (data := self._store.load_all()) else None

    # ---------- 恢复 ----------

    def recover(self) -> int:
        """重启恢复：RUNNING 残留 -> READY 重放；RETRYING 退避清零。

        必须在 start() 之前调用。返回恢复的 RUNNING 任务数。
        """
        recovered = 0
        now = self._clock()
        with self._store.transaction() as data:
            for wf in data.values():
                for task in wf.tasks.values():
                    if task.state == TaskState.RUNNING:
                        self._set_state(
                            wf,
                            task,
                            TaskState.READY,
                            reason=REASON_CRASH_RECOVERY,
                            detail=(
                                "RUNNING residue after process restart; "
                                "replayed without consuming retry budget"
                            ),
                        )
                        recovered += 1
                    elif task.state == TaskState.RETRYING:
                        task.scheduled_at = now
                self._recompute_workflow_state(wf)
        logger.info("recovery complete: %d RUNNING task(s) replayed", recovered)
        return recovered

    # ---------- 主循环 ----------

    def _run(self) -> None:
        while True:
            with self._cond:
                while not self._stopped:
                    self._cond.wait(timeout=self._tick_interval)
                    break
                if self._stopped:
                    return
            try:
                self._tick()
            except Exception:  # pragma: no cover - 防御性：循环不能死
                logger.exception("scheduler tick failed")

    def _tick(self) -> None:
        with self._store.transaction() as data:
            candidates: list[tuple[str, str]] = []
            for wf_id in sorted(data):
                wf = data[wf_id]
                candidates.extend(self._evaluate_workflow(wf))

            # 并发受控地启动候选任务，顺序确定：(workflow_id, task_name)
            candidates.sort()
            per_wf_running: dict[str, int] = {}
            for wf_id, wf in data.items():
                per_wf_running[wf_id] = sum(
                    1
                    for t in wf.tasks.values()
                    if t.state == TaskState.RUNNING
                )
            global_running = len(self._threads)
            for wf_id, task_name in candidates:
                if global_running >= self._max_global:
                    break
                wf = data[wf_id]
                if per_wf_running[wf_id] >= wf.max_concurrency:
                    continue
                task = wf.tasks[task_name]
                if task.state != TaskState.READY:
                    continue  # 状态守卫：同一任务绝不重复启动
                self._spawn(wf, task)
                per_wf_running[wf_id] += 1
                global_running += 1

    # ---------- 依赖级联与工作流状态 ----------

    def _evaluate_workflow(self, wf: WorkflowEntity) -> list[tuple[str, str]]:
        """把工作流内任务推进到当前时刻的不动点，返回可启动候选。"""
        candidates: list[tuple[str, str]] = []
        changed = True
        now = self._clock()
        while changed:
            changed = False
            for name in sorted(wf.tasks):
                task = wf.tasks[name]
                if task.state in TASK_TERMINAL_STATES:
                    continue

                # 规则 1：显式取消优先（RUNNING 由 worker 协作处理）
                if task.state != TaskState.RUNNING and task.cancel_requested:
                    self._set_state(
                        wf,
                        task,
                        TaskState.CANCELLED,
                        reason=task.cancel_reason or FailureReason.TASK_CANCELLED,
                        detail="cancel requested before execution started",
                    )
                    changed = True
                    continue

                if task.state == TaskState.RUNNING:
                    continue

                # 规则 2：依赖判定
                verdict = self._dependency_verdict(wf.tasks, task)
                if verdict[0] == "bad":
                    _, dep_name, dep_state = verdict
                    if dep_state == TaskState.FAILED:
                        reason = FailureReason.DEPENDENCY_FAILED
                    else:
                        reason = FailureReason.DEPENDENCY_CANCELLED
                    detail = f"upstream '{dep_name}' is {dep_state.value}"
                    task.failure_detail = detail
                    self._set_state(
                        wf,
                        task,
                        TaskState.SKIPPED,
                        reason=reason,
                        detail=detail,
                    )
                    changed = True
                    continue
                if verdict[0] == "blocked":
                    continue

                # 规则 3：全部依赖成功
                if task.state == TaskState.PENDING:
                    self._set_state(
                        wf,
                        task,
                        TaskState.READY,
                        reason=REASON_DEPS_MET,
                        detail="all dependencies succeeded",
                    )
                    changed = True
                if task.state == TaskState.READY:
                    candidates.append((wf.id, task.name))
                elif (
                    task.state == TaskState.RETRYING
                    and task.scheduled_at is not None
                    and now >= task.scheduled_at
                ):
                    # 退避到期：经 READY 再启动，保留 attempts 计数
                    self._set_state(
                        wf,
                        task,
                        TaskState.READY,
                        reason=REASON_RETRY_SCHEDULED,
                        detail="backoff elapsed, eligible for another attempt",
                    )
                    candidates.append((wf.id, task.name))

        self._recompute_workflow_state(wf)
        return candidates

    @staticmethod
    def _dependency_verdict(
        tasks: dict[str, TaskEntity], task: TaskEntity
    ) -> tuple:
        """返回 ("ok"|"blocked"|"bad", dep_name, effective_state)。

        阻断优先级（确定性）：
            FAILED 与“因失败而 SKIPPED”视为失败并沿链传播（最高）；
            CANCELLED 次之；因取消而 SKIPPED 最后。
        因此失败隔代下游始终得到 DEPENDENCY_FAILED，不会被中间的 SKIPPED
        节点改写成取消原因。其余未完成状态构成 blocked。
        """
        rank = {
            TaskState.FAILED: 0,
            TaskState.CANCELLED: 2,
            TaskState.SKIPPED: 3,
        }
        best_bad: tuple[str, TaskState, int] | None = None

        def is_failure_skipped(t: TaskEntity) -> bool:
            return (
                t.state == TaskState.SKIPPED
                and t.failure_reason == FailureReason.DEPENDENCY_FAILED
            )

        all_succeeded = True
        for dep_name in task.deps:
            dep = tasks.get(dep_name)
            if dep is None:  # 理论不可达：提交时已校验
                return ("bad", dep_name, TaskState.FAILED)
            if dep.state == TaskState.SUCCEEDED:
                continue
            if is_failure_skipped(dep):
                effective, dep_rank = TaskState.FAILED, 1
            elif dep.state in rank:
                effective, dep_rank = dep.state, rank[dep.state]
            else:
                all_succeeded = False
                continue
            if best_bad is None or dep_rank < best_bad[2]:
                best_bad = (dep_name, effective, dep_rank)
        if best_bad is not None:
            return ("bad", best_bad[0], best_bad[1])
        return ("ok", None, None) if all_succeeded else ("blocked", None, None)

    def _recompute_workflow_state(self, wf: WorkflowEntity) -> None:
        """终态判定：有 FAILED -> FAILED；全终态且有取消/跳过 -> CANCELLED；
        全 SUCCEEDED -> SUCCEEDED；否则 RUNNING。"""
        states = [t.state for t in wf.tasks.values()]
        new_state: WorkflowState | None = None
        reason: FailureReason | str | None = None
        if any(s == TaskState.FAILED for s in states):
            new_state = WorkflowState.FAILED
            reason = "TASK_FAILED"
        elif all(s in TASK_TERMINAL_STATES for s in states):
            if any(s in (TaskState.CANCELLED, TaskState.SKIPPED) for s in states):
                new_state = WorkflowState.CANCELLED
                reason = "TASK_CANCELLED_OR_SKIPPED"
            else:
                new_state = WorkflowState.SUCCEEDED
                reason = "ALL_TASKS_SUCCEEDED"
        if new_state is not None and wf.state != new_state:
            old = wf.state
            wf.state = new_state
            self._record_event(
                wf,
                to_state=new_state.value,
                reason=reason,
                detail=f"workflow aggregated from {old.value}",
            )
            logger.info(
                "[%s/<workflow>] %s -> %s | reason=%s",
                wf.id, old.value, new_state.value, reason,
            )

    # ---------- worker ----------

    def _spawn(self, wf: WorkflowEntity, task: TaskEntity) -> None:
        """调用前已持锁。置 RUNNING -> 固化磁盘 -> 再启动线程。"""
        task.attempts += 1
        try:
            handler = self._registry.get(task.handler)
        except KeyError:
            self._set_state(
                wf,
                task,
                TaskState.FAILED,
                reason=FailureReason.NON_RETRYABLE,
                detail=f"no handler registered for '{task.handler}'",
            )
            return

        abort_event = threading.Event()
        ctx = TaskContext(
            workflow_id=wf.id,
            task_name=task.name,
            attempt=task.attempts,
            payload=task.payload,
            cancel_event=threading.Event(),
            abort_event=abort_event,
        )
        self._set_state(
            wf,
            task,
            TaskState.RUNNING,
            reason=REASON_START,
            detail=f"attempt {task.attempts} started",
        )
        thread = threading.Thread(
            target=self._worker,
            args=(wf.id, task.name, ctx, handler, abort_event),
            name=f"worker:{wf.id}:{task.name}",
            daemon=True,
        )
        info = {
            "thread": thread,
            "ctx": ctx,
            "abort_event": abort_event,
            "aborted": False,
        }
        self._threads[(wf.id, task.name)] = info
        # RUNNING 必须在业务代码开始执行前持久化（崩溃可重放的前提）
        self._store.persist(wf)
        thread.start()

    def _worker(
        self,
        wf_id: str,
        name: str,
        ctx: TaskContext,
        handler,
        abort_event: threading.Event,
    ) -> None:
        result = None
        error: BaseException | None = None
        try:
            result = handler(ctx)
        except BaseException as exc:  # noqa: BLE001 - 处理器任意异常都要收敛为任务失败
            error = exc

        # 崩溃式停机路径：放弃落盘，磁盘保留 RUNNING 残留
        with self._store.lock():
            info = self._threads.get((wf_id, name))
            if info is not None and info.get("aborted"):
                self._threads.pop((wf_id, name), None)
                self._cond.notify_all()
                return

        try:
            with self._store.transaction() as data:
                info = self._threads.pop((wf_id, name), None)
                if info is None or info.get("aborted"):
                    return
                wf = data.get(wf_id)
                if wf is None:
                    return
                task = wf.tasks.get(name)
                if task is None or task.state != TaskState.RUNNING:
                    # 终态守卫：任务已被取消/恢复等路径处理，不覆盖结论
                    return
                self._apply_outcome(wf, task, ctx, result, error)
        except Exception:  # pragma: no cover
            logger.exception("failed to finalize task %s/%s", wf_id, name)
        finally:
            with self._cond:
                self._cond.notify_all()

    def _apply_outcome(
        self,
        wf: WorkflowEntity,
        task: TaskEntity,
        ctx: TaskContext,
        result: dict | None,
        error: BaseException | None,
    ) -> None:
        # 协作式取消优先于执行结果
        if ctx.cancel_event.is_set() or task.cancel_requested:
            self._set_state(
                wf,
                task,
                TaskState.CANCELLED,
                reason=task.cancel_reason or FailureReason.TASK_CANCELLED,
                detail="cancel observed during execution",
            )
            return

        if error is not None:
            error_type = getattr(error, "error_type", type(error).__name__)
            detail = str(error) or error_type
            task.error_type = error_type
            task.failure_detail = detail

            from app.errors import NonRetryableTaskError

            if isinstance(error, NonRetryableTaskError):
                self._set_state(
                    wf,
                    task,
                    TaskState.FAILED,
                    reason=FailureReason.NON_RETRYABLE,
                    detail=detail,
                )
                return

            policy = task.retry_policy
            if task.attempts <= policy.max_retries:
                delay = backoff_delay(policy, task.attempts)
                task.scheduled_at = self._clock() + delay
                self._set_state(
                    wf,
                    task,
                    TaskState.RETRYING,
                    reason=REASON_RETRY_SCHEDULED,
                    detail=(
                        f"retryable {error_type} on attempt {task.attempts}; "
                        f"retry #{task.attempts} in {delay:.3f}s"
                    ),
                )
            else:
                self._set_state(
                    wf,
                    task,
                    TaskState.FAILED,
                    reason=FailureReason.RETRY_EXHAUSTED,
                    detail=(
                        f"{error_type} after {task.attempts} attempt(s); "
                        f"max_retries={policy.max_retries}"
                    ),
                )
            return

        task.result = result
        self._set_state(
            wf,
            task,
            TaskState.SUCCEEDED,
            reason="EXECUTION_OK",
            detail=f"attempt {task.attempts} returned result",
        )

    # ---------- 事件与日志 ----------

    def _set_state(
        self,
        wf: WorkflowEntity,
        task: TaskEntity,
        new_state: TaskState,
        reason,
        detail: str = "",
    ) -> None:
        old = task.state
        task.state = new_state
        task.updated_at = utcnow_iso()
        if new_state == TaskState.RETRYING:
            task.failure_reason = None
        elif new_state in TASK_TERMINAL_STATES:
            task.failure_reason = reason if isinstance(reason, FailureReason) else None
        self._record_event(
            wf,
            to_state=new_state.value,
            reason=reason,
            detail=detail,
            task_name=task.name,
            from_state=old.value,
        )
        logger.info(
            "[%s/%s] %s -> %s | reason=%s | %s",
            wf.id, task.name, old.value, new_state.value,
            reason.value if isinstance(reason, FailureReason) else reason,
            detail,
        )

    def _record_event(
        self,
        wf: WorkflowEntity,
        to_state: str,
        reason,
        detail: str = "",
        task_name: str | None = None,
        from_state: str | None = None,
    ) -> None:
        wf.event_seq += 1
        wf.events.append(
            Transition(
                seq=wf.event_seq,
                task=task_name,
                workflow_id=wf.id,
                from_state=from_state,
                to_state=to_state,
                reason=reason.value if isinstance(reason, FailureReason) else str(reason),
                detail=detail,
            )
        )
