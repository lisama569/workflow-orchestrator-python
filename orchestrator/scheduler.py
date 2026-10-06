"""调度循环:依赖推进、退避重试、并发上限、崩溃恢复。

关键不变量:
  1. 任务只有在【全部依赖 SUCCEEDED】时才会被派发;
  2. 依赖 FAILED/SKIPPED/CANCELLED 时,下游 PENDING 任务被确定性地标记 SKIPPED;
  3. 全局在执行任务数永不超过 max_concurrency;同一 (工作流,任务) 同时只执行一次;
  4. 可重试失败按指数退避重试到 max_attempts,不可重试失败立即 FAILED 终态;
  5. 成功/终态落盘后进程重启不重跑;崩溃时处于 RUNNING 的任务回退 PENDING 重调度;
  6. 取消/终止通过持久化的 cancel_requested 标志表达,不可逆;终止立即进入终态;
  7. 锁序全局统一为【工作流锁 -> 存储锁 -> 在执行表锁】:tick、任务完成回调、
     单工作流取消/终止均先取工作流锁再进存储事务;组事务不取工作流锁,
     中断信号在事务提交后按成员加锁补发。取消/终止因此可与任务执行任意交错而不死锁。
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field

from .errors import TerminalStateError
from .executor import ExecutionResult, Executor, FailureKind, TaskCancelled
from .logging_setup import log_transition
from .models import (
    GroupStatus,
    TaskState,
    TaskStatus,
    WorkflowGroupState,
    WorkflowState,
    WorkflowStatus,
)
from .store import JsonStore

logger = logging.getLogger("orchestrator.scheduler")

_BAD_DEP_STATUSES = frozenset(
    {TaskStatus.FAILED, TaskStatus.SKIPPED, TaskStatus.CANCELLED}
)
_TASK_TERMINAL = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.SKIPPED, TaskStatus.CANCELLED}
)


@dataclass
class _Inflight:
    """一次在执行任务的运行时句柄(不持久化)。"""

    workflow_id: str
    task_id: str
    attempt: int
    cancel_event: threading.Event = field(default_factory=threading.Event)
    future: Future | None = None


class Scheduler:
    """跨工作流的本地线程池调度器。"""

    def __init__(
        self,
        store: JsonStore,
        executor: Executor | None = None,
        max_concurrency: int = 4,
        tick_seconds: float = 0.02,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self.store = store
        self.executor = executor or Executor()
        self.max_concurrency = max_concurrency
        self.tick_seconds = tick_seconds
        self._pool = ThreadPoolExecutor(max_workers=max_concurrency, thread_name_prefix="wf-task")
        self._running: dict[tuple[str, str], _Inflight] = {}
        self._running_lock = threading.Lock()
        self._wf_locks: dict[str, threading.Lock] = {}
        self._wf_locks_guard = threading.Lock()
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: threading.Thread | None = None

    # ================= 生命周期 =================
    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self.recover()
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, name="wf-scheduler", daemon=True)
        self._thread.start()
        logger.info("scheduler started (max_concurrency=%d)", self.max_concurrency)

    def stop(self, wait: bool = True) -> None:
        self._stop_event.set()
        self._wake_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        self._pool.shutdown(wait=wait, cancel_futures=False)

    def recover(self) -> None:
        """进程重启恢复。

        - 终态工作流/任务原样保留(SUCCEEDED 绝不重跑);
        - RUNNING 任务来自崩溃中断,回退 PENDING 立即重调度(结论未持久化=未完成);
        - 退避中的 PENDING、持久化的 cancel_requested 均保留,由 tick 继续收敛。
        """
        now = time.time()
        workflows = self.store.load_all()
        for wf_id, wf in workflows.items():
            if wf.status.is_terminal:
                # 终止已落盘但在执行任务的完成回调尚未落盘(进程崩溃)时,
                # 终态工作流上可能残留 RUNNING/PENDING 任务:终态不可逆,
                # 结论确定为 CANCELLED,这里确定性补齐,绝不重跑。
                reopen = self._sweep_terminal_cancelled_tasks(wf, now)
                if reopen:
                    wf.updated_at = now
                    self.store.save_workflow(wf)
                    logger.info(
                        "recovery: terminal workflow=%s swept unfinished tasks to CANCELLED", wf_id
                    )
                else:
                    logger.info(
                        "recovery: workflow=%s kept terminal status=%s", wf_id, wf.status.value
                    )
                continue
            changed = False
            for tid, t in wf.tasks.items():
                if t.status == TaskStatus.RUNNING:
                    self._set_task(
                        t,
                        TaskStatus.PENDING,
                        "crash recovery: RUNNING->PENDING, no durable conclusion, safe to rerun",
                        now,
                    )
                    t.started_at = None
                    t.next_run_at = now
                    changed = True
            if changed:
                wf.updated_at = now
                self.store.save_workflow(wf)
                logger.info("recovery: workflow=%s reopened, pending rerun of crashed tasks", wf_id)
        for gid, g in self.store.load_groups().items():
            if g.status.is_terminal:
                logger.info("recovery: group=%s kept terminal status=%s", gid, g.status.value)
            else:
                # 组结论由成员状态 + 持久化的取消标志确定性推导,tick 会继续收敛
                logger.info(
                    "recovery: group=%s status=%s will be re-evaluated from member states",
                    gid,
                    g.status.value,
                )

    def _sweep_terminal_cancelled_tasks(self, wf: WorkflowState, now: float) -> bool:
        """终态工作流恢复时的确定性补齐。

        仅对 CANCELLED 终态(取消/终止)有意义:终止落盘后、完成回调落盘前崩溃,
        会留下 RUNNING(或退避 PENDING)任务。终态不可逆,这些任务一律 CANCELLED,
        不回退、不重跑。返回是否发生了补齐(需要落盘)。
        """
        if wf.status != WorkflowStatus.CANCELLED:
            return False
        changed = False
        for t in wf.tasks.values():
            if t.status in (TaskStatus.RUNNING, TaskStatus.PENDING):
                was_running = t.status == TaskStatus.RUNNING
                self._set_task(
                    t,
                    TaskStatus.CANCELLED,
                    "crash recovery: workflow already terminal, unfinished task closed as cancelled",
                    now,
                )
                t.finished_at = now
                if not was_running:
                    t.started_at = None
                changed = True
        return changed

    def notify(self) -> None:
        self._wake_event.set()

    @property
    def running_count(self) -> int:
        with self._running_lock:
            return len(self._running)

    # ================= 控制面 =================
    def request_cancel(self, workflow_id: str, reason: str = "cancel requested by client") -> None:
        """优雅取消(不可逆):不再派发新任务;未开始任务立即 CANCELLED;
        执行中的任务自然结束后工作流收敛为 CANCELLED。重复取消幂等。"""
        self._request_cancel(workflow_id, reason, force_terminal=False)

    def request_terminate(self, workflow_id: str, reason: str = "terminate requested by client") -> None:
        """立即终止(不可逆):未开始任务立即 CANCELLED;执行中的任务收到中断信号,
        其完成结果一律按 CANCELLED 收敛;工作流立即进入 CANCELLED 终态。"""
        self._request_cancel(workflow_id, reason, force_terminal=True)

    def _request_cancel(self, workflow_id: str, reason: str, force_terminal: bool) -> None:
        wf_lock = self._lock_for(workflow_id)
        affected = False

        def _apply(wf: WorkflowState) -> None:
            nonlocal affected
            now = time.time()
            if wf.status == WorkflowStatus.CANCELLED or wf.cancel_requested:
                # 已接受过取消/终止:本次可能是"先取消后终止",需要升级为立即终态;
                # 否则就是重复请求,幂等 no-op。
                if force_terminal and not wf.status.is_terminal:
                    # "先优雅取消、后立即终止":升级为立即终态,
                    # 退避/未派发的 PENDING 任务立即 CANCELLED,在执行任务由中断信号收尾。
                    for t in wf.tasks.values():
                        if t.status == TaskStatus.PENDING:
                            self._set_task(
                                t,
                                TaskStatus.CANCELLED,
                                "workflow termination escalated before task started",
                                now,
                            )
                    self._set_wf(wf, WorkflowStatus.CANCELLED, f"terminated: {reason}", now)
                    wf.reason = reason
                    wf.cancel_reason = reason
                    wf.updated_at = now
                    affected = True
                else:
                    logger.info(
                        "workflow=%s cancel is already effective; idempotent no-op", workflow_id
                    )
                return
            if wf.status.is_terminal:
                raise TerminalStateError(workflow_id, wf.status.value)
            self._apply_cancel_to_wf(wf, reason, force_terminal, now)
            affected = True

        # 锁顺序约定:一律 工作流锁 -> 存储锁(与 tick、完成回调一致)。
        # 绝不能在 store.mutate 事务内再获取工作流锁,否则会与完成回调
        # (工作流锁 -> 存储锁)构成 AB-BA,在任务执行完成的瞬间取消即可卡死整个服务。
        inflights: list[_Inflight] = []
        with wf_lock:
            self.store.mutate(workflow_id, _apply)
            if affected:
                # 在工作流锁内读在执行表(与 _tick_locked 一致的锁序),
                # 此时新任务还没被派发、旧任务完成回调拿不到工作流锁,读到的集合确定。
                with self._running_lock:
                    inflights = [
                        inf for (wid, _tid), inf in self._running.items() if wid == workflow_id
                    ]
                for inf in inflights:
                    inf.cancel_event.set()
        logger.info(
            "workflow=%s %s; %d in-flight task(s) signalled",
            workflow_id,
            "terminated" if force_terminal else "cancelled",
            len(inflights) if affected else 0,
        )
        self.notify()

    def _apply_cancel_to_wf(
        self, wf: WorkflowState, reason: str, force_terminal: bool, now: float
    ) -> None:
        """对单个工作流应用取消/终止的纯状态变更(调用方保证持锁与落盘)。

        未开始任务立即 CANCELLED;force_terminal 时工作流立即进入 CANCELLED 终态,
        在执行任务的结果稍后一律按 CANCELLED 收敛。已终态工作流不应传入。
        """
        wf.cancel_requested = True
        wf.cancel_reason = reason
        wf.updated_at = now
        self._set_wf(wf, WorkflowStatus.RUNNING, f"cancel accepted: {reason}", now)
        for t in wf.tasks.values():
            if t.status == TaskStatus.PENDING:
                self._set_task(
                    t,
                    TaskStatus.CANCELLED,
                    "workflow cancel requested before task started",
                    now,
                )
        if force_terminal:
            # 终止:工作流立即终态;在执行任务的结果稍后一律按 CANCELLED 收敛。
            self._set_wf(wf, WorkflowStatus.CANCELLED, f"terminated: {reason}", now)

    def _escalate_wf_terminal(
        self, wf: WorkflowState, reason: str, now: float
    ) -> bool:
        """工作流已接受过优雅取消、尚未终态,此时收到立即终止:升级为立即终态。

        已 PENDING(含退避等待)的任务立即 CANCELLED,在执行任务由中断信号收尾。
        返回是否真的发生了升级(调用方据此决定是否要补发中断信号)。
        """
        if wf.status.is_terminal or not wf.cancel_requested:
            return False
        wf.cancel_reason = reason
        wf.updated_at = now
        for t in wf.tasks.values():
            if t.status == TaskStatus.PENDING:
                self._set_task(
                    t,
                    TaskStatus.CANCELLED,
                    "workflow termination escalated before task started",
                    now,
                )
        self._set_wf(wf, WorkflowStatus.CANCELLED, f"terminated: {reason}", now)
        wf.reason = reason
        return True

    # ================= 组级控制面 =================
    def request_group_cancel(self, group_id: str, reason: str = "cancel requested by client") -> None:
        """组级优雅取消(不可逆):作用到组内所有尚未完成的工作流,重复调用幂等。"""
        self._request_group_cancel(group_id, reason, force_terminal=False)

    def request_group_terminate(self, group_id: str, reason: str = "terminate requested by client") -> None:
        """组级立即终止(不可逆):组与未完成的成员立即进入终态,重复调用幂等。"""
        self._request_group_cancel(group_id, reason, force_terminal=True)

    def _request_group_cancel(self, group_id: str, reason: str, force_terminal: bool) -> None:
        now = time.time()
        affected: list[str] = []

        def _txn(
            wfs: dict[str, WorkflowState], groups: dict[str, WorkflowGroupState]
        ) -> None:
            g = groups.get(group_id)
            if g is None:
                raise KeyError(group_id)  # 服务层已先校验存在
            if g.status == GroupStatus.CANCELLED or g.cancel_requested:
                if force_terminal and not g.status.is_terminal:
                    # "先优雅取消、后立即终止":把已取消但还没跑完的成员升级为立即终态。
                    self._set_group(g, GroupStatus.CANCELLED, f"terminated: {reason}", now)
                    g.reason = reason
                    g.updated_at = now
                    member_reason = f"group {group_id} terminate: {reason}"
                    for wid in g.workflow_ids:
                        wf = wfs.get(wid)
                        if wf is not None and not wf.status.is_terminal and self._escalate_wf_terminal(
                            wf, member_reason, now
                        ):
                            affected.append(wid)
                else:
                    logger.info("group=%s cancel is already effective; idempotent no-op", group_id)
                return
            if g.status.is_terminal:
                raise TerminalStateError(group_id, g.status.value)
            g.cancel_requested = True
            g.cancel_reason = reason
            g.updated_at = now
            self._set_group(g, GroupStatus.RUNNING, f"cancel accepted: {reason}", now)
            member_reason = f"group {group_id} cancel: {reason}"
            for wid in g.workflow_ids:
                wf = wfs.get(wid)
                # 已终态成员(如已 SUCCEEDED)原样保留,绝不影响其结论
                if wf is None or wf.status.is_terminal or wf.cancel_requested:
                    continue
                self._apply_cancel_to_wf(wf, member_reason, force_terminal, now)
                affected.append(wid)
            if force_terminal:
                # 终止:组立即终态;成员在执行任务的结果稍后一律按 CANCELLED 收敛。
                self._set_group(g, GroupStatus.CANCELLED, f"terminated: {reason}", now)
                g.reason = reason

        self.store.transact(_txn)
        # 状态已落盘,再给成员的在执行任务发中断信号(组事务不取工作流锁,
        # 这里按成员取工作流锁后读在执行表,锁序与其余路径一致)
        signalled = 0
        for wid in affected:
            wf_lock = self._lock_for(wid)
            with wf_lock, self._running_lock:
                inflights = [inf for (w, _), inf in self._running.items() if w == wid]
            for inf in inflights:
                inf.cancel_event.set()
                signalled += 1
        logger.info(
            "group=%s %s; %d member workflow(s) affected, %d in-flight task(s) signalled",
            group_id,
            "terminated" if force_terminal else "cancelled",
            len(affected),
            signalled,
        )
        self.notify()

    # ================= 调度面 =================
    def tick(self) -> None:
        """一次全局调度扫描。"""
        now = time.time()
        for wf_id in sorted(self.store.load_all().keys()):
            wf_lock = self._lock_for(wf_id)
            if not wf_lock.acquire(blocking=False):
                continue  # 该工作流正被完成回调修改,下一轮再看
            try:
                launches: list[tuple[TaskState, _Inflight]] = []

                def _scan(wf: WorkflowState) -> None:
                    launches.extend(self._tick_locked(wf, now))

                self.store.mutate(wf_id, _scan)
            finally:
                wf_lock.release()
            # 状态已先落盘,再派发执行(执行中崩溃最坏只是重复一次未完成任务)
            for t, inf in launches:
                self._submit(wf_id, t, inf, now)
        self._tick_groups(now)

    # ---------- 组级推进:全部成员终态后组才收敛 ----------
    def _tick_groups(self, now: float) -> None:
        for gid in sorted(self.store.load_groups().keys()):
            def _scan(
                wfs: dict[str, WorkflowState], groups: dict[str, WorkflowGroupState]
            ) -> None:
                g = groups.get(gid)
                if g is None or g.status.is_terminal:
                    return
                members = [wfs[wid] for wid in g.workflow_ids if wid in wfs]
                if g.cancel_requested:
                    # 崩溃恢复缺口:组取消已持久化但个别成员尚未应用,这里确定性补齐
                    for wf in members:
                        if not wf.status.is_terminal and not wf.cancel_requested:
                            self._apply_cancel_to_wf(
                                wf,
                                f"group {gid} cancel: {g.cancel_reason}",
                                False,
                                now,
                            )
                if g.status == GroupStatus.PENDING:
                    self._set_group(g, GroupStatus.RUNNING, "scheduler picked up group", now)
                self._conclude_group(g, members, now)
                g.updated_at = now

            self.store.transact(_scan)

    def _conclude_group(
        self, g: WorkflowGroupState, members: list[WorkflowState], now: float
    ) -> None:
        """组结论的确定规则(只在全部成员到达终态后评估):

        - 组级取消已生效            -> CANCELLED;
        - 全部成员 SUCCEEDED        -> SUCCEEDED;
        - 其余(有成员 FAILED/CANCELLED) -> FAILED,reason 列出导致结论的成员及其原因。
        """
        if g.status.is_terminal or not members:
            return
        if any(not wf.status.is_terminal for wf in members):
            return  # 还有成员未结束
        if g.cancel_requested:
            self._set_group(g, GroupStatus.CANCELLED, f"cancelled: {g.cancel_reason}", now)
            g.reason = g.cancel_reason
            return
        not_ok = [wf for wf in members if wf.status != WorkflowStatus.SUCCEEDED]
        if not not_ok:
            self._set_group(
                g, GroupStatus.SUCCEEDED, f"all {len(members)} workflow(s) succeeded", now
            )
            g.reason = None
            return
        details = "; ".join(
            f"{wf.id}={wf.status.value}({wf.reason or 'no reason'})"
            for wf in sorted(not_ok, key=lambda w: w.id)
        )
        reason = f"member workflow(s) not succeeded: {details}"
        self._set_group(g, GroupStatus.FAILED, reason, now)
        g.reason = reason

    def run_until_idle(self, timeout: float = 10.0) -> None:
        """测试/本地工具:反复 tick 直到没有在执行与待执行任务、组全部收敛。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            self.tick()
            pending = False
            for wf in self.store.load_all().values():
                if wf.status.is_terminal:
                    continue
                if any(t.status in (TaskStatus.RUNNING, TaskStatus.PENDING) for t in wf.tasks.values()):
                    pending = True
            for g in self.store.load_groups().values():
                if not g.status.is_terminal:
                    pending = True
            with self._running_lock:
                busy = bool(self._running)
            if not pending and not busy:
                return
            time.sleep(self.tick_seconds)
        raise TimeoutError("scheduler did not become idle within timeout")

    # ---------- 单个工作流的扫描(持 wf 锁;由 store.mutate 包裹落盘) ----------
    def _tick_locked(self, wf: WorkflowState, now: float) -> list[tuple[TaskState, _Inflight]]:
        if wf.status.is_terminal:
            return []
        if wf.status == WorkflowStatus.PENDING:
            self._set_wf(wf, WorkflowStatus.RUNNING, "scheduler picked up workflow", now)

        self._propagate(wf, now)
        launches: list[tuple[TaskState, _Inflight]] = []
        if not wf.cancel_requested:
            for tid in sorted(wf.tasks):
                with self._running_lock:
                    slots = self.max_concurrency - len(self._running)
                if slots <= 0:
                    logger.info("concurrency cap reached (%d); task=%s waits", self.max_concurrency, tid)
                    break
                t = wf.tasks[tid]
                if t.status != TaskStatus.PENDING or (t.next_run_at is not None and t.next_run_at > now):
                    continue
                if self._deps_ok(t, wf):
                    inf = _Inflight(wf.id, t.id, t.attempt + 1)
                    self._set_task(
                        t,
                        TaskStatus.RUNNING,
                        f"all dependencies succeeded; attempt {inf.attempt}/{t.max_attempts}",
                        now,
                    )
                    t.attempt = inf.attempt
                    t.started_at = now
                    t.next_run_at = None
                    with self._running_lock:
                        self._running[(wf.id, t.id)] = inf
                    launches.append((t, inf))
        self._conclude(wf, now)
        wf.updated_at = now
        return launches

    # ---------- 依赖结论的确定性传播 ----------
    def _propagate(self, wf: WorkflowState, now: float) -> None:
        changed = True
        while changed:  # 不动点传播:A 失败 -> B SKIPPED -> C SKIPPED
            changed = False
            for t in wf.tasks.values():
                if t.status != TaskStatus.PENDING:
                    continue
                if wf.cancel_requested:
                    self._set_task(
                        t,
                        TaskStatus.CANCELLED,
                        f"workflow cancel requested ({wf.cancel_reason})",
                        now,
                    )
                    t.skipped_by = self._bad_deps(t, wf)
                    changed = True
                    continue
                bad = self._bad_deps(t, wf)
                if bad:
                    self._set_task(
                        t,
                        TaskStatus.SKIPPED,
                        f"dependency never succeeded: {', '.join(bad)}",
                        now,
                    )
                    t.skipped_by = bad
                    changed = True

    @staticmethod
    def _bad_deps(t: TaskState, wf: WorkflowState) -> list[str]:
        return [
            d for d in t.dependencies if wf.tasks[d].status in _BAD_DEP_STATUSES
        ]

    @staticmethod
    def _deps_ok(task: TaskState, wf: WorkflowState) -> bool:
        """全部依赖 SUCCEEDED 才可执行。"""
        return all(wf.tasks[d].status == TaskStatus.SUCCEEDED for d in task.dependencies)

    # ---------- 工作流结论 ----------
    def _conclude(self, wf: WorkflowState, now: float) -> None:
        if wf.status.is_terminal:
            return
        statuses = [t.status for t in wf.tasks.values()]
        if any(s in (TaskStatus.RUNNING, TaskStatus.PENDING) for s in statuses):
            return  # 还有未结束任务
        if wf.cancel_requested:
            self._set_wf(wf, WorkflowStatus.CANCELLED, f"cancelled: {wf.cancel_reason}", now)
            wf.reason = wf.cancel_reason
            return
        failed_tasks = [t for t in wf.tasks.values() if t.status == TaskStatus.FAILED]
        skipped = sorted(t.id for t in wf.tasks.values() if t.status == TaskStatus.SKIPPED)
        if failed_tasks:
            details = "; ".join(
                f"{t.id}=[{t.error_type}] {t.error or 'failed'}" for t in sorted(failed_tasks, key=lambda x: x.id)
            )
            reason = f"task(s) failed terminally: {details}"
            if skipped:
                reason += f"; downstream skipped: {', '.join(skipped)}"
            self._set_wf(wf, WorkflowStatus.FAILED, reason, now)
            wf.reason = reason
        elif skipped:
            reason = f"all tasks skipped due to failed dependencies: {', '.join(skipped)}"
            self._set_wf(wf, WorkflowStatus.FAILED, reason, now)
            wf.reason = reason
        elif all(s == TaskStatus.SUCCEEDED for s in statuses):
            self._set_wf(wf, WorkflowStatus.SUCCEEDED, f"all {len(statuses)} task(s) succeeded", now)
            wf.reason = None
        else:  # 理论不可达:所有任务终态但出现意外组合
            reason = f"inconclusive terminal task statuses: {sorted({s.value for s in statuses})}"
            self._set_wf(wf, WorkflowStatus.FAILED, reason, now)
            wf.reason = reason

    # ================= 执行与回调 =================
    def _submit(self, wf_id: str, t: TaskState, inf: _Inflight, now: float) -> None:
        def _cancelled() -> bool:
            return inf.cancel_event.is_set()

        def _work() -> None:
            result: ExecutionResult
            try:
                result = self.executor.execute(
                    t.action, dict(t.params), inf.attempt, task_id=t.id, cancelled=_cancelled
                )
            except TaskCancelled:
                result = ExecutionResult.fail(FailureKind.NONE, "task interrupted by termination", "CANCELLED")
            except BaseException as exc:  # 执行器自身崩溃:按不可重试失败收敛,不吞线程
                logger.exception("task=%s executor crashed", t.id)
                result = ExecutionResult.fail(
                    FailureKind.NON_RETRYABLE, f"executor crashed: {exc!r}", "EXECUTOR_CRASHED"
                )
            # 锁顺序约定:一律 工作流锁 -> 存储锁(与 tick / request_cancel 一致),
            # 否则完成回调(存储锁->工作流锁)与 tick(工作流锁->存储锁)会 AB-BA 死锁。
            wf_lock = self._lock_for(wf_id)
            with wf_lock:
                self.store.mutate(
                    wf_id, lambda wf: self._apply_result(wf, t.id, inf.attempt, result, now0=time.time())
                )
            with self._running_lock:
                self._running.pop((wf_id, t.id), None)
            self.notify()

        inf.future = self._pool.submit(_work)

    def _apply_result(
        self,
        wf: WorkflowState,
        task_id: str,
        attempt: int,
        result: ExecutionResult,
        now0: float,
    ) -> None:
        """在 wf 锁 + 存储锁内应用一次执行结论,并立即传播/收敛/落盘。

        调用方(_work)已持有该工作流的锁,此处不再重复获取。
        """
        t = wf.tasks[task_id]
        if t.status != TaskStatus.RUNNING or t.attempt != attempt:
            logger.info(
                "task=%s stale completion ignored (status=%s attempt=%s vs %s)",
                task_id,
                t.status.value,
                t.attempt,
                attempt,
            )
            return
        # 终止优先:无论执行结果如何,运行中的任务一律 CANCELLED(终态不可逆)
        if wf.cancel_requested and result.error_type == "CANCELLED":
            self._finish_task(t, TaskStatus.CANCELLED, "interrupted by workflow termination", now0)
        elif wf.cancel_requested:
            self._finish_task(
                t, TaskStatus.CANCELLED, "workflow terminated; execution result discarded", now0
            )
        elif result.success:
            t.result = result.output
            self._finish_task(t, TaskStatus.SUCCEEDED, f"attempt {attempt} succeeded", now0)
        elif result.kind == FailureKind.NON_RETRYABLE:
            t.error = result.error
            t.error_type = result.error_type
            self._finish_task(
                t,
                TaskStatus.FAILED,
                f"non-retryable failure: [{result.error_type}] {result.error}",
                now0,
            )
        else:  # RETRYABLE
            t.error = result.error
            t.error_type = result.error_type
            if attempt >= t.max_attempts:
                self._finish_task(
                    t,
                    TaskStatus.FAILED,
                    f"retry budget exhausted after {attempt} attempt(s): {result.error}",
                    now0,
                )
            else:
                delay = t.retry_policy.delay_for_attempt(attempt)
                t.next_run_at = now0 + delay
                t.started_at = None
                self._set_task(
                    t,
                    TaskStatus.PENDING,
                    f"retryable failure [{t.error_type}] {t.error}; "
                    f"backoff {delay:.3f}s then retry {attempt + 1}/{t.max_attempts}",
                    now0,
                )
        self._propagate(wf, now0)
        self._conclude(wf, now0)
        wf.updated_at = now0

    # ================= 辅助 =================
    def _lock_for(self, wf_id: str) -> threading.Lock:
        with self._wf_locks_guard:
            lk = self._wf_locks.get(wf_id)
            if lk is None:
                lk = threading.Lock()
                self._wf_locks[wf_id] = lk
            return lk

    def _finish_task(self, t: TaskState, to: TaskStatus, reason: str, now: float) -> None:
        self._set_task(t, to, reason, now)
        t.finished_at = now
        t.started_at = t.started_at if to == TaskStatus.CANCELLED else t.started_at

    @staticmethod
    def _set_task(t: TaskState, to: TaskStatus, reason: str, now: float) -> None:
        frm = t.status
        if frm == to:
            return
        t.status = to
        entry = {"ts": now, "from": frm.value, "to": to.value, "reason": reason}
        t.transitions.append(entry)
        log_transition(logger, t.id, frm.value, to.value, reason)

    @staticmethod
    def _set_wf(wf: WorkflowState, to: WorkflowStatus, reason: str, now: float) -> None:
        frm = wf.status
        if frm == to:
            return
        wf.status = to
        entry = {"ts": now, "from": frm.value, "to": to.value, "reason": reason}
        wf.transitions.append(entry)
        logger.info("workflow=%s transition %s->%s reason=%s", wf.id, frm.value, to.value, reason)

    @staticmethod
    def _set_group(g: WorkflowGroupState, to: GroupStatus, reason: str, now: float) -> None:
        frm = g.status
        if frm == to:
            return
        g.status = to
        entry = {"ts": now, "from": frm.value, "to": to.value, "reason": reason}
        g.transitions.append(entry)
        logger.info("group=%s transition %s->%s reason=%s", g.id, frm.value, to.value, reason)

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.tick()
            except Exception:
                logger.exception("scheduler tick failed")
            self._wake_event.wait(timeout=self.tick_seconds)
            self._wake_event.clear()
