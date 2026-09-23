"""场景四:进程重启后的结论一致且可解释。

- SUCCEEDED 任务绝不重跑(用幂等动作记录执行次数证明);
- FAILED/SKIPPED/CANCELLED 终态原样恢复;
- 崩溃时 RUNNING 的任务回退 PENDING 并重新调度到终态;
- 退避中的 PENDING(带 next_run_at)继续按退避重试;
- 已持久化的取消请求在重启后仍被遵守。
"""
from __future__ import annotations

import threading
import time

from orchestrator.models import RetryPolicy, TaskDef, TaskStatus, WorkflowStatus


class _CountingPass:
    """每个任务只记录执行次数的确定性动作包装。"""

    def __init__(self):
        self.counts = {}
        self.guard = threading.Lock()
        from orchestrator.executor import Executor

        self._inner = Executor()

    def execute(self, action, params, attempt, task_id="", cancelled=None):
        with self.guard:
            self.counts[task_id] = self.counts.get(task_id, 0) + 1
        return self._inner.execute(action, params, attempt, task_id, cancelled)


def test_succeeded_tasks_are_not_rerun_after_restart(env):
    e = env()
    probe = _CountingPass()
    e.scheduler.executor = probe
    e.service.submit_workflow(
        "wf",
        [
            TaskDef(id="a", action="pass"),
            TaskDef(id="b", action="pass", dependencies=["a"]),
        ],
    )
    e.run()
    assert probe.counts == {"a": 1, "b": 1}

    # 模拟进程重启:全新调度器 + recover(),然后再跑一个调度周期
    sch2 = e.new_scheduler_on_same_store()
    e.scheduler = sch2
    e.service.scheduler = sch2
    e.run()
    wf = e.service.get_workflow("wf")
    assert wf.status == WorkflowStatus.SUCCEEDED
    assert wf.tasks["a"].status == TaskStatus.SUCCEEDED
    assert wf.tasks["b"].status == TaskStatus.SUCCEEDED
    assert probe.counts == {"a": 1, "b": 1}, "成功任务被重跑了!"


def test_terminal_states_survive_restart_and_are_explainable(env):
    e = env()
    e.service.submit_workflow(
        "wf",
        [
            TaskDef(id="a", action="fail", params={"kind": "non_retryable"}, retryable=False),
            TaskDef(id="b", action="pass", dependencies=["a"]),
        ],
    )
    e.run()
    before = e.service.get_workflow("wf")
    assert before.status == WorkflowStatus.FAILED

    sch2 = e.new_scheduler_on_same_store()
    e.scheduler = sch2
    e.service.scheduler = sch2
    e.run()
    after = e.service.get_workflow("wf")
    assert after.status == WorkflowStatus.FAILED
    assert after.tasks["a"].status == TaskStatus.FAILED
    assert after.tasks["a"].attempt == 1
    assert after.tasks["a"].error_type == "SIMULATED_FAILURE"
    assert after.tasks["b"].status == TaskStatus.SKIPPED
    assert after.tasks["b"].skipped_by == ["a"]
    # 状态转移记录完整,判定依据可解释
    assert after.tasks["b"].transitions[-1]["to"] == "SKIPPED"
    assert after.reason == before.reason


def test_running_task_at_crash_is_reevaluated_and_completes(env):
    """磁盘上停在 RUNNING(进程在执行中途被杀),恢复后回退 PENDING 并跑到终态。"""
    e = env()
    e.service.submit_workflow("wf", [TaskDef(id="a", action="pass")])
    # 制造崩溃现场:磁盘上的任务结论停在 RUNNING
    e.store.mutate("wf", lambda wf: setattr(wf.tasks["a"], "status", TaskStatus.RUNNING))

    from orchestrator.executor import Executor
    from orchestrator.scheduler import Scheduler

    fresh_store = type(e.store)(e.path)
    assert fresh_store.get("wf").tasks["a"].status == TaskStatus.RUNNING
    new_sched = Scheduler(fresh_store, Executor(), max_concurrency=4, tick_seconds=0.005)
    new_sched.recover()
    e.store = fresh_store
    e.service.store = fresh_store
    e.scheduler = new_sched
    e.service.scheduler = new_sched
    e.run()
    t = e.service.get_task("wf", "a")
    assert t.status == TaskStatus.SUCCEEDED
    # 恢复动作必须留痕,判定依据可解释
    assert any(
        x["to"] == "PENDING" and "crash recovery" in x["reason"] for x in t.transitions
    )
    assert e.service.get_workflow("wf").status == WorkflowStatus.SUCCEEDED


def test_cancel_request_before_restart_is_honoured_after_restart(env):
    e = env()
    e.service.submit_workflow(
        "wf",
        [TaskDef(id="a", action="pass"), TaskDef(id="b", action="pass", dependencies=["a"])],
    )
    e.service.cancel_workflow("wf")
    assert e.service.get_workflow("wf").cancel_requested is True

    from orchestrator.scheduler import Scheduler
    from orchestrator.executor import Executor

    fresh_store = type(e.store)(e.path)
    new_sched = Scheduler(fresh_store, Executor(), max_concurrency=4, tick_seconds=0.005)
    new_sched.recover()
    e.store = fresh_store
    e.service.store = fresh_store
    e.scheduler = new_sched
    e.service.scheduler = new_sched
    e.run()
    wf = e.service.get_workflow("wf")
    assert wf.status == WorkflowStatus.CANCELLED
    assert wf.tasks["a"].status == TaskStatus.CANCELLED
    assert wf.tasks["b"].status == TaskStatus.CANCELLED
