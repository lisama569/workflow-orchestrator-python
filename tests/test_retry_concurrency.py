"""场景三:可重试失败按指数退避重试到上限;并发上限不被突破;同一任务不重复执行。"""
from __future__ import annotations

import threading
import time

from orchestrator.models import RetryPolicy, TaskDef, TaskStatus, WorkflowStatus


def test_retryable_failure_retries_with_backoff_then_succeeds(env):
    e = env()
    e.service.submit_workflow(
        "wf",
        [
            TaskDef(
                id="a",
                action="flaky",
                params={"fail_times": 2},
                retry_policy=RetryPolicy(
                    max_retries=3,
                    base_delay_seconds=0.02,
                    backoff_multiplier=2.0,
                    max_delay_seconds=5.0,
                ),
            )
        ],
    )
    t0 = time.time()
    e.run(timeout=6)
    elapsed = time.time() - t0
    wf = e.service.get_workflow("wf")
    t = wf.tasks["a"]
    assert t.status == TaskStatus.SUCCEEDED
    assert t.attempt == 3  # 失败 2 次 + 成功 1 次
    # 退避:第1次失败 0.02s,第2次失败 0.04s -> 至少 0.06s
    assert elapsed >= 0.055
    states = [(x["from"], x["to"]) for x in t.transitions]
    assert states.count(("RUNNING", "PENDING")) == 2  # 可重试失败回退 PENDING 等待退避
    assert states[-1] == ("RUNNING", "SUCCEEDED")
    reasons = " ".join(x["reason"] for x in t.transitions)
    assert "backoff" in reasons and "retry 3/4" in reasons
    assert wf.status == WorkflowStatus.SUCCEEDED


def test_retry_budget_exhausted_converges_to_failed(env):
    e = env()
    e.service.submit_workflow(
        "wf",
        [
            TaskDef(
                id="a",
                action="fail",
                params={"kind": "retryable", "message": "always down"},
                retry_policy=RetryPolicy(
                    max_retries=2, base_delay_seconds=0.005, backoff_multiplier=1.0
                ),
            )
        ],
    )
    e.run(timeout=6)
    t = e.service.get_task("wf", "a")
    assert t.status == TaskStatus.FAILED
    assert t.attempt == 3  # 1 + 2 次重试,到上限收敛
    assert "retry budget exhausted" in t.transitions[-1]["reason"]
    assert e.service.get_workflow("wf").status == WorkflowStatus.FAILED


def test_non_retryable_task_does_not_retry_even_when_policy_allows(env):
    e = env()
    e.service.submit_workflow(
        "wf",
        [
            TaskDef(
                id="a",
                action="fail",
                params={"kind": "non_retryable"},
                retryable=True,  # 执行结论仍是 non_retryable
                retry_policy=RetryPolicy(max_retries=5, base_delay_seconds=0.005),
            )
        ],
    )
    e.run()
    t = e.service.get_task("wf", "a")
    assert t.status == TaskStatus.FAILED and t.attempt == 1


class _ConcurrencyProbe:
    """记录动作并发进入数的探针执行器包装。"""

    def __init__(self):
        self.active = 0
        self.peak = 0
        self.executions = {}
        self.guard = threading.Lock()
        from orchestrator.executor import Executor

        self._inner = Executor()

    def execute(self, action, params, attempt, task_id="", cancelled=None):
        with self.guard:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.executions[task_id] = self.executions.get(task_id, 0) + 1
        time.sleep(0.05)
        try:
            return self._inner.execute(action, params, attempt, task_id, cancelled)
        finally:
            with self.guard:
                self.active -= 1


def test_concurrency_cap_never_exceeded_and_no_duplicate_execution(env):
    e = env(max_concurrency=2)
    probe = _ConcurrencyProbe()
    e.scheduler.executor = probe
    tasks = [TaskDef(id=f"t{i}", action="pass", params={"i": i}) for i in range(6)]
    e.service.submit_workflow("wf", tasks)
    e.run(timeout=8)
    wf = e.service.get_workflow("wf")
    assert wf.status == WorkflowStatus.SUCCEEDED
    assert probe.peak <= 2, f"并发突破上限: peak={probe.peak}"
    assert probe.peak == 2
    # 每个任务恰好执行一次
    assert all(c == 1 for c in probe.executions.values())
    assert set(probe.executions) == {f"t{i}" for i in range(6)}
