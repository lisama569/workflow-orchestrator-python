"""场景二:依赖成功才执行;依赖失败按确定规则使下游 SKIPPED,不静默跳过;
不可重试失败立即终止;取消/终止具备明确不可逆终态语义。"""
from __future__ import annotations

from orchestrator.errors import TerminalStateError
from orchestrator.models import (
    RetryPolicy,
    TaskDef,
    TaskStatus,
    WorkflowStatus,
)


def _fast_retry() -> RetryPolicy:
    return RetryPolicy(max_retries=3, base_delay_seconds=0.005, backoff_multiplier=1.0)


def test_task_runs_only_after_all_dependencies_succeed(env):
    e = env()
    e.service.submit_workflow(
        "wf",
        [
            TaskDef(id="a", action="pass"),
            TaskDef(id="b", action="pass"),
            TaskDef(id="c", action="pass", dependencies=["a", "b"]),
        ],
    )
    e.run()
    wf = e.service.get_workflow("wf")
    assert wf.status == WorkflowStatus.SUCCEEDED
    c = wf.tasks["c"]
    reasons = " ".join(x["reason"] for x in c.transitions)
    assert c.status == TaskStatus.SUCCEEDED
    assert "all dependencies succeeded" in reasons


def test_non_retryable_failure_terminates_immediately(env):
    e = env()
    e.service.submit_workflow(
        "wf",
        [
            TaskDef(
                id="a",
                action="fail",
                params={"kind": "non_retryable", "message": "boom"},
                retryable=False,
                retry_policy=_fast_retry(),
            ),
        ],
    )
    e.run()
    wf = e.service.get_workflow("wf")
    t = wf.tasks["a"]
    assert t.status == TaskStatus.FAILED
    assert t.attempt == 1  # 不可重试:只执行一次,立即终止
    assert t.error_type == "SIMULATED_FAILURE"
    assert "non-retryable" in t.transitions[-1]["reason"]
    assert wf.status == WorkflowStatus.FAILED
    assert "boom" in (wf.reason or "")


def test_failed_dependency_propagates_skip_downstream_deterministically(env):
    e = env()
    e.service.submit_workflow(
        "wf",
        [
            TaskDef(
                id="a",
                action="fail",
                params={"kind": "non_retryable"},
                retryable=False,
            ),
            TaskDef(id="b", action="pass", dependencies=["a"]),
            TaskDef(id="c", action="pass", dependencies=["b"]),
            TaskDef(id="d", action="pass"),  # 无关节点不受影响,正常成功
        ],
    )
    e.run()
    wf = e.service.get_workflow("wf")
    assert wf.tasks["a"].status == TaskStatus.FAILED
    assert wf.tasks["b"].status == TaskStatus.SKIPPED
    assert wf.tasks["b"].skipped_by == ["a"]
    assert wf.tasks["c"].status == TaskStatus.SKIPPED
    assert wf.tasks["c"].skipped_by == ["b"]
    assert wf.tasks["d"].status == TaskStatus.SUCCEEDED
    assert wf.status == WorkflowStatus.FAILED
    assert wf.reason and "failed terminally" in wf.reason and "downstream skipped" in wf.reason
    # 被跳过的任务必须留下明确判定依据,而不是静默丢失
    assert wf.tasks["b"].transitions[-1]["to"] == "SKIPPED"
    assert "dependency never succeeded" in wf.tasks["b"].transitions[-1]["reason"]


def test_skipped_dependency_also_blocks_downstream(env):
    e = env()
    e.service.submit_workflow(
        "wf",
        [
            TaskDef(id="a", action="fail", params={"kind": "non_retryable"}, retryable=False),
            TaskDef(id="b", action="pass", dependencies=["a"]),
            TaskDef(id="c", action="pass", dependencies=["a", "b"]),
        ],
    )
    e.run()
    wf = e.service.get_workflow("wf")
    assert wf.tasks["c"].status == TaskStatus.SKIPPED
    assert set(wf.tasks["c"].skipped_by) == {"a", "b"}


def test_cancel_is_terminal_and_idempotent(env):
    e = env(max_concurrency=2)
    # 先提交两个无依赖的 pass,在它们执行前取消;不 run,直接取消。
    e.service.submit_workflow(
        "wf",
        [TaskDef(id="a", action="pass"), TaskDef(id="b", action="pass", dependencies=["a"])],
    )
    wf = e.service.cancel_workflow("wf")
    assert wf.cancel_requested is True
    assert wf.tasks["a"].status == TaskStatus.CANCELLED
    assert wf.tasks["b"].status == TaskStatus.CANCELLED
    e.run()
    wf = e.service.get_workflow("wf")
    assert wf.status == WorkflowStatus.CANCELLED  # 明确终态
    # 所有任务都没有真正执行过
    assert all(t.attempt == 0 for t in wf.tasks.values())
    # 再次取消:幂等,不报错,终态不变
    wf2 = e.service.cancel_workflow("wf")
    assert wf2.status == WorkflowStatus.CANCELLED
    assert len(wf2.transitions) == len(wf.transitions)


def test_terminate_cancels_running_task_and_is_irreversible(env):
    e = env()
    e.service.submit_workflow(
        "wf",
        [
            TaskDef(id="a", action="sleep", params={"seconds": 2.0}),
            TaskDef(id="b", action="pass", dependencies=["a"]),
        ],
    )
    # 启动后台调度,让 a 进入 RUNNING,随后终止
    e.scheduler.start()
    e.scheduler.notify()
    import time

    deadline = time.time() + 3
    while time.time() < deadline:
        if e.service.get_task("wf", "a").status == TaskStatus.RUNNING:
            break
        time.sleep(0.01)
    assert e.service.get_task("wf", "a").status == TaskStatus.RUNNING
    wf = e.service.terminate_workflow("wf")
    assert wf.status == WorkflowStatus.CANCELLED  # 立即终态
    e.scheduler.stop()

    wf = e.service.get_workflow("wf")
    assert wf.status == WorkflowStatus.CANCELLED
    assert wf.tasks["a"].status == TaskStatus.CANCELLED
    assert wf.tasks["b"].status == TaskStatus.CANCELLED
    # 终态不可逆:再 terminate 幂等返回同一终态
    assert e.service.terminate_workflow("wf").status == WorkflowStatus.CANCELLED
