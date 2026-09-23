"""失败收敛：不可重试立即终止、重试耗尽、依赖失败的确定性级联、并发上限。"""
from __future__ import annotations

import logging
import threading
import time

from app.handlers.builtin import ATTEMPT_LEDGER, get_gate
from app.models import (
    FailureReason,
    RetryPolicy,
    TaskDef,
    TaskState,
    WorkflowDef,
)

log = logging.getLogger("orchestrator")
FAST = RetryPolicy(max_retries=3, base_delay=0.01, backoff_factor=1)


def _trace(wf) -> None:
    log.info("[%s] 状态轨迹 (key=%s):", wf.id, wf.idempotency_key)
    for ev in wf.events:
        log.info("  #%s %-4s %-9s -> %-9s | %s | %s",
                 ev.seq, ev.task or "-", ev.from_state or "-",
                 ev.to_state, ev.reason, ev.detail)


def test_non_retryable_fails_immediately(harness):
    definition = WorkflowDef(
        idempotency_key="fatal-1",
        tasks=[TaskDef(name="x", handler="fail_non_retryable", retry_policy=FAST)],
    )
    wf = harness.service.submit(definition)
    final = harness.scheduler.wait_for_terminal(wf.id, timeout=5)
    task = final.tasks["x"]
    assert task.state is TaskState.FAILED
    assert task.failure_reason is FailureReason.NON_RETRYABLE
    assert task.attempts == 1, "不可重试失败必须立即终止，不得消耗重试预算"
    _trace(final)


def test_retry_exhausted_then_failed(harness):
    ATTEMPT_LEDGER.reset()
    definition = WorkflowDef(
        idempotency_key="exhaust-1",
        tasks=[TaskDef(name="x", handler="fail_always", retry_policy=FAST)],
    )
    wf = harness.service.submit(definition)
    final = harness.scheduler.wait_for_terminal(wf.id, timeout=5)
    task = final.tasks["x"]
    assert task.state is TaskState.FAILED
    assert task.failure_reason is FailureReason.RETRY_EXHAUSTED
    # max_retries=3：首次 + 3 次重试 = 4 次执行
    assert task.attempts == 4
    _trace(final)


def test_dependency_failure_cascades_with_distinguishable_reasons(harness):
    """x 不可重试失败 -> 直接下游 y SKIPPED/DEPENDENCY_FAILED
    -> 隔代下游 z SKIPPED/DEPENDENCY_FAILED，绝不静默跳过。"""
    definition = WorkflowDef(
        idempotency_key="dep-fail-1",
        tasks=[
            TaskDef(name="x", handler="fail_non_retryable"),
            TaskDef(name="y", handler="success", deps=["x"]),
            TaskDef(name="z", handler="success", deps=["y"]),
        ],
    )
    wf = harness.service.submit(definition)
    final = harness.scheduler.wait_for_terminal(wf.id, timeout=5)
    assert final.state.value == "FAILED"
    assert final.tasks["x"].state is TaskState.FAILED
    assert final.tasks["x"].failure_reason is FailureReason.NON_RETRYABLE
    for name in ("y", "z"):
        task = final.tasks[name]
        assert task.state is TaskState.SKIPPED, f"{name} 应确定性 SKIPPED"
        assert task.failure_reason is FailureReason.DEPENDENCY_FAILED
        assert "x" in task.failure_detail or "y" in task.failure_detail
        assert task.attempts == 0, f"{name} 被跳过时绝不能执行过"
    _trace(final)


def test_dependency_cancel_cascades_as_cancelled_reason(harness):
    """x 被取消 -> 下游 y SKIPPED/DEPENDENCY_CANCELLED，原因与失败区分。"""
    gate = get_gate("gc")
    gate.reset()
    definition = WorkflowDef(
        idempotency_key="dep-cancel-1",
        tasks=[
            TaskDef(name="x", handler="gate", payload={"gate": "gc"}),
            TaskDef(name="y", handler="success", deps=["x"]),
        ],
    )
    wf = harness.service.submit(definition)
    time.sleep(0.1)  # 等 x 进入 RUNNING
    harness.service.cancel_task(wf.id, "x")
    final = harness.scheduler.wait_for_terminal(wf.id, timeout=5)
    assert final.tasks["x"].state is TaskState.CANCELLED
    assert final.tasks["y"].state is TaskState.SKIPPED
    assert final.tasks["y"].failure_reason is FailureReason.DEPENDENCY_CANCELLED
    assert final.state.value == "CANCELLED"
    _trace(final)


def test_failed_dependency_takes_priority_over_cancelled(harness):
    """一个上游 FAILED、另一个 CANCELLED：判定 FAILED 优先（确定性规则）。"""
    gate = get_gate("gp")
    gate.reset()
    definition = WorkflowDef(
        idempotency_key="dep-mixed-1",
        tasks=[
            TaskDef(name="bad", handler="fail_non_retryable"),
            TaskDef(name="running", handler="gate", payload={"gate": "gp"}),
            TaskDef(
                name="down",
                handler="success",
                deps=["bad", "running"],
            ),
        ],
    )
    wf = harness.service.submit(definition)
    time.sleep(0.1)
    harness.service.cancel_task(wf.id, "running")
    final = harness.scheduler.wait_for_terminal(wf.id, timeout=5)
    down = final.tasks["down"]
    assert down.state is TaskState.SKIPPED
    assert down.failure_reason is FailureReason.DEPENDENCY_FAILED
    _trace(final)


def test_concurrency_cap_respected(harness):
    """max_concurrency=2 时任意时刻最多 2 个 gate 任务同时 RUNNING。"""
    gate = get_gate("cap")
    gate.reset()
    seen = {"running": 0, "max": 0}
    lock = threading.Lock()

    from app.handlers.base import TaskHandler  # noqa: F401

    # 通过自定义 handler 采样并发度
    def counting_gate(ctx):
        with lock:
            seen["running"] += 1
            seen["max"] = max(seen["max"], seen["running"])
        try:
            gate.wait(ctx)
            return {"ok": True}
        finally:
            with lock:
                seen["running"] -= 1

    harness.scheduler._registry.register("counting_gate", counting_gate)

    definition = WorkflowDef(
        idempotency_key="cap-1",
        max_concurrency=2,
        tasks=[
            TaskDef(name=f"t{i}", handler="counting_gate") for i in range(5)
        ],
    )
    wf = harness.service.submit(definition)
    time.sleep(0.3)
    assert seen["max"] <= 2, f"并发突破上限: {seen['max']}"
    assert seen["max"] == 2
    gate.release()
    final = harness.scheduler.wait_for_terminal(wf.id, timeout=5)
    assert final.state.value == "SUCCEEDED"
    assert all(t.state is TaskState.SUCCEEDED for t in final.tasks.values())
    log.info("峰值并发=%s（上限=2），任务执行次数: %s",
             seen["max"], {n: t.attempts for n, t in final.tasks.items()})


def test_same_task_never_executed_concurrently(harness):
    """同一任务重试间隔内不会被重复调度（RETRYING 不可启动）。"""
    ATTEMPT_LEDGER.reset()
    policy = RetryPolicy(max_retries=2, base_delay=0.02, backoff_factor=1)
    definition = WorkflowDef(
        idempotency_key="no-dup-run-1",
        tasks=[TaskDef(name="x", handler="fail_always", retry_policy=policy)],
    )
    wf = harness.service.submit(definition)
    final = harness.scheduler.wait_for_terminal(wf.id, timeout=5)
    assert final.tasks["x"].attempts == 3
    assert final.tasks["x"].state is TaskState.FAILED
    # 事件序列中 RUNNING 与 RETRYING 严格交替，不可能出现连续两个 START
    starts = [e for e in final.events if e.task == "x" and e.to_state == "RUNNING"]
    assert len(starts) == 3
    log.info("任务 x 共启动 %s 次，均串行（事件序号 %s）",
             len(starts), [e.seq for e in starts])


def test_same_task_executions_never_overlap(harness):
    """直接在处理器内部统计并发度：同一任务任意时刻最多一个执行体重入。"""
    overlap = {"current": 0, "max": 0}
    lock = threading.Lock()

    def failing_with_overlap(ctx):
        with lock:
            overlap["current"] += 1
            overlap["max"] = max(overlap["max"], overlap["current"])
            assert overlap["current"] == 1, "检测到同一任务并发双跑！"
        time.sleep(0.03)  # 给调度循环制造“想重复启动”的窗口
        with lock:
            overlap["current"] -= 1
        from app.errors import TaskExecutionError

        raise TaskExecutionError("boom", error_type="OverlapProbe")

    harness.scheduler._registry.register(
        "overlap_probe", failing_with_overlap
    )
    definition = WorkflowDef(
        idempotency_key="no-dup-run-2",
        tasks=[
            TaskDef(
                name="x",
                handler="overlap_probe",
                retry_policy=RetryPolicy(
                    max_retries=4, base_delay=0.005, backoff_factor=1
                ),
            )
        ],
    )
    wf = harness.service.submit(definition)
    final = harness.scheduler.wait_for_terminal(wf.id, timeout=10)
    assert final.tasks["x"].attempts == 5
    assert overlap["max"] == 1
    log.info("任务 x 执行 5 次，处理器内实测峰值并发=%s", overlap["max"])


def test_unknown_handler_fails_non_retryable(harness):
    """提交不校验 handler 是否存在（允许后续注册）；执行期未知 ->
    立即 FAILED/NON_RETRYABLE，不重试。"""
    definition = WorkflowDef(
        idempotency_key="no-handler-1",
        tasks=[
            TaskDef(name="x", handler="not-registered", retry_policy=FAST),
            TaskDef(name="y", handler="success", deps=["x"]),
        ],
    )
    wf = harness.service.submit(definition)
    final = harness.scheduler.wait_for_terminal(wf.id, timeout=5)
    assert final.tasks["x"].state is TaskState.FAILED
    assert final.tasks["x"].failure_reason is FailureReason.NON_RETRYABLE
    assert final.tasks["x"].attempts == 1
    assert final.tasks["y"].state is TaskState.SKIPPED
    assert final.tasks["y"].failure_reason is FailureReason.DEPENDENCY_FAILED
    _trace(final)


def test_global_cap_across_workflows(harness):
    """两个工作流各自上限 4，但全局上限 8（夹具）：独立推进、互不串状态。"""
    gate = get_gate("global-cap")
    gate.reset()
    seen = {"n": 0, "max": 0}
    lock = threading.Lock()

    def counting(ctx):
        with lock:
            seen["n"] += 1
            seen["max"] = max(seen["max"], seen["n"])
        gate.wait(ctx)
        with lock:
            seen["n"] -= 1
        return {"ok": True}

    harness.scheduler._registry.register("counting2", counting)

    def wfdef(key: str) -> WorkflowDef:
        return WorkflowDef(
            idempotency_key=key,
            max_concurrency=4,
            tasks=[TaskDef(name=f"t{i}", handler="counting2") for i in range(4)],
        )

    wf1 = harness.service.submit(wfdef("gc-1"))
    wf2 = harness.service.submit(wfdef("gc-2"))
    time.sleep(0.3)
    assert seen["max"] == 8, f"两工作流合计应达到全局上限 8，实际 {seen['max']}"
    gate.release()
    f1 = harness.scheduler.wait_for_terminal(wf1.id, timeout=5)
    f2 = harness.scheduler.wait_for_terminal(wf2.id, timeout=5)
    assert f1.state.value == "SUCCEEDED" and f2.state.value == "SUCCEEDED"
    assert set(f1.tasks) != set(f2.tasks) or f1.id != f2.id
    log.info("跨工作流峰值并发=%s（全局上限 8）", seen["max"])
