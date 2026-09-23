"""进程重启 / 崩溃恢复：成功任务不重跑、未完成状态不丢、RUNNING 残留重放。"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from app.handlers.builtin import ATTEMPT_LEDGER, get_gate
from app.models import (
    FailureReason,
    RetryPolicy,
    TaskDef,
    TaskState,
    WorkflowDef,
    WorkflowState,
)

log = logging.getLogger("orchestrator")
FAST = RetryPolicy(max_retries=3, base_delay=0.01, backoff_factor=1)


def _trace(wf) -> None:
    log.info("[%s] 恢复后状态轨迹:", wf.id)
    for ev in wf.events:
        log.info("  #%s %-4s %-9s -> %-9s | %s | %s",
                 ev.seq, ev.task or "-", ev.from_state or "-",
                 ev.to_state, ev.reason, ev.detail)


def test_succeeded_tasks_not_rerun_after_restart(harness, state_dir):
    """a 已 SUCCEEDED、b 在 RUNNING 时崩溃：
    重启后 a 不重跑（attempts 保持 1），b 从 RUNNING 重放并完成。"""
    ATTEMPT_LEDGER.reset()
    gate = get_gate("rec-1")
    gate.reset()
    definition = WorkflowDef(
        idempotency_key="restart-1",
        tasks=[
            TaskDef(name="a", handler="success"),
            TaskDef(
                name="b",
                handler="gate",
                payload={"gate": "rec-1"},
                deps=["a"],
            ),
        ],
    )
    wf = harness.service.submit(definition)
    time.sleep(0.15)  # a 成功落盘，b 进入 RUNNING 并阻塞
    mid = harness.service.get_workflow(wf.id)
    assert mid.tasks["a"].state is TaskState.SUCCEEDED
    assert mid.tasks["b"].state is TaskState.RUNNING

    # 磁盘上 b 确为 RUNNING 残留
    raw = json.loads((Path(state_dir) / f"{wf.id}.json").read_text())
    assert raw["tasks"]["b"]["state"] == "RUNNING"
    assert raw["tasks"]["a"]["state"] == "SUCCEEDED"

    h, recovered = harness.crash_and_rebuild()
    assert recovered == 1, f"应恢复 1 个 RUNNING 任务，实际 {recovered}"
    # 释放闸门并让新进程完成重放
    get_gate("rec-1").release()
    final = h.scheduler.wait_for_terminal(wf.id, timeout=5)

    assert final.state is WorkflowState.SUCCEEDED
    assert final.tasks["a"].state is TaskState.SUCCEEDED
    assert final.tasks["a"].attempts == 1, "已成功任务重启后绝不能重跑"
    assert final.tasks["b"].state is TaskState.SUCCEEDED
    assert final.tasks["b"].attempts == 2, (
        "崩溃前 attempt 1，恢复重放为 attempt 2"
    )
    _trace(final)


def test_unfinished_state_preserved_through_restart(harness):
    """x 重试耗尽 FAILED、y 等待依赖 PENDING：重启后状态不丢不乱，
    且 y 确定性 SKIPPED/DEPENDENCY_FAILED。"""
    definition = WorkflowDef(
        idempotency_key="restart-2",
        tasks=[
            TaskDef(name="x", handler="fail_always", retry_policy=FAST),
            TaskDef(name="y", handler="success", deps=["x"]),
        ],
    )
    wf = harness.service.submit(definition)
    harness.scheduler.wait_for_terminal(wf.id, timeout=5)
    before = harness.service.get_workflow(wf.id)
    assert before.tasks["x"].state is TaskState.FAILED

    h, recovered = harness.crash_and_rebuild()
    assert recovered == 0, "没有 RUNNING 残留"
    # 给恢复后的 tick 一点时间收敛级联
    time.sleep(0.1)
    after = h.service.get_workflow(wf.id)
    assert after.state is WorkflowState.FAILED
    assert after.tasks["x"].state is TaskState.FAILED
    assert after.tasks["x"].failure_reason is FailureReason.RETRY_EXHAUSTED
    assert after.tasks["x"].attempts == 4
    assert after.tasks["y"].state is TaskState.SKIPPED
    assert after.tasks["y"].failure_reason is FailureReason.DEPENDENCY_FAILED
    _trace(after)


def test_restart_replay_does_not_consume_retry_budget(harness):
    """flaky 在第 1 次执行（RUNNING）时崩溃；重放不消耗预算：
    fail_times=1 的任务重放时算第 2 次执行 -> 成功，共 attempts=2。"""
    ATTEMPT_LEDGER.reset()
    gate = get_gate("rec-3")
    gate.reset()
    definition = WorkflowDef(
        idempotency_key="restart-3",
        tasks=[
            TaskDef(
                name="f",
                handler="gate",
                payload={"gate": "rec-3"},
            ),
        ],
    )
    wf = harness.service.submit(definition)
    time.sleep(0.15)
    h, recovered = harness.crash_and_rebuild()
    assert recovered == 1
    recovered_wf = h.service.get_workflow(wf.id)
    assert recovered_wf.tasks["f"].state is TaskState.READY
    assert recovered_wf.tasks["f"].attempts == 1, "attempts 计数必须保留"
    # 重放成功（gate 被 interrupt 后 wait 立即返回成功）
    final = h.scheduler.wait_for_terminal(wf.id, timeout=5)
    assert final.tasks["f"].state is TaskState.SUCCEEDED
    assert final.tasks["f"].attempts == 2


def test_terminal_state_irreversible_after_restart(harness):
    """CANCELLED 是不可逆终态：重启后不会被重新调度。"""
    gate = get_gate("rec-4")
    gate.reset()
    definition = WorkflowDef(
        idempotency_key="restart-4",
        tasks=[
            TaskDef(name="c", handler="gate", payload={"gate": "rec-4"}),
        ],
    )
    wf = harness.service.submit(definition)
    time.sleep(0.15)
    harness.service.cancel_task(wf.id, "c")
    time.sleep(0.15)
    before = harness.service.get_workflow(wf.id)
    assert before.tasks["c"].state is TaskState.CANCELLED

    h, recovered = harness.crash_and_rebuild()
    assert recovered == 0, "CANCELLED 不是 RUNNING 残留，无需恢复"
    time.sleep(0.1)
    after = h.service.get_workflow(wf.id)
    assert after.tasks["c"].state is TaskState.CANCELLED
    assert after.tasks["c"].attempts == 1
    assert after.state is WorkflowState.CANCELLED


def test_idempotent_resubmit_after_restart_returns_same_workflow(harness):
    """重启后以相同幂等键提交：返回磁盘上的原工作流，不重新执行。"""
    definition = WorkflowDef(
        idempotency_key="restart-5",
        tasks=[TaskDef(name="a", handler="success")],
    )
    wf = harness.service.submit(definition)
    harness.scheduler.wait_for_terminal(wf.id, timeout=5)
    original_id = wf.id

    h, _ = harness.crash_and_rebuild()
    again = h.service.submit(definition)
    assert again.id == original_id
    assert again.tasks["a"].state is TaskState.SUCCEEDED
    assert again.tasks["a"].attempts == 1


def test_pending_task_resumes_after_restart(harness):
    """崩溃时 a 在 RUNNING、b 仍 PENDING：重启后 a 重放成功，b 接着被调度，
    工作流最终 SUCCEEDED（PENDING 状态不丢、不乱）。"""
    gate = get_gate("rec-pending")
    gate.reset()
    definition = WorkflowDef(
        idempotency_key="restart-pending",
        tasks=[
            TaskDef(name="a", handler="gate", payload={"gate": "rec-pending"}),
            TaskDef(name="b", handler="success", deps=["a"]),
        ],
    )
    wf = harness.service.submit(definition)
    time.sleep(0.15)
    before = harness.service.get_workflow(wf.id)
    assert before.tasks["a"].state is TaskState.RUNNING
    assert before.tasks["b"].state is TaskState.PENDING

    h, recovered = harness.crash_and_rebuild()
    assert recovered == 1
    after_recover = h.service.get_workflow(wf.id)
    assert after_recover.tasks["b"].state is TaskState.PENDING, "b 必须仍是 PENDING"
    # abort 信号让 gate 返回后，恢复重放的 a 执行完成，b 自动解锁
    final = h.scheduler.wait_for_terminal(wf.id, timeout=5)
    assert final.state is WorkflowState.SUCCEEDED
    assert final.tasks["a"].state is TaskState.SUCCEEDED
    assert final.tasks["b"].state is TaskState.SUCCEEDED
    assert final.tasks["b"].attempts == 1
    _trace(final)
