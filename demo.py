"""本地端到端演示（无需 HTTP）：uv run python demo.py

依次展示：
1. 依赖链 + 可重试失败退避后成功
2. 不可重试失败立即终止、下游确定性 SKIPPED
3. 幂等重复提交不重复执行
4. 进程“崩溃”后重启恢复（RUNNING 残留重放、成功任务不重跑）
5. 取消的终态语义
"""
from __future__ import annotations

import logging
import shutil
import tempfile
import time

from app.handlers.builtin import ATTEMPT_LEDGER, BuiltinRegistry, get_gate
from app.models import RetryPolicy, TaskDef, WorkflowDef
from app.scheduler import Scheduler
from app.service import WorkflowService
from app.store import JsonStateStore

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
FAST = RetryPolicy(max_retries=3, base_delay=0.05, backoff_factor=2, max_delay=1)


def banner(title: str) -> None:
    print("\n" + "=" * 70 + f"\n{title}\n" + "=" * 70)


def build(state_dir: str):
    store = JsonStateStore(state_dir)
    scheduler = Scheduler(store, BuiltinRegistry(), tick_interval=0.02)
    service = WorkflowService(store, scheduler)
    scheduler.recover()
    scheduler.start()
    return store, scheduler, service


def show(wf) -> None:
    print(f"workflow={wf.id} state={wf.state.value}")
    for name in sorted(wf.tasks):
        t = wf.tasks[name]
        extra = (
            f" reason={t.failure_reason.value}" if t.failure_reason else ""
        )
        print(f"  - {name:<8} {t.state.value:<9} attempts={t.attempts}{extra}")


def main() -> None:
    state_dir = tempfile.mkdtemp(prefix="wf-demo-")
    print("state_dir =", state_dir)
    _store, scheduler, service = build(state_dir)

    try:
        banner("1. 依赖链 + flaky 重试（b 前两次失败，第三次成功）")
        ATTEMPT_LEDGER.reset()
        wf1 = service.submit(WorkflowDef(
            idempotency_key="demo-chain",
            tasks=[
                TaskDef(name="a", handler="success"),
                TaskDef(name="b", handler="flaky", payload={"fail_times": 2},
                        retry_policy=FAST),
                TaskDef(name="c", handler="success", deps=["a", "b"]),
            ],
        ))
        show(scheduler.wait_for_terminal(wf1.id, timeout=10))

        banner("2. 不可重试失败 -> 立即终止，下游 y/z 确定性 SKIPPED")
        wf2 = service.submit(WorkflowDef(
            idempotency_key="demo-fatal",
            tasks=[
                TaskDef(name="x", handler="fail_non_retryable"),
                TaskDef(name="y", handler="success", deps=["x"]),
                TaskDef(name="z", handler="success", deps=["y"]),
            ],
        ))
        show(scheduler.wait_for_terminal(wf2.id, timeout=10))

        banner("3. 幂等：相同键重复提交，返回同一工作流且 attempts 不增加")
        same = service.submit(WorkflowDef(
            idempotency_key="demo-chain",
            tasks=[
                TaskDef(name="a", handler="success"),
                TaskDef(name="b", handler="flaky", payload={"fail_times": 2},
                        retry_policy=FAST),
                TaskDef(name="c", handler="success", deps=["a", "b"]),
            ],
        ))
        print("same id:", same.id == wf1.id, "| c attempts:", same.tasks["c"].attempts)

        banner("4. 崩溃恢复：g 处于 RUNNING 时杀死进程，重启后重放")
        gate = get_gate("demo-gate")
        gate.reset()
        wf4 = service.submit(WorkflowDef(
            idempotency_key="demo-restart",
            tasks=[
                TaskDef(name="done", handler="success"),
                TaskDef(name="g", handler="gate", payload={"gate": "demo-gate"},
                        deps=["done"]),
            ],
        ))
        time.sleep(0.2)
        scheduler.stop(drain_workers=False)
        gate.interrupt()
        _store2, scheduler, service = build(state_dir)
        time.sleep(0.2)
        gate.release()
        show(scheduler.wait_for_terminal(wf4.id, timeout=10))
        print("done.attempts 必须为 1（成功不重跑）:",
              service.get_workflow(wf4.id).tasks["done"].attempts)

        banner("5. 取消：取消运行中的 gate，工作流进入 CANCELLED 终态")
        gate2 = get_gate("demo-cancel")
        gate2.reset()
        wf5 = service.submit(WorkflowDef(
            idempotency_key="demo-cancel",
            tasks=[TaskDef(name="h", handler="gate",
                           payload={"gate": "demo-cancel"})],
        ))
        time.sleep(0.2)
        service.cancel_workflow(wf5.id)
        show(scheduler.wait_for_terminal(wf5.id, timeout=10))
    finally:
        scheduler.stop(timeout=5)
        shutil.rmtree(state_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
