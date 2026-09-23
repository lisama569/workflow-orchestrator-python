"""内置模拟处理器（本地演示与测试用）。

- success:            恒成功
- fail_always:        恒可重试失败
- fail_non_retryable: 恒不可重试失败（立即终止）
- flaky:              前 N 次可重试失败，之后成功
- gate:              默认阻塞，直到测试释放（模拟卡在执行中的任务/崩溃点）

所有状态只保存在进程内，重启即归零，仅用于本地模拟。
"""
from __future__ import annotations

import threading

from app.errors import NonRetryableTaskError, TaskExecutionError
from app.handlers.base import TaskContext, TaskHandler


def success_handler(ctx: TaskContext) -> dict:
    return {"ok": True, "echo": ctx.payload, "attempt": ctx.attempt}


def fail_always_handler(ctx: TaskContext) -> dict:
    raise TaskExecutionError(
        f"simulated retryable failure (attempt={ctx.attempt})",
        error_type="AlwaysFail",
    )


def fail_non_retryable_handler(ctx: TaskContext) -> dict:
    raise NonRetryableTaskError(
        "simulated non-retryable failure",
        error_type="FatalError",
    )


# 名称 -> 已开始执行的次数（含成功前被打断的尝试），供测试断言重放。
class _AttemptLedger:
    def __init__(self) -> None:
        self._counts: dict[str, int] = {}
        self._lock = threading.Lock()

    def record(self, key: str) -> int:
        with self._lock:
            self._counts[key] = self._counts.get(key, 0) + 1
            return self._counts[key]

    def count(self, key: str) -> int:
        with self._lock:
            return self._counts.get(key, 0)

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()


ATTEMPT_LEDGER = _AttemptLedger()


def flaky_handler(ctx: TaskContext) -> dict:
    """前 fail_times 次执行可重试失败，第 fail_times+1 次起成功。"""
    fail_times = int(ctx.payload.get("fail_times", 1))
    key = f"{ctx.workflow_id}/{ctx.task_name}"
    n = ATTEMPT_LEDGER.record(key)
    if n <= fail_times:
        raise TaskExecutionError(
            f"flaky failure {n}/{fail_times}", error_type="FlakyError"
        )
    return {"ok": True, "succeeded_on_attempt": n}


class Gate:
    """测试闸门：handler 阻塞在 wait 上，直到 release / interrupt / abort。"""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._interrupt = threading.Event()

    def wait(self, ctx: TaskContext) -> None:
        while not self._event.is_set():
            if ctx.cancel_event.wait(timeout=0.02):
                return
            if self._interrupt.wait(timeout=0.02):
                return
            if ctx.abort_event is not None and ctx.abort_event.wait(timeout=0.02):
                return

    def release(self) -> None:
        self._event.set()

    def interrupt(self) -> None:
        """模拟进程崩溃前的突然停止：不设置成功信号，让等待直接返回。"""
        self._interrupt.set()

    def reset(self) -> None:
        self._event.clear()
        self._interrupt.clear()


GATES: dict[str, Gate] = {}
_GATES_LOCK = threading.Lock()


def get_gate(name: str) -> Gate:
    with _GATES_LOCK:
        gate = GATES.get(name)
        if gate is None:
            gate = Gate()
            GATES[name] = gate
        return gate


def gate_handler(ctx: TaskContext) -> dict:
    """阻塞直到命名闸门 release（成功）或 interrupt（模拟崩溃）。"""
    gate = get_gate(ctx.payload.get("gate", "default"))
    gate.wait(ctx)
    if ctx.cancel_event.is_set():
        # 协作式取消：由调度侧判定 CANCELLED，这里不再产生成功结果。
        raise TaskExecutionError("cancelled while running", error_type="Cancelled")
    return {"ok": True, "released": True}


class BuiltinRegistry:
    """名称 -> 处理器的注册表。"""

    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {
            "success": success_handler,
            "fail_always": fail_always_handler,
            "fail_non_retryable": fail_non_retryable_handler,
            "flaky": flaky_handler,
            "gate": gate_handler,
        }

    def register(self, name: str, handler: TaskHandler) -> None:
        self._handlers[name] = handler

    def get(self, name: str) -> TaskHandler:
        try:
            return self._handlers[name]
        except KeyError:
            raise KeyError(f"no handler registered for '{name}'") from None
