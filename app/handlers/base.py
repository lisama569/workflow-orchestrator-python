"""任务处理器接口与执行上下文。

自定义处理器是一个接收 TaskContext、返回结果 dict 的可调用对象。
长任务应周期性检查 ``ctx.cancel_event.is_set()`` 以支持协作式取消。
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Event
from typing import Any, Protocol

TaskHandler = Callable[["TaskContext"], dict[str, Any]]


@dataclass
class TaskContext:
    workflow_id: str
    task_name: str
    attempt: int
    payload: dict
    cancel_event: Event = field(default_factory=Event)
    # 进程崩溃模拟信号（正常执行路径下永不置位）
    abort_event: Event | None = None


class HandlerRegistry(Protocol):
    def get(self, name: str) -> TaskHandler:
        """按名称返回处理器，不存在时抛出 KeyError。"""
        ...
