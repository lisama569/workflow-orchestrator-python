"""任务执行器:本地模拟任务动作,结论确定且可解释。

支持的 action:
  pass    始终成功;params: {"output": {...}}(可选)
  fail    始终失败;params: {"kind": "retryable"|"non_retryable", "message": "..."}
  flaky   前 fail_times 次以可重试错误失败,第 fail_times+1 次起成功
          params: {"fail_times": 2}
  sleep   睡眠 seconds 秒后成功;每秒检查 cancelled 回调,被取消则抛 TaskCancelled
"""
from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum


class FailureKind(str, Enum):
    """失败分类:决定调度器是退避重试还是立即终止。"""

    NONE = "NONE"
    RETRYABLE = "RETRYABLE"
    NON_RETRYABLE = "NON_RETRYABLE"

    @classmethod
    def parse(cls, raw: str | None) -> "FailureKind":
        if not raw:
            return cls.RETRYABLE
        v = raw.strip().lower().replace("-", "_")
        if v in ("non_retryable", "fatal", "permanent"):
            return cls.NON_RETRYABLE
        return cls.RETRYABLE


class TaskCancelled(Exception):
    """执行中的任务收到终止信号。"""


@dataclass
class ExecutionResult:
    """一次任务执行的结论。"""

    success: bool
    kind: FailureKind = FailureKind.NONE
    output: dict = field(default_factory=dict)
    error: str | None = None
    error_type: str | None = None

    @classmethod
    def ok(cls, output: dict | None = None) -> "ExecutionResult":
        return cls(True, FailureKind.NONE, output or {})

    @classmethod
    def fail(cls, kind: FailureKind, message: str, error_type: str) -> "ExecutionResult":
        return cls(False, kind, {}, message, error_type)


IsCancelled = Callable[[], bool]


class Executor:
    """本地模拟执行器。"""

    def execute(
        self,
        action: str,
        params: dict,
        attempt: int,
        task_id: str = "",
        cancelled: IsCancelled | None = None,
    ) -> ExecutionResult:
        params = params or {}
        handler = {
            "pass": self._act_pass,
            "fail": self._act_fail,
            "flaky": self._act_flaky,
            "sleep": self._act_sleep,
        }.get(action)
        if handler is None:
            return ExecutionResult.fail(
                FailureKind.NON_RETRYABLE,
                f"unknown action '{action}'",
                "UNKNOWN_ACTION",
            )
        return handler(params, attempt, task_id, cancelled or (lambda: False))

    # ---------- 内置动作 ----------
    @staticmethod
    def _act_pass(
        params: dict, attempt: int, task_id: str, cancelled: IsCancelled
    ) -> ExecutionResult:
        if cancelled():
            raise TaskCancelled(task_id)
        output = dict(params.get("output") or {})
        output.setdefault("executed_by", "local-executor")
        output["attempt"] = attempt
        return ExecutionResult.ok(output)

    @staticmethod
    def _act_fail(
        params: dict, attempt: int, task_id: str, cancelled: IsCancelled
    ) -> ExecutionResult:
        if cancelled():
            raise TaskCancelled(task_id)
        kind = FailureKind.parse(params.get("kind"))
        message = str(params.get("message") or f"simulated {kind.value} failure")
        return ExecutionResult.fail(kind, message, "SIMULATED_FAILURE")

    @staticmethod
    def _act_flaky(
        params: dict, attempt: int, task_id: str, cancelled: IsCancelled
    ) -> ExecutionResult:
        fail_times = int(params.get("fail_times", 1))
        if attempt <= fail_times:
            if cancelled():
                raise TaskCancelled(task_id)
            return ExecutionResult.fail(
                FailureKind.RETRYABLE,
                f"flaky failure {attempt}/{fail_times}",
                "FLAKY_FAILURE",
            )
        return ExecutionResult.ok({"attempt": attempt, "fail_times": fail_times})

    @staticmethod
    def _act_sleep(
        params: dict, attempt: int, task_id: str, cancelled: IsCancelled
    ) -> ExecutionResult:
        seconds = float(params.get("seconds", 0.05))
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if cancelled():
                raise TaskCancelled(task_id)
            time.sleep(min(0.01, deadline - time.monotonic()))
        return ExecutionResult.ok({"slept": seconds})
