"""错误类型定义。"""
from __future__ import annotations

from app.models import FailureReason


class WorkflowError(Exception):
    """所有可预期业务错误的基类，携带可区分的 FailureReason。"""

    def __init__(self, reason: FailureReason, message: str, status_code: int = 400):
        super().__init__(message)
        self.reason = reason
        self.status_code = status_code


class SubmissionError(WorkflowError):
    """提交期拒绝（环、未知依赖、重复任务名、幂等冲突）。"""


class NonRetryableTaskError(Exception):
    """任务执行体显式抛出：立即失败，不消耗剩余重试预算。"""

    def __init__(self, message: str, error_type: str = "NonRetryableTaskError"):
        super().__init__(message)
        self.error_type = error_type


class TaskExecutionError(Exception):
    """任务执行体抛出的普通异常：视为可重试失败。"""

    def __init__(self, message: str, error_type: str = "TaskExecutionError"):
        super().__init__(message)
        self.error_type = error_type
