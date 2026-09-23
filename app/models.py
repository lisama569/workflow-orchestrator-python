"""核心领域模型：工作流定义、运行态实体与状态枚举。

状态语义（不可逆的终态以 * 标注）：

任务状态:
    PENDING   已提交，尚有依赖未成功
    READY     全部依赖已成功，等待调度执行
    RUNNING   正在执行（内存中持有 worker，持久化层也可能残留此状态）
    RETRYING  可重试失败，等待退避后再次调度
    SUCCEEDED* 执行成功，重启后不会重跑
    FAILED*   不可重试失败或重试次数耗尽
    SKIPPED*  上游依赖 FAILED/CANCELLED，按确定规则级联
    CANCELLED* 用户取消（RUNNING 任务为协作式取消）

工作流状态:
    RUNNING   至少一个任务未终态
    SUCCEEDED* 全部任务 SUCCEEDED
    FAILED*   任一任务 FAILED（SKIPPED/CANCELLED 不视为工作流失败）
    CANCELLED* 用户取消（含级联 CANCELLED/SKIPPED）
"""
from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum

from pydantic import BaseModel, Field


def utcnow_iso() -> str:
    """统一的 UTC 时间戳格式。"""
    return datetime.now(UTC).isoformat()


class TaskState(str, Enum):
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"


class WorkflowState(str, Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TASK_TERMINAL_STATES = frozenset(
    {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.SKIPPED, TaskState.CANCELLED}
)
WORKFLOW_TERMINAL_STATES = frozenset(
    {WorkflowState.SUCCEEDED, WorkflowState.FAILED, WorkflowState.CANCELLED}
)


class FailureReason(str, Enum):
    """可区分的失败/拒绝原因。"""

    CYCLIC_DEPENDENCY = "CYCLIC_DEPENDENCY"          # 提交时检测到环
    UNKNOWN_DEPENDENCY = "UNKNOWN_DEPENDENCY"        # 依赖指向不存在的任务
    DUPLICATE_TASK_NAME = "DUPLICATE_TASK_NAME"      # 工作流内任务名重复
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"    # 同幂等键但内容不一致
    NON_RETRYABLE = "NON_RETRYABLE"                  # 执行失败且不可重试
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"              # 可重试失败但次数耗尽
    DEPENDENCY_FAILED = "DEPENDENCY_FAILED"          # 上游 FAILED
    DEPENDENCY_CANCELLED = "DEPENDENCY_CANCELLED"    # 上游 CANCELLED
    WORKFLOW_CANCELLED = "WORKFLOW_CANCELLED"        # 用户取消整个工作流
    TASK_CANCELLED = "TASK_CANCELLED"                # 用户取消单个任务
    ALREADY_TERMINAL = "ALREADY_TERMINAL"            # 对终态对象执行取消等操作
    NOT_FOUND = "NOT_FOUND"                          # 工作流/任务不存在


class RetryPolicy(BaseModel):
    """退避重试策略。

    第 n 次失败后的退避 = min(base_delay * backoff_factor ** (n-1), max_delay)。
    max_retries=0 表示不重试；执行体显式抛出不可重试错误时立即终止。
    """

    max_retries: int = 3
    base_delay: float = 1.0
    backoff_factor: float = 2.0
    max_delay: float = 30.0


class TaskDef(BaseModel):
    """任务定义（提交载荷）。"""

    name: str
    handler: str
    payload: dict = Field(default_factory=dict)
    deps: list[str] = Field(default_factory=list)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)


class WorkflowDef(BaseModel):
    """工作流定义（提交载荷）。"""

    idempotency_key: str
    tasks: list[TaskDef]
    max_concurrency: int = 4


class Transition(BaseModel):
    """一次状态转移的审计记录：任务标识、从何到何、判定依据。"""

    seq: int
    task: str | None = None
    workflow_id: str
    from_state: str | None = None
    to_state: str
    reason: str
    detail: str = ""
    at: str = Field(default_factory=utcnow_iso)


class TaskEntity(BaseModel):
    """任务运行态（持久化单位）。"""

    name: str
    handler: str
    payload: dict = Field(default_factory=dict)
    deps: list[str] = Field(default_factory=list)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    state: TaskState = TaskState.PENDING
    attempts: int = 0
    scheduled_at: float | None = None  # epoch 秒，RETRYING 的下次可执行时刻
    failure_reason: FailureReason | None = None
    failure_detail: str = ""
    error_type: str | None = None
    cancel_reason: FailureReason | None = None
    cancel_requested: bool = False
    result: dict | None = None
    updated_at: str = Field(default_factory=utcnow_iso)


class WorkflowEntity(BaseModel):
    """工作流运行态（持久化单位）。"""

    id: str  # 首次提交时生成
    idempotency_key: str
    tasks: dict[str, TaskEntity]
    state: WorkflowState = WorkflowState.RUNNING
    max_concurrency: int = 4
    event_seq: int = 0
    events: list[Transition] = Field(default_factory=list)
    created_at: str = Field(default_factory=utcnow_iso)
    updated_at: str = Field(default_factory=utcnow_iso)
