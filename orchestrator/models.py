"""状态枚举与核心数据模型。"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum


class TaskStatus(str, Enum):
    """任务状态机:

    PENDING    -> RUNNING / SKIPPED / CANCELLED
    RUNNING    -> SUCCEEDED / PENDING(可重试失败,带退避时间) / FAILED(终态)
                 (进程崩溃时由恢复逻辑从 RUNNING 回退 PENDING)
    FAILED     终态:不可重试失败,或可重试失败达到上限;不会再回到 PENDING
    SUCCEEDED  终态:成功,重启后绝不重跑
    SKIPPED    终态:依赖 FAILED/SKIPPED/CANCELLED,按确定规则不执行
    CANCELLED  终态:被取消或终止

    注意:可重试失败不经过 FAILED(否则下游会把"重试中的失败"误判为终态而提前
    SKIPPED),而是 RUNNING -> PENDING 并设置 next_run_at 退避时间。
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        return self in _TASK_TERMINAL


_TASK_TERMINAL = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.SKIPPED, TaskStatus.CANCELLED}
)


class WorkflowStatus(str, Enum):
    """工作流状态机:

    PENDING  -> RUNNING -> SUCCEEDED / FAILED / CANCELLED
    后三者均为不可逆终态。
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        return self in _WF_TERMINAL


_WF_TERMINAL = frozenset(
    {WorkflowStatus.SUCCEEDED, WorkflowStatus.FAILED, WorkflowStatus.CANCELLED}
)


class GroupStatus(str, Enum):
    """工作流组状态机:

    PENDING  -> RUNNING -> SUCCEEDED / FAILED / CANCELLED
    后三者均为不可逆终态。组只有在【全部成员工作流到达终态】后才收敛:
      - 全部成员 SUCCEEDED            -> SUCCEEDED
      - 组级取消/终止已生效           -> CANCELLED(终态操作,不可逆)
      - 其余情况(有成员 FAILED/CANCELLED) -> FAILED,reason 给出导致结论的成员
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        return self in _GROUP_TERMINAL


_GROUP_TERMINAL = frozenset(
    {GroupStatus.SUCCEEDED, GroupStatus.FAILED, GroupStatus.CANCELLED}
)


@dataclass
class RetryPolicy:
    """指数退避重试策略。"""

    max_retries: int = 3
    base_delay_seconds: float = 0.1
    backoff_multiplier: float = 2.0
    max_delay_seconds: float = 30.0

    def delay_for_attempt(self, attempts_made: int) -> float:
        """已执行 attempts_made 次后,下一次重试前的退避秒数。

        第 1 次失败后用 base_delay,之后每次乘 backoff_multiplier,封顶 max_delay。
        """
        if attempts_made < 1:
            return 0.0
        raw = self.base_delay_seconds * (self.backoff_multiplier ** (attempts_made - 1))
        return min(self.max_delay_seconds, raw)


@dataclass
class TaskDef:
    """提交的任务定义。"""

    id: str
    action: str
    params: dict = field(default_factory=dict)
    dependencies: list[str] = field(default_factory=list)
    retryable: bool = True
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["retry_policy"] = asdict(self.retry_policy)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "TaskDef":
        return cls(
            id=d["id"],
            action=d["action"],
            params=dict(d.get("params") or {}),
            dependencies=list(d.get("dependencies") or []),
            retryable=bool(d.get("retryable", True)),
            retry_policy=RetryPolicy(**(d.get("retry_policy") or {})),
        )


@dataclass
class TaskState:
    """任务运行时状态。"""

    id: str
    action: str
    params: dict = field(default_factory=dict)
    dependencies: list[str] = field(default_factory=list)
    retryable: bool = True
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    status: TaskStatus = TaskStatus.PENDING
    attempt: int = 0
    max_attempts: int = 1
    error: str | None = None
    error_type: str | None = None
    skip_reason: str | None = None
    skipped_by: list[str] = field(default_factory=list)
    next_run_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    result: dict | None = None
    transitions: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status.value
        d["retry_policy"] = asdict(self.retry_policy)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "TaskState":
        return cls(
            id=d["id"],
            action=d["action"],
            params=dict(d.get("params") or {}),
            dependencies=list(d.get("dependencies") or []),
            retryable=bool(d.get("retryable", True)),
            retry_policy=RetryPolicy(**(d.get("retry_policy") or {})),
            status=TaskStatus(d["status"]),
            attempt=int(d.get("attempt", 0)),
            max_attempts=int(d.get("max_attempts", 1)),
            error=d.get("error"),
            error_type=d.get("error_type"),
            skip_reason=d.get("skip_reason"),
            skipped_by=list(d.get("skipped_by") or []),
            next_run_at=d.get("next_run_at"),
            started_at=d.get("started_at"),
            finished_at=d.get("finished_at"),
            result=d.get("result"),
            transitions=list(d.get("transitions") or []),
        )


@dataclass
class GroupWorkflowDef:
    """组提交中单个工作流的定义。"""

    workflow_id: str
    tasks: list[TaskDef] = field(default_factory=list)
    idempotency_key: str | None = None


@dataclass
class WorkflowGroupState:
    """工作流组运行时状态。

    spec_hash: 组提交内容(成员工作流定义)的规范化指纹,
    用于组键防重:同键同内容 -> 幂等返回;同键不同内容 -> 明确拒绝。
    """

    id: str
    group_key: str | None = None
    spec_hash: str = ""
    workflow_ids: list[str] = field(default_factory=list)
    status: GroupStatus = GroupStatus.PENDING
    created_at: float = 0.0
    updated_at: float = 0.0
    reason: str | None = None
    cancel_requested: bool = False
    cancel_reason: str | None = None
    transitions: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "group_key": self.group_key,
            "spec_hash": self.spec_hash,
            "workflow_ids": list(self.workflow_ids),
            "status": self.status.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "reason": self.reason,
            "cancel_requested": self.cancel_requested,
            "cancel_reason": self.cancel_reason,
            "transitions": self.transitions,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "WorkflowGroupState":
        return cls(
            id=d["id"],
            group_key=d.get("group_key"),
            spec_hash=str(d.get("spec_hash") or ""),
            workflow_ids=list(d.get("workflow_ids") or []),
            status=GroupStatus(d["status"]),
            created_at=float(d.get("created_at", 0.0)),
            updated_at=float(d.get("updated_at", 0.0)),
            reason=d.get("reason"),
            cancel_requested=bool(d.get("cancel_requested", False)),
            cancel_reason=d.get("cancel_reason"),
            transitions=list(d.get("transitions") or []),
        )


@dataclass
class WorkflowState:
    """工作流运行时状态。"""

    id: str
    idempotency_key: str | None = None
    tasks: dict[str, TaskState] = field(default_factory=dict)
    status: WorkflowStatus = WorkflowStatus.PENDING
    created_at: float = 0.0
    updated_at: float = 0.0
    reason: str | None = None
    cancel_requested: bool = False
    cancel_reason: str | None = None
    transitions: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "idempotency_key": self.idempotency_key,
            "tasks": {tid: t.to_dict() for tid, t in self.tasks.items()},
            "status": self.status.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "reason": self.reason,
            "cancel_requested": self.cancel_requested,
            "cancel_reason": self.cancel_reason,
            "transitions": self.transitions,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "WorkflowState":
        return cls(
            id=d["id"],
            idempotency_key=d.get("idempotency_key"),
            tasks={tid: TaskState.from_dict(td) for tid, td in (d.get("tasks") or {}).items()},
            status=WorkflowStatus(d["status"]),
            created_at=float(d.get("created_at", 0.0)),
            updated_at=float(d.get("updated_at", 0.0)),
            reason=d.get("reason"),
            cancel_requested=bool(d.get("cancel_requested", False)),
            cancel_reason=d.get("cancel_reason"),
            transitions=list(d.get("transitions") or []),
        )
