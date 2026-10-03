"""可区分的失败原因。

每类错误都带稳定的 error_code,HTTP 层据此映射不同状态码,
调用方也可以按错误类型区分:环 / 缺依赖 / 幂等冲突 / 终态非法操作。
"""
from __future__ import annotations


class OrchestratorError(Exception):
    """所有调度器错误的基类,携带机器可读的错误码。"""

    error_code = "ORCHESTRATOR_ERROR"


class WorkflowValidationError(OrchestratorError):
    """工作流定义校验失败的基类(提交即被拒绝,不会产生任何执行)。"""

    error_code = "WORKFLOW_VALIDATION_ERROR"


class CycleDetectedError(WorkflowValidationError):
    """依赖图存在环;cycle 字段给出环上的任务 id。"""

    error_code = "CYCLE_DETECTED"

    def __init__(self, cycle: list[str]):
        self._cycle: list[str] = list(cycle)
        super().__init__(f"dependency cycle detected: {' -> '.join(self._cycle)}")

    @property
    def cycle(self) -> list[str]:
        return list(self._cycle)


class MissingDependencyError(WorkflowValidationError):
    """依赖指向不存在的任务。"""

    error_code = "MISSING_DEPENDENCY"

    def __init__(self, task_id: str, missing: str):
        self._task_id = task_id
        self._missing = missing
        super().__init__(f"task '{task_id}' depends on missing task '{missing}'")

    @property
    def task_id(self) -> str:
        return self._task_id

    @property
    def missing_dependency(self) -> str:
        return self._missing


class DuplicateTaskIdError(WorkflowValidationError):
    """同一次提交中任务 id 重复。"""

    error_code = "DUPLICATE_TASK_ID"

    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"duplicate task id in submission: '{task_id}'")


class EmptyWorkflowError(WorkflowValidationError):
    """提交了不含任何任务的工作流。"""

    error_code = "EMPTY_WORKFLOW"

    def __init__(self):
        super().__init__("workflow must contain at least one task")


class DuplicateIdempotencyKeyError(OrchestratorError):
    """幂等键冲突:不产生重复执行,可通过 existing_workflow_id 取回原工作流。"""

    error_code = "IDEMPOTENCY_KEY_CONFLICT"

    def __init__(self, existing_workflow_id: str):
        self.existing_workflow_id = existing_workflow_id
        super().__init__(
            f"idempotency key already used by workflow '{existing_workflow_id}'"
        )


class WorkflowNotFoundError(OrchestratorError):
    """工作流不存在。"""

    error_code = "WORKFLOW_NOT_FOUND"

    def __init__(self, workflow_id: str):
        self.workflow_id = workflow_id
        super().__init__(f"workflow not found: '{workflow_id}'")


class TaskNotFoundError(OrchestratorError):
    """任务不存在。"""

    error_code = "TASK_NOT_FOUND"

    def __init__(self, workflow_id: str, task_id: str):
        self.workflow_id = workflow_id
        self.task_id = task_id
        super().__init__(f"task not found: '{task_id}' in workflow '{workflow_id}'")


class TerminalStateError(OrchestratorError):
    """对已处于不可逆终态的工作流/任务执行了非法操作。"""

    error_code = "TERMINAL_STATE"

    def __init__(self, target: str, status: str):
        self.target = target
        self.status = status
        super().__init__(f"'{target}' is already in terminal state {status}; operation refused")


# ================= 工作流组 =================
class EmptyGroupError(WorkflowValidationError):
    """提交了不含任何工作流的组。"""

    error_code = "EMPTY_GROUP"

    def __init__(self):
        super().__init__("workflow group must contain at least one workflow")


class DuplicateWorkflowIdInGroupError(WorkflowValidationError):
    """同一组内工作流 id 重复。"""

    error_code = "DUPLICATE_WORKFLOW_ID"

    def __init__(self, workflow_id: str):
        self.workflow_id = workflow_id
        super().__init__(f"duplicate workflow id in group: '{workflow_id}'")


class GroupKeyConflictError(OrchestratorError):
    """组键(幂等键)被复用但提交内容不一致。

    与其它校验失败可区分:HTTP 409 + GROUP_KEY_CONFLICT,
    并携带 existing_group_id 指向最终落地的那个已有组。
    """

    error_code = "GROUP_KEY_CONFLICT"

    def __init__(self, existing_group_id: str):
        self.existing_group_id = existing_group_id
        super().__init__(
            f"group idempotency key reused with different content; "
            f"existing group: '{existing_group_id}'"
        )


class GroupNotFoundError(OrchestratorError):
    """工作流组不存在。"""

    error_code = "GROUP_NOT_FOUND"

    def __init__(self, group_id: str):
        self.group_id = group_id
        super().__init__(f"workflow group not found: '{group_id}'")
