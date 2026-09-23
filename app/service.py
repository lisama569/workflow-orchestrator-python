"""应用服务层：对外操作门面（提交 / 查询 / 取消）。

所有方法都在存储事务（互斥锁）内完成读改写，提交后同步推进一轮调度，
使 HTTP 响应返回时依赖已就绪的任务已进入 READY/RUNNING。
"""
from __future__ import annotations

import json
import uuid

from app.errors import WorkflowError
from app.models import (
    FailureReason,
    RetryPolicy,
    TaskDef,
    TaskEntity,
    TaskState,
    WorkflowDef,
    WorkflowEntity,
    WorkflowState,
    utcnow_iso,
)
from app.scheduler import REASON_SUBMITTED, Scheduler
from app.store import JsonStateStore
from app.validation import validate_workflow


def _definition_fingerprint(definition: WorkflowDef) -> str:
    """定义的规范化指纹：任务按名排序，避免顺序差异导致误判冲突。"""
    tasks = sorted(
        (t.model_dump() for t in definition.tasks), key=lambda d: d["name"]
    )
    return json.dumps(
        {
            "idempotency_key": definition.idempotency_key,
            "max_concurrency": definition.max_concurrency,
            "tasks": tasks,
        },
        sort_keys=True,
    )


def _entity_fingerprint(wf: WorkflowEntity) -> str:
    tasks = sorted(
        (
            TaskDef(
                name=t.name,
                handler=t.handler,
                payload=t.payload,
                deps=list(t.deps),
                retry_policy=t.retry_policy,
            ).model_dump()
            for t in wf.tasks.values()
        ),
        key=lambda d: d["name"],
    )
    return json.dumps(
        {
            "idempotency_key": wf.idempotency_key,
            "max_concurrency": wf.max_concurrency,
            "tasks": tasks,
        },
        sort_keys=True,
    )


class WorkflowService:
    def __init__(self, store: JsonStateStore, scheduler: Scheduler):
        self._store = store
        self._scheduler = scheduler

    # ---------- 提交 ----------

    def submit(self, definition: WorkflowDef) -> WorkflowEntity:
        # 提交前校验：环 / 未知依赖 / 重名，原因可区分
        validate_workflow(definition)
        fingerprint = _definition_fingerprint(definition)

        with self._store.transaction() as data:
            existing = next(
                (
                    wf
                    for wf in data.values()
                    if wf.idempotency_key == definition.idempotency_key
                ),
                None,
            )
            if existing is not None:
                if _entity_fingerprint(existing) != fingerprint:
                    raise WorkflowError(
                        FailureReason.IDEMPOTENCY_CONFLICT,
                        (
                            f"idempotency key '{definition.idempotency_key}' reused "
                            f"with a different definition (existing workflow "
                            f"{existing.id})"
                        ),
                        status_code=409,
                    )
                # 相同幂等键 + 相同定义：原样返回，绝不重复执行
                return existing

            wf_id = f"wf-{uuid.uuid4().hex[:12]}"
            tasks = {
                t.name: TaskEntity(
                    name=t.name,
                    handler=t.handler,
                    payload=t.payload,
                    deps=list(t.deps),
                    retry_policy=RetryPolicy(**t.retry_policy.model_dump()),
                    state=TaskState.PENDING,
                )
                for t in definition.tasks
            }
            wf = WorkflowEntity(
                id=wf_id,
                idempotency_key=definition.idempotency_key,
                tasks=tasks,
                state=WorkflowState.RUNNING,
                max_concurrency=definition.max_concurrency,
            )
            data[wf_id] = wf
            for task in sorted(wf.tasks.values(), key=lambda x: x.name):
                self._scheduler._record_event(
                    wf,
                    to_state=TaskState.PENDING.value,
                    reason=REASON_SUBMITTED,
                    detail=f"task accepted with deps={list(task.deps)}",
                    task_name=task.name,
                )
            wf.updated_at = utcnow_iso()
            return wf
        # 事务提交后同步推进一轮：无依赖任务立即 READY/RUNNING，
        # 结论不依赖后台轮询时序
        self._scheduler.run_once()
        return self.get_workflow(wf_id)

    # ---------- 查询 ----------

    def find_by_idempotency_key(self, key: str) -> WorkflowEntity | None:
        with self._store.transaction() as data:
            return next(
                (wf for wf in data.values() if wf.idempotency_key == key), None
            )

    def get_workflow(self, workflow_id: str) -> WorkflowEntity:
        with self._store.transaction() as data:
            wf = data.get(workflow_id)
            if wf is None:
                raise WorkflowError(
                    FailureReason.NOT_FOUND,
                    f"workflow '{workflow_id}' not found",
                    status_code=404,
                )
            return wf

    def list_workflows(self) -> list[WorkflowEntity]:
        with self._store.transaction() as data:
            return [data[k] for k in sorted(data)]

    # ---------- 取消（不可逆终态语义） ----------

    def cancel_workflow(self, workflow_id: str) -> WorkflowEntity:
        with self._store.transaction() as data:
            wf = data.get(workflow_id)
            if wf is None:
                raise WorkflowError(
                    FailureReason.NOT_FOUND,
                    f"workflow '{workflow_id}' not found",
                    status_code=404,
                )
            if wf.state in {
                WorkflowState.SUCCEEDED,
                WorkflowState.FAILED,
                WorkflowState.CANCELLED,
            }:
                # 终态不可逆：成功/失败/已取消的工作流拒绝再次取消
                raise WorkflowError(
                    FailureReason.ALREADY_TERMINAL,
                    f"workflow '{workflow_id}' is already {wf.state.value}",
                    status_code=409,
                )
            self._scheduler.request_cancel(
                workflow_id, None, FailureReason.WORKFLOW_CANCELLED
            )
            # 立即把未开始任务级联为终态；RUNNING 由 worker 协作收敛
            self._scheduler._evaluate_workflow(wf)
            return wf
        self._scheduler.run_once()

    def cancel_task(self, workflow_id: str, task_name: str) -> WorkflowEntity:
        with self._store.transaction() as data:
            wf = data.get(workflow_id)
            if wf is None:
                raise WorkflowError(
                    FailureReason.NOT_FOUND,
                    f"workflow '{workflow_id}' not found",
                    status_code=404,
                )
            task = wf.tasks.get(task_name)
            if task is None:
                raise WorkflowError(
                    FailureReason.NOT_FOUND,
                    f"task '{task_name}' not found in workflow '{workflow_id}'",
                    status_code=404,
                )
            if task.state in (
                TaskState.SUCCEEDED,
                TaskState.FAILED,
                TaskState.CANCELLED,
                TaskState.SKIPPED,
            ):
                raise WorkflowError(
                    FailureReason.ALREADY_TERMINAL,
                    f"task '{task_name}' is already {task.state.value}",
                    status_code=409,
                )
            self._scheduler.request_cancel(
                workflow_id, task_name, FailureReason.TASK_CANCELLED
            )
            self._scheduler._evaluate_workflow(wf)
            return wf
        self._scheduler.run_once()
