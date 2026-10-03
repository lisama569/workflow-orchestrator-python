"""FastAPI HTTP 接口。

端点:
  POST   /workflows                 提交工作流(支持 Idempotency-Key 头)
  GET    /workflows                 列出工作流
  GET    /workflows/{wf_id}         查询工作流(含全部任务状态/转移记录)
  GET    /workflows/{wf_id}/tasks/{task_id}  查询单个任务
  POST   /workflows/{wf_id}/cancel  优雅取消(不可逆)
  POST   /workflows/{wf_id}/terminate 立即终止(不可逆)
  POST   /groups                    一次提交一组工作流(支持 Group-Key 头防重)
  GET    /groups                    列出工作流组
  GET    /groups/{group_id}         查询组(含组内各工作流当前状态)
  POST   /groups/{group_id}/cancel  组级优雅取消(不可逆,幂等)
  POST   /groups/{group_id}/terminate 组级立即终止(不可逆,幂等)
  GET    /health                    健康检查
"""
from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import (
    CycleDetectedError,
    DuplicateIdempotencyKeyError,
    DuplicateTaskIdError,
    DuplicateWorkflowIdError,
    EmptyGroupError,
    EmptyWorkflowError,
    GroupKeyConflictError,
    GroupNotFoundError,
    MissingDependencyError,
    OrchestratorError,
    TaskNotFoundError,
    TerminalStateError,
    WorkflowIdConflictError,
    WorkflowNotFoundError,
)
from .executor import Executor
from .models import GroupWorkflowDef, RetryPolicy, TaskDef
from .scheduler import Scheduler
from .service import OrchestratorService
from .store import JsonStore


# ---------------- 请求/响应模型 ----------------
class RetryPolicyIn(BaseModel):
    max_retries: int = Field(default=3, ge=0, le=100)
    base_delay_seconds: float = Field(default=0.1, ge=0)
    backoff_multiplier: float = Field(default=2.0, ge=1.0)
    max_delay_seconds: float = Field(default=30.0, ge=0)


class TaskIn(BaseModel):
    id: str = Field(min_length=1)
    action: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    dependencies: list[str] = Field(default_factory=list)
    retryable: bool = True
    retry_policy: RetryPolicyIn = Field(default_factory=RetryPolicyIn)


class WorkflowIn(BaseModel):
    workflow_id: str = Field(min_length=1)
    idempotency_key: str | None = None
    tasks: list[TaskIn] = Field(min_length=1)


class GroupWorkflowIn(BaseModel):
    workflow_id: str = Field(min_length=1)
    idempotency_key: str | None = None
    tasks: list[TaskIn] = Field(min_length=1)


class GroupIn(BaseModel):
    group_id: str = Field(min_length=1)
    group_key: str | None = None
    workflows: list[GroupWorkflowIn] = Field(min_length=1)


def _task_to_def(t: TaskIn) -> TaskDef:
    return TaskDef(
        id=t.id,
        action=t.action,
        params=t.params,
        dependencies=list(t.dependencies),
        retryable=t.retryable,
        retry_policy=RetryPolicy(**t.retry_policy.model_dump()),
    )


def _wf_to_json(wf) -> dict:
    return {
        "id": wf.id,
        "status": wf.status.value,
        "idempotency_key": wf.idempotency_key,
        "reason": wf.reason,
        "cancel_requested": wf.cancel_requested,
        "cancel_reason": wf.cancel_reason,
        "created_at": wf.created_at,
        "updated_at": wf.updated_at,
        "tasks": {
            tid: {
                "id": t.id,
                "status": t.status.value,
                "action": t.action,
                "dependencies": t.dependencies,
                "attempt": t.attempt,
                "max_attempts": t.max_attempts,
                "retryable": t.retryable,
                "error": t.error,
                "error_type": t.error_type,
                "skip_reason": t.skip_reason,
                "skipped_by": t.skipped_by,
                "next_run_at": t.next_run_at,
                "started_at": t.started_at,
                "finished_at": t.finished_at,
                "result": t.result,
                "transitions": t.transitions,
            }
            for tid, t in wf.tasks.items()
        },
        "transitions": wf.transitions,
    }


def _group_to_json(group, members: dict | None = None) -> dict:
    payload = {
        "id": group.id,
        "status": group.status.value,
        "group_key": group.group_key,
        "workflow_ids": list(group.workflow_ids),
        "reason": group.reason,
        "cancel_requested": group.cancel_requested,
        "cancel_reason": group.cancel_reason,
        "created_at": group.created_at,
        "updated_at": group.updated_at,
        "transitions": group.transitions,
    }
    if members is not None:
        payload["members"] = {
            wid: {
                "id": w.id,
                "status": w.status.value,
                "reason": w.reason,
                "tasks": {tid: t.status.value for tid, t in w.tasks.items()},
            }
            for wid, w in members.items()
        }
    return payload


# ---------------- 默认单例(本地运行) ----------------
_default: OrchestratorService | None = None


def get_default_service() -> OrchestratorService:
    global _default
    if _default is None:
        path = os.environ.get("WF_STATE_PATH", "workflow_state.json")
        max_conc = int(os.environ.get("WF_MAX_CONCURRENCY", "4"))
        store = JsonStore(path)
        sched = Scheduler(store, Executor(), max_concurrency=max_conc)
        sched.start()
        _default = OrchestratorService(store, sched)
    return _default


def _error_response(exc: OrchestratorError) -> JSONResponse:
    status_codes = {
        "CYCLE_DETECTED": (422, lambda e: {"cycle": e.cycle}),
        "MISSING_DEPENDENCY": (
            422,
            lambda e: {"task_id": e.task_id, "missing_dependency": e.missing_dependency},
        ),
        "DUPLICATE_TASK_ID": (422, lambda e: {"task_id": e.task_id}),
        "EMPTY_WORKFLOW": (422, lambda e: {}),
        "IDEMPOTENCY_KEY_CONFLICT": (409, lambda e: {"existing_workflow_id": e.existing_workflow_id}),
        "WORKFLOW_NOT_FOUND": (404, lambda e: {}),
        "TASK_NOT_FOUND": (404, lambda e: {}),
        "TERMINAL_STATE": (409, lambda e: {"target": e.target, "status": e.status}),
        "EMPTY_GROUP": (422, lambda e: {}),
        "DUPLICATE_WORKFLOW_ID": (422, lambda e: {"workflow_id": e.workflow_id}),
        "WORKFLOW_ID_CONFLICT": (409, lambda e: {"workflow_id": e.workflow_id}),
        "GROUP_KEY_CONFLICT": (409, lambda e: {"existing_group_id": e.existing_group_id}),
        "GROUP_NOT_FOUND": (404, lambda e: {}),
    }
    code = exc.error_code
    http_status, extra_fn = status_codes.get(code, (400, lambda e: {}))
    return JSONResponse(
        status_code=http_status,
        content={"error_code": code, "message": str(exc), **extra_fn(exc)},
    )


def create_app(service: OrchestratorService | None = None) -> FastAPI:
    app = FastAPI(title="Local Workflow Orchestrator", version="0.1.0")
    app.state.service = service

    def svc() -> OrchestratorService:
        return app.state.service or get_default_service()

    @app.exception_handler(OrchestratorError)
    async def _on_orch_error(request: Request, exc: OrchestratorError) -> JSONResponse:
        return _error_response(exc)

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "running_tasks": svc().scheduler.running_count}

    @app.post("/workflows", status_code=201)
    async def submit(body: WorkflowIn, idempotency_key: str | None = Header(default=None)) -> dict:
        key = body.idempotency_key or idempotency_key
        wf = svc().submit_workflow(body.workflow_id, [_task_to_def(t) for t in body.tasks], key)
        return _wf_to_json(wf)

    @app.get("/workflows")
    async def list_workflows() -> dict:
        workflows = svc().store.load_all()
        return {"workflows": [{"id": w.id, "status": w.status.value} for w in workflows.values()]}

    @app.get("/workflows/{wf_id}")
    async def get_workflow(wf_id: str) -> dict:
        return _wf_to_json(svc().get_workflow(wf_id))

    @app.get("/workflows/{wf_id}/tasks/{task_id}")
    async def get_task(wf_id: str, task_id: str) -> dict:
        t = svc().get_task(wf_id, task_id)
        return {
            "id": t.id,
            "status": t.status.value,
            "attempt": t.attempt,
            "max_attempts": t.max_attempts,
            "error": t.error,
            "error_type": t.error_type,
            "skip_reason": t.skip_reason,
            "skipped_by": t.skipped_by,
            "result": t.result,
            "transitions": t.transitions,
        }

    @app.post("/workflows/{wf_id}/cancel")
    async def cancel(wf_id: str) -> dict:
        return _wf_to_json(svc().cancel_workflow(wf_id))

    @app.post("/workflows/{wf_id}/terminate")
    async def terminate(wf_id: str) -> dict:
        return _wf_to_json(svc().terminate_workflow(wf_id))

    # ---------------- 工作流组 ----------------
    @app.post("/groups", status_code=201)
    async def submit_group(body: GroupIn, group_key: str | None = Header(default=None)) -> dict:
        key = body.group_key or group_key
        defs = [
            GroupWorkflowDef(
                workflow_id=w.workflow_id,
                idempotency_key=w.idempotency_key,
                tasks=[_task_to_def(t) for t in w.tasks],
            )
            for w in body.workflows
        ]
        group = svc().submit_group(body.group_id, defs, group_key=key)
        return _group_to_json(group, svc().get_group_members(group.id))

    @app.get("/groups")
    async def list_groups() -> dict:
        groups = svc().store.load_groups()
        return {"groups": [{"id": g.id, "status": g.status.value} for g in groups.values()]}

    @app.get("/groups/{group_id}")
    async def get_group(group_id: str) -> dict:
        group = svc().get_group(group_id)
        return _group_to_json(group, svc().get_group_members(group_id))

    @app.post("/groups/{group_id}/cancel")
    async def cancel_group(group_id: str) -> dict:
        group = svc().cancel_group(group_id)
        return _group_to_json(group, svc().get_group_members(group_id))

    @app.post("/groups/{group_id}/terminate")
    async def terminate_group(group_id: str) -> dict:
        group = svc().terminate_group(group_id)
        return _group_to_json(group, svc().get_group_members(group_id))

    return app
