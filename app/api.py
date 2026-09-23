"""FastAPI 路由层：提交 / 查询 / 取消工作流与任务。"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.config import Settings
from app.errors import WorkflowError
from app.handlers.builtin import BuiltinRegistry
from app.models import FailureReason, WorkflowDef
from app.scheduler import Scheduler
from app.service import WorkflowService
from app.store import JsonStateStore


def create_app(
    service: WorkflowService | None = None,
    *,
    state_dir: str | None = None,
    autostart: bool = True,
) -> FastAPI:
    """构造 FastAPI 应用。

    service 为 None 时按本地配置自建一套（main.py / uvicorn 使用）；
    测试可注入隔离的 service 与临时 state_dir。
    """
    settings = Settings()
    if service is None:
        store = JsonStateStore(state_dir or settings.state_dir)
        scheduler = Scheduler(
            store=store,
            registry=BuiltinRegistry(),
            tick_interval=settings.tick_interval,
        )
        service = WorkflowService(store, scheduler)
        scheduler.recover()
        owns_service = True
    else:
        scheduler = service._scheduler
        owns_service = False

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        if autostart:
            scheduler.start()
        try:
            yield
        finally:
            scheduler.stop(drain_workers=True, timeout=5.0)

    app = FastAPI(
        title="Local Workflow Orchestrator",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.service = service
    app.state.scheduler = scheduler
    app.state.owns_service = owns_service

    @app.exception_handler(WorkflowError)
    async def _handle_workflow_error(_: Request, exc: WorkflowError) -> JSONResponse:
        # 失败原因以稳定的 code 暴露，客户端可区分处理
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.reason.value, "message": str(exc)},
        )

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.post("/workflows", status_code=201)
    def submit_workflow(definition: WorkflowDef) -> dict:
        wf = service.submit(definition)
        return wf.model_dump(mode="json")

    @app.get("/workflows")
    def list_workflows() -> dict:
        items = [w.model_dump(mode="json") for w in service.list_workflows()]
        return {"workflows": items}

    @app.get("/workflows/{workflow_id}")
    def get_workflow(workflow_id: str) -> dict:
        return service.get_workflow(workflow_id).model_dump(mode="json")

    @app.get("/workflows/idempotency/{key}")
    def get_by_idempotency_key(key: str) -> dict:
        wf = service.find_by_idempotency_key(key)
        if wf is None:
            raise WorkflowError(
                FailureReason.NOT_FOUND,
                f"no workflow with idempotency key '{key}'",
                status_code=404,
            )
        return wf.model_dump(mode="json")

    @app.post("/workflows/{workflow_id}/cancel")
    def cancel_workflow(workflow_id: str) -> dict:
        return service.cancel_workflow(workflow_id).model_dump(mode="json")

    @app.post("/workflows/{workflow_id}/tasks/{task_name}/cancel")
    def cancel_task(workflow_id: str, task_name: str) -> dict:
        return service.cancel_task(workflow_id, task_name).model_dump(mode="json")

    return app
