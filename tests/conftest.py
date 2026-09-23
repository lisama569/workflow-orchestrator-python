"""测试夹具:每个用例独立的临时状态文件、注入式调度器(不启动后台线程)。"""
from __future__ import annotations

import os
import tempfile

import pytest

from orchestrator.executor import Executor
from orchestrator.scheduler import Scheduler
from orchestrator.service import OrchestratorService
from orchestrator.store import JsonStore


@pytest.fixture
def env(tmp_path):
    state_path = str(tmp_path / "state.json")

    class _Env:
        def __init__(self, max_concurrency: int = 4, tick_seconds: float = 0.005):
            self.path = state_path
            self.store = JsonStore(state_path)
            self.scheduler = Scheduler(
                self.store,
                Executor(),
                max_concurrency=max_concurrency,
                tick_seconds=tick_seconds,
            )
            self.service = OrchestratorService(self.store, self.scheduler)

        def run(self, timeout: float = 8.0):
            self.scheduler.run_until_idle(timeout=timeout)

        def new_scheduler_on_same_store(self, max_concurrency: int = 4):
            """模拟进程重启:用同一个状态文件构造全新调度器并执行恢复。"""
            sch = Scheduler(
                self.store,
                Executor(),
                max_concurrency=max_concurrency,
                tick_seconds=0.005,
            )
            sch.recover()
            return sch

    return _Env


@pytest.fixture
def fastapi_client(tmp_path):
    """httpx ASGI 客户端 + 注入式服务(不启用默认单例/后台线程)。"""
    from fastapi.testclient import TestClient

    from orchestrator.api import create_app

    store = JsonStore(str(tmp_path / "api_state.json"))
    sched = Scheduler(store, Executor(), max_concurrency=4, tick_seconds=0.005)
    service = OrchestratorService(store, sched)
    sched.start()
    app = create_app(service)
    client = TestClient(app)
    yield client, service, sched
    client.close()
    sched.stop()
