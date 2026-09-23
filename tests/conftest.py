"""pytest 公共夹具。

每个测试获得独立的临时状态目录；内置 flaky 计数与 gate 闸门在用例间重置。
测试日志会打印任务标识、状态转移与判定依据（"orchestrator" logger，
或直接在断言中打印工作流事件流）。
"""
from __future__ import annotations

import logging
import time

import pytest

from app.handlers.builtin import BuiltinRegistry
from app.scheduler import Scheduler
from app.service import WorkflowService
from app.store import JsonStateStore


@pytest.fixture(autouse=True)
def _orchestrator_logs(caplog):
    """打开状态转移日志，便于在单测输出中看到任务标识与判定依据。

    pytest.ini 配置了 log_cli=INFO，运行 uv run pytest 即可实时看到
    [workflow/task] from -> to | reason 形式的转移日志。
    """
    logger = logging.getLogger("orchestrator")
    logger.setLevel(logging.INFO)
    caplog.set_level(logging.INFO, logger="orchestrator")
    yield


@pytest.fixture
def state_dir(tmp_path):
    return str(tmp_path / "state")


@pytest.fixture
def harness(state_dir):
    """组装一套隔离的 store/scheduler/service，返回控制句柄。"""
    from app.handlers import builtin

    builtin.ATTEMPT_LEDGER.reset()
    for gate in builtin.GATES.values():
        gate.reset()

    store = JsonStateStore(state_dir)
    # 快时钟：让退避在测试中几乎立即到期
    clock = time.monotonic
    scheduler = Scheduler(
        store,
        BuiltinRegistry(),
        tick_interval=0.01,
        max_global_concurrency=8,
        clock=clock,
    )
    service = WorkflowService(store, scheduler)
    scheduler.start()

    class H:
        pass

    h = H()
    h.store = store
    h.scheduler = scheduler
    h.service = service

    def crash_and_rebuild(interrupt_gates: bool = True):
        """模拟进程崩溃：不落盘在途 worker（保留 RUNNING 残留），
        然后用同一状态目录重建一套全新实例并执行恢复。"""
        scheduler.stop(drain_workers=False, timeout=5.0)
        if interrupt_gates:
            # 让卡在 gate 上的工作线程退出（崩溃后线程本就消失）
            for gate in builtin.GATES.values():
                gate.interrupt()
        new_store = JsonStateStore(state_dir)
        new_scheduler = Scheduler(
            new_store,
            BuiltinRegistry(),
            tick_interval=0.01,
            max_global_concurrency=8,
            clock=time.monotonic,
        )
        new_service = WorkflowService(new_store, new_scheduler)
        recovered = new_scheduler.recover()
        new_scheduler.start()
        h.store = new_store
        h.scheduler = new_scheduler
        h.service = new_service
        return h, recovered

    h.crash_and_rebuild = crash_and_rebuild

    yield h

    h.scheduler.stop(drain_workers=True, timeout=5.0)
