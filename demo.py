"""一键本地演示:正常链路 + 拒绝原因 + 幂等 + 进程重启恢复。

用法:
  uv run python demo.py                 # 自动管理临时状态文件
  WF_STATE_PATH=/tmp/demo.json uv run python demo.py
"""
from __future__ import annotations

import json
import os
import tempfile
import time

from orchestrator.executor import Executor
from orchestrator.models import RetryPolicy, TaskDef
from orchestrator.scheduler import Scheduler
from orchestrator.service import OrchestratorService
from orchestrator.store import JsonStore
from orchestrator.logging_setup import setup_logging

setup_logging()

STATE = os.environ.get("WF_STATE_PATH", os.path.join(tempfile.mkdtemp(), "demo_state.json"))


def show(title, wf):
    print(f"\n=== {title}: workflow={wf.id} status={wf.status.value} ===")
    if wf.reason:
        print("reason:", wf.reason)
    for t in wf.tasks.values():
        extra = f" attempt={t.attempt}/{t.max_attempts}"
        if t.skipped_by:
            extra += f" skipped_by={t.skipped_by}"
        if t.error:
            extra += f" error=[{t.error_type}] {t.error}"
        print(f"  task={t.id:<8} status={t.status.value:<9}{extra}")


def main() -> None:
    # ---- 进程 1:提交并执行 ----
    store = JsonStore(STATE)
    sch = Scheduler(store, Executor(), max_concurrency=2, tick_seconds=0.01)
    sch.start()
    svc = OrchestratorService(store, sch)

    wf = svc.submit_workflow(
        "demo",
        [
            TaskDef(id="fetch", action="pass", params={"output": {"rows": 42}}),
            TaskDef(
                id="wobbly",
                action="flaky",
                params={"fail_times": 2},
                retry_policy=RetryPolicy(max_retries=3, base_delay_seconds=0.05, backoff_multiplier=2),
            ),
            TaskDef(id="report", action="pass", dependencies=["fetch", "wobbly"]),
        ],
        idempotency_key="demo-key-1",
    )
    # 重复提交:必须返回同一工作流、不重复执行
    again = svc.submit_workflow(
        "demo",
        [TaskDef(id="fetch", action="pass")],
        idempotency_key="demo-key-1",
    )
    assert again is wf
    print("duplicate submission with same idempotency key -> same workflow object, no re-run")

    deadline = time.time() + 10
    while time.time() < deadline and svc.get_workflow("demo").status.value == "RUNNING" or \
            svc.get_workflow("demo").status.value == "PENDING":
        if svc.get_workflow("demo").status.is_terminal:
            break
        time.sleep(0.05)
    show("after execution (process 1)", svc.get_workflow("demo"))

    # ---- 校验拒绝:环 / 缺依赖 ----
    from orchestrator.errors import CycleDetectedError, MissingDependencyError

    try:
        OrchestratorService.validate(
            [TaskDef(id="a", action="pass", dependencies=["b"]),
             TaskDef(id="b", action="pass", dependencies=["a"])]
        )
    except CycleDetectedError as exc:
        print(f"\nrejected cycle: code={exc.error_code} cycle={exc.cycle}")
    try:
        OrchestratorService.validate([TaskDef(id="a", action="pass", dependencies=["ghost"])])
    except MissingDependencyError as exc:
        print(f"rejected missing dep: code={exc.error_code} "
              f"task={exc.task_id} missing={exc.missing_dependency}")

    # ---- 工作流组:整组提交 + 组键防重 + 成员失败时组级收敛 ----
    from orchestrator.errors import GroupKeyConflictError
    from orchestrator.models import WorkflowDef

    group_members = [
        WorkflowDef(id="demo-ok", tasks=[TaskDef(id="t1", action="pass")]),
        WorkflowDef(
            id="demo-bad",
            tasks=[
                TaskDef(id="boom", action="fail", params={"kind": "non_retryable"}),
                TaskDef(id="after", action="pass", dependencies=["boom"]),
            ],
        ),
    ]
    g = svc.submit_group("demo-group", group_members, idempotency_key="demo-group-key")
    # 同一组键并发/重复提交:返回同一个组,不重复创建
    g_again = svc.submit_group("demo-group", group_members, idempotency_key="demo-group-key")
    assert g_again is g
    print("\nduplicate group submission with same key -> same group object, no re-run")
    # 组键复用但内容不一致:可区分的拒绝,并指向已有组
    try:
        svc.submit_group(
            "other-group",
            [WorkflowDef(id="wf-x", tasks=[TaskDef(id="x", action="pass")])],
            idempotency_key="demo-group-key",
        )
    except GroupKeyConflictError as exc:
        print(f"rejected group key conflict: code={exc.error_code} existing={exc.existing_group_id}")

    deadline = time.time() + 10
    while time.time() < deadline and not svc.get_group("demo-group").status.is_terminal:
        time.sleep(0.05)
    g_done, members = svc.get_group_view("demo-group")
    print(f"\n=== group={g_done.id} status={g_done.status.value} ===")
    print("reason:", g_done.reason)
    for m in members:
        print(f"  member={m.id:<9} status={m.status.value:<9} reason={m.reason}")

    sch.stop()

    # ---- 进程 2:用同一状态文件"重启",恢复后结论一致 ----
    store2 = JsonStore(STATE)
    sch2 = Scheduler(store2, Executor(), max_concurrency=2, tick_seconds=0.01)
    sch2.start()
    svc2 = OrchestratorService(store2, sch2)
    wf2 = svc2.get_workflow("demo")
    show("after restart (process 2, recover())", wf2)
    assert wf2.status.value == "SUCCEEDED"
    assert wf2.tasks["fetch"].attempt == 1
    assert wf2.tasks["wobbly"].attempt == 3  # 重启后没有重跑
    # 组的终态与成员结论在重启后保持一致
    g2 = svc2.get_group("demo-group")
    print(f"\nafter restart: group={g2.id} status={g2.status.value}")
    print("reason:", g2.reason)
    assert g2.status.value == "FAILED"
    print("\nstate file:", STATE)
    print("DEMO OK")
    sch2.stop()


if __name__ == "__main__":
    main()
