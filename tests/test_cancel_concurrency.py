"""场景七:取消/终止与执行任意交错的并发健壮性(死锁回归)。

背景:单工作流取消曾在 store.mutate 持有的【存储锁】内再获取【工作流锁】,
与完成回调(工作流锁 -> 存储锁)构成 AB-BA 死锁:任务边跑边取消时,
取消接口永不返回、调度循环停摆,整个服务卡死只能强杀。

本文件用看门狗超时保证(超时即失败):
- 任务边跑边取消/终止:任何交错下取消/终止都及时返回,调度继续推进;
- 多线程反复取消/终止同一对象:幂等,无部分生效/永久挂起的中间态;
- 取消已取消/已跑完的对象:确定性响应,已成功任务绝不重跑;
- 取消与执行交错后进程重启:从状态文件恢复出同一个结论。
"""
from __future__ import annotations

import threading
import time

import pytest

from orchestrator.errors import TerminalStateError
from orchestrator.models import (
    GroupStatus,
    GroupWorkflowDef,
    TaskDef,
    TaskStatus,
    WorkflowStatus,
)

# 单个用例的看门狗上限:正常应在数秒内完成,超过即按死锁失败处理
_WATCHDOG_SECONDS = 20.0


def _pass(tid: str, deps: list[str] | None = None) -> TaskDef:
    return TaskDef(id=tid, action="pass", dependencies=deps or [])


def _sleep(tid: str, seconds: float, deps: list[str] | None = None) -> TaskDef:
    return TaskDef(id=tid, action="sleep", params={"seconds": seconds}, dependencies=deps or [])


def _wf(wid: str, *tasks: TaskDef) -> GroupWorkflowDef:
    return GroupWorkflowDef(workflow_id=wid, tasks=list(tasks))


def _start_threads(targets) -> list[threading.Thread]:
    threads = [
        threading.Thread(target=fn, name=f"storm-{i}", daemon=True)
        for i, fn in enumerate(targets)
    ]
    for t in threads:
        t.start()
    return threads


def _join_with_watchdog(threads: list[threading.Thread], timeout: float = _WATCHDOG_SECONDS) -> None:
    """在看门狗时限内等全部线程结束;有线程没结束即判定卡死,测试失败。"""
    deadline = time.monotonic() + timeout
    for t in threads:
        t.join(timeout=max(0.0, deadline - time.monotonic()))
    stuck = [t.name for t in threads if t.is_alive()]
    assert not stuck, f"取消/终止与执行交错后线程未在看门狗时限内返回(疑似死锁): {stuck}"


def _wait_for(predicate, timeout: float = 5.0, interval: float = 0.005) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _assert_no_unfinished_tasks(store) -> None:
    """全部工作流/任务都必须到达确定终态,不允许 RUNNING/PENDING 残留。"""
    for wf in store.load_all().values():
        assert wf.status.is_terminal, f"workflow={wf.id} 未收敛: {wf.status}"
        for t in wf.tasks.values():
            assert t.status.is_terminal, f"task={wf.id}/{t.id} 残留非终态: {t.status}"


# ================= 任务边跑边取消/终止:及时返回、确定性收敛 =================
def test_cancel_storm_during_execution_always_returns_and_converges(env):
    """执行中反复取消/终止多个工作流:接口必须及时返回,状态确定性收敛。

    短 sleep 任务让完成回调非常密集,最大化与取消路径的锁交错面
    (修复前:完成回调持工作流锁等存储锁,取消持存储锁等工作流锁,AB-BA 死锁)。
    """
    e = env(max_concurrency=4)
    for i in range(6):
        e.service.submit_workflow(
            f"wf-{i}",
            [_sleep(f"t{j}", 0.05) for j in range(6)],
        )
    e.scheduler.start()
    try:
        def hammer(wid: str) -> None:
            for _ in range(30):
                try:
                    e.service.cancel_workflow(wid)
                except TerminalStateError:
                    return  # 已跑完:确定性的终态拒绝,属正常返回
                try:
                    e.service.terminate_workflow(wid)
                except TerminalStateError:
                    return
                time.sleep(0.001)

        threads = _start_threads([lambda wid=f"wf-{i}": hammer(wid) for i in range(6)])
        _join_with_watchdog(threads)
        # 取消生效后,调度必须能继续推进到全部收敛(不被死锁残留阻塞)
        e.scheduler.run_until_idle(timeout=10)
    finally:
        e.scheduler.stop()
    _assert_no_unfinished_tasks(e.store)
    for wf in e.store.load_all().values():
        if wf.cancel_requested:
            assert wf.status == WorkflowStatus.CANCELLED
        for t in wf.tasks.values():
            if t.status == TaskStatus.SUCCEEDED:
                assert t.attempt == 1, "已成功的任务绝不重跑"


def test_group_cancel_terminate_storm_during_execution(env):
    """组级取消/终止风暴 + 夹杂单成员取消:组与成员收敛到一致的确定终态。"""
    e = env(max_concurrency=4)
    e.service.submit_group(
        "g",
        [
            _wf("w1", _sleep("s", 0.3)),
            _wf("w2", _sleep("s", 0.3), _pass("b", ["s"])),
            _wf("w3", _pass("x"), _sleep("s", 0.3, ["x"])),
        ],
        group_key="k",
    )
    e.scheduler.start()
    try:
        assert _wait_for(lambda: e.scheduler.running_count > 0), "任务未进入执行"

        def hammer(i: int) -> None:
            for _ in range(20):
                try:
                    if i % 2 == 0:
                        e.service.cancel_group("g")
                    else:
                        e.service.terminate_group("g")
                except TerminalStateError:
                    pass
                try:  # 组级与单工作流级取消交错,制造锁竞争
                    e.service.cancel_workflow("w2")
                except TerminalStateError:
                    pass

        threads = _start_threads([lambda i=i: hammer(i) for i in range(6)])
        _join_with_watchdog(threads)
        e.scheduler.run_until_idle(timeout=10)
    finally:
        e.scheduler.stop()
    g = e.service.get_group("g")
    assert g.status == GroupStatus.CANCELLED
    assert g.cancel_requested
    _assert_no_unfinished_tasks(e.store)
    members = e.service.get_group_members("g")
    for wid, wf in members.items():
        # 组取消已生效:成员要么已成功(绝不重跑),要么收敛为 CANCELLED
        assert wf.status in (WorkflowStatus.SUCCEEDED, WorkflowStatus.CANCELLED)
        if wf.status == WorkflowStatus.SUCCEEDED:
            assert all(t.status == TaskStatus.SUCCEEDED for t in wf.tasks.values())


# ================= 多线程反复取消/终止同一对象:幂等 =================
def test_concurrent_repeated_cancel_terminate_is_idempotent(env):
    """多线程对同一工作流反复取消/终止:全部正常返回,取消只生效一次。"""
    e = env()
    e.service.submit_workflow("wf", [_sleep("s", 0.3), _pass("b", ["s"])])
    e.scheduler.start()
    try:
        assert _wait_for(lambda: e.scheduler.running_count > 0), "任务未进入执行"
        seen: list[bool] = []

        def op(i: int) -> None:
            for _ in range(15):
                wf = (
                    e.service.cancel_workflow("wf")
                    if i % 2 == 0
                    else e.service.terminate_workflow("wf")
                )
                seen.append(wf.cancel_requested)  # 每次返回都必须看到取消已生效

        threads = _start_threads([lambda i=i: op(i) for i in range(8)])
        _join_with_watchdog(threads)
        e.scheduler.run_until_idle(timeout=10)
    finally:
        e.scheduler.stop()
    assert seen and all(seen), "存在取消未生效就返回的中间态"
    wf = e.service.get_workflow("wf")
    assert wf.status == WorkflowStatus.CANCELLED
    # 幂等:无论并发调用多少次,进入 CANCELLED 的转移只有一条
    cancels = [x for x in wf.transitions if x["to"] == "CANCELLED"]
    assert len(cancels) == 1
    _assert_no_unfinished_tasks(e.store)


def test_repeated_group_cancel_terminate_is_idempotent(env):
    """多线程反复组级取消/终止:全部正常返回同一终态,组只收敛一次。"""
    e = env()
    e.service.submit_group(
        "g",
        [_wf("w1", _sleep("s", 0.3)), _wf("w2", _sleep("s", 0.3))],
        group_key="k",
    )
    e.scheduler.start()
    try:
        assert _wait_for(lambda: e.scheduler.running_count > 0), "任务未进入执行"

        def op(i: int) -> None:
            for _ in range(15):
                g = e.service.cancel_group("g") if i % 2 == 0 else e.service.terminate_group("g")
                assert g.cancel_requested

        threads = _start_threads([lambda i=i: op(i) for i in range(8)])
        _join_with_watchdog(threads)
        e.scheduler.run_until_idle(timeout=10)
    finally:
        e.scheduler.stop()
    g = e.service.get_group("g")
    assert g.status == GroupStatus.CANCELLED
    cancels = [x for x in g.transitions if x["to"] == "CANCELLED"]
    assert len(cancels) == 1
    _assert_no_unfinished_tasks(e.store)


# ================= 取消已取消/已跑完的对象:确定性响应,绝不重跑 =================
def test_cancel_terminate_on_succeeded_is_refused_without_hanging(env):
    """已成功的工作流:并发取消/终止立即得到确定性拒绝,成功结论与任务不重跑。"""
    e = env()
    e.service.submit_workflow("wf", [_pass("a"), _pass("b", ["a"])])
    e.run()
    assert e.service.get_workflow("wf").status == WorkflowStatus.SUCCEEDED
    outcomes: list[str] = []

    def op(i: int) -> None:
        for _ in range(10):
            try:
                (e.service.cancel_workflow if i % 2 == 0 else e.service.terminate_workflow)("wf")
                outcomes.append("returned")
            except TerminalStateError:
                outcomes.append("refused")

    threads = _start_threads([lambda i=i: op(i) for i in range(8)])
    _join_with_watchdog(threads)
    assert outcomes and all(o == "refused" for o in outcomes)
    wf = e.service.get_workflow("wf")
    assert wf.status == WorkflowStatus.SUCCEEDED
    assert all(t.attempt == 1 for t in wf.tasks.values()), "已成功的任务绝不重跑"


def test_cancel_already_cancelled_is_idempotent_noop(env):
    """已取消的对象:反复取消/终止幂等返回同一终态,不出现中间态。"""
    e = env()
    e.service.submit_workflow("wf", [_sleep("s", 0.3)])
    e.scheduler.start()
    try:
        assert _wait_for(lambda: e.scheduler.running_count > 0)
        e.service.terminate_workflow("wf")
        e.scheduler.run_until_idle(timeout=10)
    finally:
        e.scheduler.stop()
    assert e.service.get_workflow("wf").status == WorkflowStatus.CANCELLED

    def op(i: int) -> None:
        for _ in range(10):
            wf = (
                e.service.cancel_workflow("wf")
                if i % 2 == 0
                else e.service.terminate_workflow("wf")
            )
            assert wf.status == WorkflowStatus.CANCELLED  # 每次返回都是同一终态

    threads = _start_threads([lambda i=i: op(i) for i in range(8)])
    _join_with_watchdog(threads)
    wf = e.service.get_workflow("wf")
    assert wf.status == WorkflowStatus.CANCELLED
    assert len([x for x in wf.transitions if x["to"] == "CANCELLED"]) == 1


# ================= 交错后重启恢复:结论一致、无残留 =================
def test_recovery_after_terminate_midflight_converges_to_same_conclusion(env):
    """执行中终止后立刻"崩溃":重启恢复出同一个结论,无 RUNNING 残留,成功不重跑。"""
    e = env()
    e.service.submit_group(
        "g",
        [
            _wf("w-done", _pass("a")),
            _wf("w-slow", _sleep("s", 0.4)),
        ],
        group_key="k",
    )
    e.scheduler.start()
    assert _wait_for(
        lambda: e.service.get_workflow("w-done").status == WorkflowStatus.SUCCEEDED
        and e.scheduler.running_count > 0
    ), "未进入预期中间态(w-done 成功、w-slow 在执行)"
    e.service.terminate_group("g")  # 组立即终态,w-slow 的任务仍在执行
    e.scheduler.stop(wait=False)  # 模拟进程崩溃:不等在执行任务收敛

    # 进程重启:同一状态文件上的全新调度器
    sch2 = e.new_scheduler_on_same_store()
    sch2.run_until_idle(timeout=8)
    g = e.service.get_group("g")
    assert g.status == GroupStatus.CANCELLED, "组终态结论重启后必须保持一致"
    members = e.service.get_group_members("g")
    assert members["w-done"].status == WorkflowStatus.SUCCEEDED
    assert members["w-done"].tasks["a"].attempt == 1, "已成功的任务绝不重跑"
    assert members["w-slow"].status == WorkflowStatus.CANCELLED
    # 崩溃时残留 RUNNING 的任务,恢复后必须确定性收敛为 CANCELLED
    assert members["w-slow"].tasks["s"].status == TaskStatus.CANCELLED
    _assert_no_unfinished_tasks(e.store)


def test_recovery_after_cancel_storm_keeps_conclusions(env):
    """取消风暴与执行交错后重启:组/成员结论与已成功内容不丢不乱。"""
    e = env(max_concurrency=4)
    e.service.submit_group(
        "g",
        [
            _wf("w-fast", _pass("a")),
            _wf("w-slow", _sleep("s", 0.4), _pass("b", ["s"])),
        ],
        group_key="k",
    )
    e.scheduler.start()
    try:
        assert _wait_for(lambda: e.scheduler.running_count > 0)

        def hammer(i: int) -> None:
            for _ in range(10):
                try:
                    e.service.cancel_group("g") if i % 2 == 0 else e.service.cancel_workflow("w-slow")
                except TerminalStateError:
                    pass

        threads = _start_threads([lambda i=i: hammer(i) for i in range(4)])
        _join_with_watchdog(threads)
        e.scheduler.run_until_idle(timeout=10)
    finally:
        e.scheduler.stop()
    before = {
        wid: (wf.status, {tid: t.status for tid, t in wf.tasks.items()})
        for wid, wf in e.store.load_all().items()
    }
    g_before = e.service.get_group("g").status

    # 重启恢复:结论必须与崩溃前完全一致
    sch2 = e.new_scheduler_on_same_store()
    sch2.run_until_idle(timeout=8)
    assert e.service.get_group("g").status == g_before == GroupStatus.CANCELLED
    for wid, (status, tasks) in before.items():
        wf = e.service.get_workflow(wid)
        assert wf.status == status, f"workflow={wid} 结论在重启后改变"
        for tid, t_status in tasks.items():
            assert wf.tasks[tid].status == t_status, f"task={wid}/{tid} 结论在重启后改变"
    assert e.service.get_workflow("w-fast").tasks["a"].attempt == 1
    _assert_no_unfinished_tasks(e.store)


# ================= HTTP 端到端:取消/终止风暴不挂起 =================
def test_http_cancel_terminate_storm_never_hangs(fastapi_client):
    """HTTP 层:任务执行中并发取消/终止,所有请求及时返回确定响应。"""
    client, service, sched = fastapi_client
    resp = client.post(
        "/groups",
        json={
            "group_id": "g",
            "group_key": "k",
            "workflows": [
                {
                    "workflow_id": f"w{i}",
                    "tasks": [
                        {"id": "s", "action": "sleep", "params": {"seconds": 0.3}}
                    ],
                }
                for i in range(3)
            ],
        },
    )
    assert resp.status_code == 201
    assert _wait_for(lambda: sched.running_count > 0), "任务未进入执行"
    results: list[int] = []

    def hammer(i: int) -> None:
        for _ in range(10):
            r = client.post(f"/groups/g/{'cancel' if i % 2 == 0 else 'terminate'}")
            results.append(r.status_code)
            r2 = client.post("/workflows/w0/cancel")
            results.append(r2.status_code)

    threads = _start_threads([lambda i=i: hammer(i) for i in range(4)])
    _join_with_watchdog(threads)
    assert results and all(code in (200, 409) for code in results)
    assert _wait_for(
        lambda: service.get_group("g").status.is_terminal, timeout=10
    ), "组未在看门狗时限内收敛"
    assert service.get_group("g").status == GroupStatus.CANCELLED
    _assert_no_unfinished_tasks(service.store)
