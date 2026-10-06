"""场景七(缺陷修复回归):执行中任意交错取消/终止不得卡死,且结论确定。

重点覆盖压测中踩到的现象:任务还在跑时,多线程反复调用取消/终止,
整个服务(后台推进、取消接口)一起永久卡住。根因是完成回调的加锁顺序
(工作流锁 -> 存储锁)与取消路径(存储锁 -> 工作流锁)构成 AB-BA 死锁。

本模块用例约定:
  - 所有“等待另一个线程”的地方都带超时,超时即判失败,不允许挂住测试进程;
  - 调度日志(pytest log_cli, INFO)逐条打印 组/工作流/任务 的状态转移,
    可直接看出每个对象是怎么一步步判定到终态的。
"""
from __future__ import annotations

import threading
import time

import pytest

from orchestrator.errors import TerminalStateError
from orchestrator.models import (
    GroupStatus,
    GroupWorkflowDef,
    RetryPolicy,
    TaskDef,
    TaskStatus,
    WorkflowStatus,
)


def _sleep(tid: str, seconds: float = 0.1, deps: list[str] | None = None) -> TaskDef:
    return TaskDef(
        id=tid, action="sleep", params={"seconds": seconds}, dependencies=deps or []
    )


def _pass(tid: str, deps: list[str] | None = None) -> TaskDef:
    return TaskDef(id=tid, action="pass", dependencies=deps or [])


def _wait_running(service, wf_id: str, task_id: str, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if service.get_task(wf_id, task_id).status == TaskStatus.RUNNING:
            return
        time.sleep(0.005)
    raise AssertionError(f"task={wf_id}/{task_id} 未在 {timeout}s 内进入 RUNNING")


def _join_all(threads: list[threading.Thread], timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    for t in threads:
        remain = max(0.0, deadline - time.time())
        t.join(timeout=remain)
        assert not t.is_alive(), f"控制面线程 {t.name} 在 {timeout}s 内未返回(疑似死锁)"


def _assert_wf_terminal_consistent(service, wf_id: str) -> None:
    wf = service.get_workflow(wf_id)
    assert wf.status.is_terminal, f"workflow={wf_id} 未到终态: {wf.status}"
    for t in wf.tasks.values():
        assert t.status.is_terminal, f"task={wf_id}/{t.id} 悬挂在 {t.status}"
        # 已执行过的任务至多有一次 RUNNING 派发:成功的内容绝不重跑
        runs = [x for x in t.transitions if x["to"] == "RUNNING"]
        assert len(runs) <= 1, (wf_id, t.id, t.transitions)


# ================= 单工作流:边跑边取消/终止 =================
def test_cancel_while_task_running_returns_promptly_and_converges(env):
    e = env()
    e.service.submit_workflow("wf", [_sleep("a", 2.0), _pass("b", ["a"])])
    e.scheduler.start()
    try:
        _wait_running(e.service, "wf", "a")
        done = threading.Event()

        def do_cancel() -> None:
            e.service.cancel_workflow("wf")
            done.set()

        th = threading.Thread(target=do_cancel)
        th.start()
        th.join(timeout=5.0)
        assert not th.is_alive(), "取消接口在任务执行中永久挂起"
        assert done.is_set()
        # 后台推进没有被卡住,工作流在在执行任务自然结束后收敛
        e.scheduler.run_until_idle(timeout=8)
    finally:
        e.scheduler.stop()
    wf = e.service.get_workflow("wf")
    assert wf.status == WorkflowStatus.CANCELLED
    assert wf.cancel_requested
    # a 在取消前已在跑(优雅取消允许其自然结束),b 不得开始
    assert wf.tasks["b"].status == TaskStatus.CANCELLED
    assert wf.tasks["a"].status in (TaskStatus.CANCELLED, TaskStatus.SUCCEEDED)


def test_terminate_while_task_running_is_immediate(env):
    e = env()
    e.service.submit_workflow("wf", [_sleep("a", 2.0), _pass("b", ["a"])])
    e.scheduler.start()
    try:
        _wait_running(e.service, "wf", "a")
        t0 = time.time()
        wf = e.service.terminate_workflow("wf")
        # 立即返回且工作流立即终态,不能等 2 秒的任务结束
        assert time.time() - t0 < 1.0
        assert wf.status == WorkflowStatus.CANCELLED
        e.scheduler.run_until_idle(timeout=8)
    finally:
        e.scheduler.stop()
    wf = e.service.get_workflow("wf")
    assert wf.tasks["a"].status == TaskStatus.CANCELLED
    assert wf.tasks["b"].status == TaskStatus.CANCELLED


def test_multi_thread_repeated_cancel_and_terminate_never_hangs(env):
    """原缺陷的核心复现:任务边跑,N 个线程反复取消/终止,全部必须及时返回。"""
    e = env(max_concurrency=3)
    tasks = [_sleep("t0", 0.05)]
    tasks += [_sleep(f"t{i}", 0.05, [f"t{i-1}"]) for i in range(1, 8)]
    e.service.submit_workflow("wf", tasks)
    e.scheduler.start()

    stop = threading.Event()
    errors: list[Exception] = []

    def hammer() -> None:
        n = 0
        while not stop.is_set():
            try:
                if n % 2 == 0:
                    e.service.cancel_workflow("wf")
                else:
                    e.service.terminate_workflow("wf")
            except TerminalStateError:
                pass  # 与其他线程交错时到达终态是正常的,不是挂起
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
                stop.set()
                return
            n += 1
            time.sleep(0)

    threads = [threading.Thread(target=hammer, name=f"hammer-{i}") for i in range(6)]
    try:
        for t in threads:
            t.start()
        time.sleep(1.0)  # 让取消/终止与任务执行、完成回调充分交错
        # 兜底:强制终止以确定性收敛(不做这步也应很快自然收敛)
        e.service.terminate_workflow("wf")
        stop.set()
        _join_all(threads, timeout=5.0)
        e.scheduler.run_until_idle(timeout=8)
    finally:
        stop.set()
        e.scheduler.stop()
    assert errors == []
    _assert_wf_terminal_consistent(e.service, "wf")


# ================= 幂等:重复取消、多线程同时取消、对终态取消 =================
def test_repeated_and_concurrent_cancel_are_idempotent(env):
    e = env()
    e.service.submit_workflow("wf", [_sleep("a", 0.3), _pass("b", ["a"])])
    e.scheduler.start()
    results: list = []
    errors: list[Exception] = []
    barrier = threading.Barrier(6, timeout=5)

    def cancel_once() -> None:
        try:
            barrier.wait()  # 尽量让多个线程同时撞进来
            results.append(e.service.cancel_workflow("wf"))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    try:
        _wait_running(e.service, "wf", "a")
        threads = [threading.Thread(target=cancel_once) for _ in range(6)]
        for t in threads:
            t.start()
        _join_all(threads)
        e.scheduler.run_until_idle(timeout=8)
        # 已经 CANCELLED 后再取消/终止,同样正常幂等返回
        assert e.service.cancel_workflow("wf").status == WorkflowStatus.CANCELLED
        assert e.service.terminate_workflow("wf").status == WorkflowStatus.CANCELLED
    finally:
        e.scheduler.stop()
    assert errors == []
    assert len(results) == 6
    wf = e.service.get_workflow("wf")
    # 只有一条进入 RUNNING 的工作流转移记录(PENDING->RUNNING),取消没有制造中间态
    enters = [x for x in wf.transitions if x["to"] == "RUNNING"]
    assert len(enters) == 1
    assert wf.status == WorkflowStatus.CANCELLED
    _assert_wf_terminal_consistent(e.service, "wf")


def test_cancel_already_succeeded_is_refused_and_succeeded_not_rerun(env):
    e = env()
    e.service.submit_workflow("wf", [_pass("a"), _pass("b", ["a"])])
    e.run()
    wf = e.service.get_workflow("wf")
    assert wf.status == WorkflowStatus.SUCCEEDED
    with pytest.raises(TerminalStateError):
        e.service.cancel_workflow("wf")
    with pytest.raises(TerminalStateError):
        e.service.terminate_workflow("wf")
    # 再 tick 也不会重跑:每个任务只有一次 RUNNING
    e.run()
    for t in e.service.get_workflow("wf").tasks.values():
        assert t.status == TaskStatus.SUCCEEDED
        assert sum(1 for x in t.transitions if x["to"] == "RUNNING") == 1
        assert t.attempt == 1


def test_cancel_then_terminate_escalates_to_immediate_terminal(env):
    """先优雅取消(允许在执行任务自然结束),随后终止应立即升级为终态。"""
    e = env()
    e.service.submit_workflow("wf", [_sleep("a", 2.0), _sleep("b", 2.0, ["a"])])
    e.scheduler.start()
    try:
        _wait_running(e.service, "wf", "a")
        e.service.cancel_workflow("wf")
        t0 = time.time()
        wf = e.service.terminate_workflow("wf")
        assert time.time() - t0 < 1.0
        assert wf.status == WorkflowStatus.CANCELLED  # 立即终态,不等 2 秒任务
        e.scheduler.run_until_idle(timeout=8)
    finally:
        e.scheduler.stop()
    wf = e.service.get_workflow("wf")
    assert wf.status == WorkflowStatus.CANCELLED
    for t in wf.tasks.values():
        assert t.status == TaskStatus.CANCELLED


# ================= 组:边跑边取消/终止 + 多线程反复操作 =================
def _submit_running_group(env):
    members = [
        GroupWorkflowDef(
            workflow_id=f"w{i}",
            tasks=[_sleep("s", 0.15), _sleep("s2", 0.15, ["s"]), _pass("tail", ["s2"])],
        )
        for i in range(3)
    ]
    env.service.submit_group("g", members, group_key="gk")


def test_group_multi_thread_repeated_cancel_terminate_never_hangs(env):
    e = env(max_concurrency=4)
    _submit_running_group(e)
    e.scheduler.start()
    stop = threading.Event()
    errors: list[Exception] = []

    def hammer_group() -> None:
        n = 0
        while not stop.is_set():
            try:
                if n % 2 == 0:
                    e.service.cancel_group("g")
                else:
                    e.service.terminate_group("g")
            except TerminalStateError:
                pass
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
                stop.set()
                return
            n += 1
            time.sleep(0)

    def hammer_members() -> None:
        while not stop.is_set():
            for i in range(3):
                try:
                    e.service.terminate_workflow(f"w{i}")
                except TerminalStateError:
                    pass
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)
                    stop.set()
                    return
            time.sleep(0)

    threads = [threading.Thread(target=hammer_group, name=f"g-hammer-{i}") for i in range(4)]
    threads += [threading.Thread(target=hammer_members, name="w-hammer")]
    try:
        for t in threads:
            t.start()
        time.sleep(1.0)
        e.service.terminate_group("g")
        stop.set()
        _join_all(threads, timeout=5.0)
        e.scheduler.run_until_idle(timeout=10)
    finally:
        stop.set()
        e.scheduler.stop()
    assert errors == []
    g = e.service.get_group("g")
    assert g.status.is_terminal
    for wid, wf in e.service.get_group_members("g").items():
        _assert_wf_terminal_consistent(e.service, wid)
        assert wf.status == WorkflowStatus.CANCELLED


def test_group_cancel_after_terminate_and_repeat_are_idempotent(env):
    e = env()
    _submit_running_group(e)
    e.scheduler.start()
    try:
        time.sleep(0.05)
        g = e.service.terminate_group("g")
        assert g.status == GroupStatus.CANCELLED  # 立即终态
        # 终止之后再取消、再终止都幂等正常返回
        for _ in range(5):
            assert e.service.cancel_group("g").status == GroupStatus.CANCELLED
            assert e.service.terminate_group("g").status == GroupStatus.CANCELLED
        e.scheduler.run_until_idle(timeout=8)
    finally:
        e.scheduler.stop()
    for wid in e.service.get_group_members("g"):
        _assert_wf_terminal_consistent(e.service, wid)


def test_group_cancel_then_terminate_escalates(env):
    e = env()
    _submit_running_group(e)
    e.scheduler.start()
    try:
        time.sleep(0.05)
        e.service.cancel_group("g")
        wf = e.service.terminate_group("g")
        assert wf.status == GroupStatus.CANCELLED  # 升级为立即终态
        e.scheduler.run_until_idle(timeout=8)
    finally:
        e.scheduler.stop()
    g = e.service.get_group("g")
    assert g.status == GroupStatus.CANCELLED
    for wid in e.service.get_group_members("g"):
        _assert_wf_terminal_consistent(e.service, wid)


# ================= 成功内容不丢、不乱,重启结论一致 =================
def test_succeeded_tasks_survive_concurrent_cancel_and_restart(env):
    """一部分任务已成功、其余在跑时并发取消;重启后结论与成功内容都不变。"""
    e = env()
    e.service.submit_workflow(
        "wf",
        [
            _pass("done1"),
            _pass("done2", ["done1"]),
            _sleep("slow", 1.0, ["done2"]),
            _pass("tail", ["slow"]),
        ],
    )
    e.scheduler.start()
    try:
        deadline = time.time() + 5
        while time.time() < deadline:
            if e.service.get_task("wf", "done2").status == TaskStatus.SUCCEEDED:
                break
            time.sleep(0.005)
        assert e.service.get_task("wf", "done2").status == TaskStatus.SUCCEEDED
        # 多线程同时取消 + 终止
        ths = [
            threading.Thread(target=e.service.cancel_workflow, args=("wf",)),
            threading.Thread(target=e.service.terminate_workflow, args=("wf",)),
            threading.Thread(target=e.service.cancel_workflow, args=("wf",)),
        ]
        for t in ths:
            t.start()
        _join_all(ths, timeout=5.0)
        e.scheduler.run_until_idle(timeout=8)
    finally:
        e.scheduler.stop()

    wf = e.service.get_workflow("wf")
    assert wf.status == WorkflowStatus.CANCELLED
    # 已成功任务结论与结果保留,只执行过一次
    for tid in ("done1", "done2"):
        t = wf.tasks[tid]
        assert t.status == TaskStatus.SUCCEEDED
        assert t.attempt == 1
        assert t.result is not None
    assert wf.tasks["tail"].status == TaskStatus.CANCELLED

    # 进程重启:同一状态文件恢复,终态/成功内容一致,成功任务不重跑
    sch2 = e.new_scheduler_on_same_store()
    sch2.run_until_idle(timeout=8)
    wf2 = e.service.get_workflow("wf")
    assert wf2.status == WorkflowStatus.CANCELLED
    for tid in ("done1", "done2"):
        t = wf2.tasks[tid]
        assert t.status == TaskStatus.SUCCEEDED
        assert t.attempt == 1
    assert wf2.tasks["slow"].status == TaskStatus.CANCELLED
    assert wf2.tasks["tail"].status == TaskStatus.CANCELLED


def test_restart_after_terminate_with_running_task_keeps_cancelled_conclusion(env):
    """终止已落盘但完成回调没落盘就崩溃:重启不得重跑,残留任务补齐 CANCELLED。"""
    e = env()
    e.service.submit_workflow(
        "wf",
        [
            _sleep("a", 0.05),
            TaskDef(
                id="flaky",
                action="flaky",
                params={"fail_times": 1},
                retry_policy=RetryPolicy(max_retries=3, base_delay_seconds=10.0),
                dependencies=["a"],
            ),
            _pass("tail", ["flaky"]),
        ],
    )
    e.scheduler.start()
    try:
        _wait_running(e.service, "wf", "a")
        e.service.terminate_workflow("wf")  # 工作流立即 CANCELLED 并落盘
    finally:
        # 不等待在执行任务的完成回调,直接"杀掉进程"
        e.scheduler.stop(wait=False)

    sch2 = e.new_scheduler_on_same_store()
    sch2.run_until_idle(timeout=8)
    wf = e.service.get_workflow("wf")
    assert wf.status == WorkflowStatus.CANCELLED
    for t in wf.tasks.values():
        assert t.status.is_terminal
        assert t.status != TaskStatus.SUCCEEDED or t.attempt == 1
    assert wf.tasks["tail"].status == TaskStatus.CANCELLED
