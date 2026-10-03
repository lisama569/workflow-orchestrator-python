"""场景六:工作流组。

- 同一组键并发重复提交:最终只落地一个组,成员工作流只建一次、只执行一次;
- 组键复用但内容不一致:GROUP_KEY_CONFLICT,与校验失败可区分,并指明已落地的组;
- 整组提交全有或全无:任一成员校验失败 -> 无任何持久化;
- 组级收敛:全部成员成功 -> 组成功;有成员失败 -> 组失败且结论可解释,
  未开始的工作流/任务不被悄悄跳过;
- 组级取消/终止:不可逆、幂等,作用到所有未完成成员,已成功成员绝不重跑;
- 重启恢复:组终态与成员结论不丢失,未完成成员继续推进到同一结论;
- 并发上限:组内工作流再多也不突破 max_concurrency。
"""
from __future__ import annotations

import threading
import time

import pytest

from orchestrator.errors import (
    CycleDetectedError,
    DuplicateWorkflowIdError,
    EmptyGroupError,
    GroupKeyConflictError,
    GroupNotFoundError,
    TerminalStateError,
    WorkflowIdConflictError,
)
from orchestrator.models import (
    GroupStatus,
    GroupWorkflowDef,
    RetryPolicy,
    TaskDef,
    TaskStatus,
    WorkflowStatus,
)


def _wf(wid: str, *tasks: TaskDef, key: str | None = None) -> GroupWorkflowDef:
    return GroupWorkflowDef(workflow_id=wid, tasks=list(tasks), idempotency_key=key)


def _pass(tid: str, deps: list[str] | None = None) -> TaskDef:
    return TaskDef(id=tid, action="pass", dependencies=deps or [])


# ================= 并发重复提交:同键只落地一个组 =================
def test_concurrent_same_group_key_lands_exactly_one_group(env):
    e = env()
    spec = lambda: [  # noqa: E731
        _wf("w1", _pass("a")),
        _wf("w2", _pass("b"), _pass("c", ["b"])),
    ]
    results: list = []
    errors: list = []

    def submit(i: int) -> None:
        try:
            # 并发提交用不同的 group_id、同一个 group_key
            results.append(e.service.submit_group(f"g-{i}", spec(), group_key="batch-001"))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=submit, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 同键同内容:全部幂等返回同一个组,没有任何冲突/异常
    assert errors == []
    assert len(results) == 8
    landed = {g.id for g in results}
    assert len(landed) == 1, f"同键并发提交落地了多个组: {landed}"
    # 组只有一个,成员工作流只建了一次
    assert len(e.store.load_groups()) == 1
    assert sorted(e.store.load_all()) == ["w1", "w2"]
    e.run()
    group = results[0]
    assert group.status == GroupStatus.SUCCEEDED
    # 每个任务恰好执行过一次(attempt=1, 只有一条 RUNNING 转移)
    for wf in e.store.load_all().values():
        for t in wf.tasks.values():
            assert t.attempt == 1
            assert [x["to"] for x in t.transitions].count("RUNNING") == 1


def test_same_key_same_content_after_completion_is_idempotent_replay(env):
    e = env()
    g1 = e.service.submit_group("g1", [_wf("w1", _pass("a"))], group_key="k")
    e.run()
    g2 = e.service.submit_group("g-other", [_wf("w1", _pass("a"))], group_key="k")
    assert g2.id == g1.id  # 返回已落地的组,而不是新建
    assert g2.status == GroupStatus.SUCCEEDED
    assert len(e.store.load_groups()) == 1
    assert len(e.store.load_all()) == 1


def test_same_key_different_content_rejected_with_distinguishable_reason(env):
    e = env()
    e.service.submit_group("g1", [_wf("w1", _pass("a"))], group_key="k")
    with pytest.raises(GroupKeyConflictError) as exc:
        e.service.submit_group("g2", [_wf("w2", _pass("b"))], group_key="k")
    # 与校验失败(EMPTY_GROUP / CYCLE_DETECTED 等)不同的错误码
    assert exc.value.error_code == "GROUP_KEY_CONFLICT"
    assert exc.value.error_code not in ("EMPTY_GROUP", "CYCLE_DETECTED", "WORKFLOW_VALIDATION_ERROR")
    # 能告诉调用方最终落到哪个已有组
    assert exc.value.existing_group_id == "g1"
    # 拒绝即无任何副作用
    assert len(e.store.load_groups()) == 1
    assert sorted(e.store.load_all()) == ["w1"]


# ================= 整组提交:全有或全无 =================
def test_group_validation_failure_persists_nothing(env):
    e = env()
    with pytest.raises(EmptyGroupError) as exc:
        e.service.submit_group("g", [], group_key="k")
    assert exc.value.error_code == "EMPTY_GROUP"

    with pytest.raises(DuplicateWorkflowIdError) as exc:
        e.service.submit_group("g", [_wf("w1", _pass("a")), _wf("w1", _pass("b"))])
    assert exc.value.error_code == "DUPLICATE_WORKFLOW_ID"

    # 组内任一工作流任务图非法(环) -> 整组拒绝
    with pytest.raises(CycleDetectedError):
        e.service.submit_group(
            "g",
            [
                _wf("w1", _pass("a")),
                _wf("w2", TaskDef(id="x", action="pass", dependencies=["x"])),
            ],
        )
    assert e.store.load_groups() == {}
    assert e.store.load_all() == {}


def test_member_workflow_id_conflict_rejects_whole_group(env):
    e = env()
    e.service.submit_workflow("w1", [_pass("a")])  # 已有单工作流占用 id
    with pytest.raises(WorkflowIdConflictError) as exc:
        e.service.submit_group("g", [_wf("w2", _pass("b")), _wf("w1", _pass("c"))])
    assert exc.value.error_code == "WORKFLOW_ID_CONFLICT"
    assert exc.value.workflow_id == "w1"
    # 全有或全无:w2 也不能被建出来
    assert e.store.load_groups() == {}
    assert sorted(e.store.load_all()) == ["w1"]


# ================= 组级收尾:全部成员终态后收敛 =================
def test_group_succeeds_only_when_all_members_succeed(env):
    e = env()
    g = e.service.submit_group(
        "g",
        [
            _wf("w1", _pass("a"), _pass("b", ["a"])),
            _wf("w2", _pass("c")),
            _wf("w3", _pass("d"), _pass("e", ["d"])),
        ],
        group_key="k",
    )
    e.run()
    assert g.status == GroupStatus.SUCCEEDED
    assert g.reason is None
    members = e.service.get_group_members("g")
    assert [w.status for w in members.values()] == [WorkflowStatus.SUCCEEDED] * 3
    # 组转移记录可解释
    assert g.transitions[-1]["to"] == "SUCCEEDED"
    assert "all 3 workflow(s) succeeded" in g.transitions[-1]["reason"]


def test_member_failure_converges_group_to_failed_with_explanation(env):
    e = env()
    g = e.service.submit_group(
        "g",
        [
            # 失败成员:任务 a 不可重试失败,下游 b 确定性 SKIPPED
            _wf(
                "w-bad",
                TaskDef(id="a", action="fail", params={"kind": "non_retryable", "message": "boom"}),
                _pass("b", ["a"]),
            ),
            # 正常成员:不受组内其他成员失败影响,语义与单独提交一致
            _wf("w-ok", _pass("x"), _pass("y", ["x"])),
        ],
        group_key="k",
    )
    e.run()
    assert g.status == GroupStatus.FAILED
    # 结论可解释:指明是哪个成员、什么原因
    assert "w-bad=FAILED" in g.reason
    assert "boom" in g.reason
    members = e.service.get_group_members("g")
    assert members["w-bad"].status == WorkflowStatus.FAILED
    assert members["w-ok"].status == WorkflowStatus.SUCCEEDED
    # 未开始的任务不被悄悄跳过:b 有明确的 SKIPPED 终态与判定依据
    b = members["w-bad"].tasks["b"]
    assert b.status == TaskStatus.SKIPPED
    assert b.skipped_by == ["a"]
    assert any("dependency never succeeded: a" in x["reason"] for x in b.transitions)


def test_group_failure_does_not_change_member_workflow_semantics(env):
    """放进组里不改变单工作流语义:依赖推进、重试、失败传播与单独提交一致。"""
    e = env()
    g = e.service.submit_group(
        "g",
        [
            _wf(
                "w1",
                TaskDef(
                    id="flaky",
                    action="flaky",
                    params={"fail_times": 1},
                    retry_policy=RetryPolicy(max_retries=2, base_delay_seconds=0.005),
                ),
                _pass("downstream", ["flaky"]),
            )
        ],
    )
    e.run()
    wf = e.service.get_workflow("w1")
    t = wf.tasks["flaky"]
    assert t.attempt == 2  # 重试一次后成功
    assert ("RUNNING", "PENDING") in [(x["from"], x["to"]) for x in t.transitions]
    assert wf.tasks["downstream"].status == TaskStatus.SUCCEEDED
    assert wf.status == WorkflowStatus.SUCCEEDED
    assert g.status == GroupStatus.SUCCEEDED


# ================= 组级取消 / 终止 =================
def test_group_cancel_applies_to_unfinished_members_and_is_idempotent(env):
    e = env()
    g = e.service.submit_group(
        "g",
        [
            _wf("w-fast", _pass("a")),
            _wf("w-slow", TaskDef(id="s", action="sleep", params={"seconds": 0.3})),
            _wf("w-waiting", TaskDef(id="s2", action="sleep", params={"seconds": 0.3})),
        ],
        group_key="k",
    )
    e.scheduler.start()
    try:
        deadline = time.time() + 5
        while time.time() < deadline:  # 等 w-fast 跑完、其余进入执行
            e.scheduler.tick()
            if e.service.get_workflow("w-fast").status == WorkflowStatus.SUCCEEDED:
                break
            time.sleep(0.01)
        e.service.cancel_group("g")
        g2 = e.service.cancel_group("g")  # 重复取消:幂等
        assert g2.cancel_requested
        e.scheduler.run_until_idle(timeout=8)
    finally:
        e.scheduler.stop()
    g = e.service.get_group("g")
    assert g.status == GroupStatus.CANCELLED
    members = e.service.get_group_members("g")
    # 已成功的成员保持 SUCCEEDED,绝不重跑
    assert members["w-fast"].status == WorkflowStatus.SUCCEEDED
    assert members["w-fast"].tasks["a"].attempt == 1
    # 未完成的成员收敛到 CANCELLED,任务有明确终态
    assert members["w-slow"].status == WorkflowStatus.CANCELLED
    assert members["w-waiting"].status == WorkflowStatus.CANCELLED
    for wid in ("w-slow", "w-waiting"):
        for t in members[wid].tasks.values():
            assert t.status in (TaskStatus.CANCELLED, TaskStatus.SUCCEEDED)


def test_group_terminate_is_immediate_terminal_and_idempotent(env):
    e = env()
    e.service.submit_group(
        "g",
        [_wf("w1", TaskDef(id="s", action="sleep", params={"seconds": 0.5}))],
        group_key="k",
    )
    e.scheduler.start()
    try:
        time.sleep(0.1)  # 让任务进入执行
        g = e.service.terminate_group("g")
        assert g.status == GroupStatus.CANCELLED  # 组立即终态
        g2 = e.service.terminate_group("g")  # 重复终止:幂等返回同一终态
        assert g2.status == GroupStatus.CANCELLED
        e.scheduler.run_until_idle(timeout=8)
    finally:
        e.scheduler.stop()
    g = e.service.get_group("g")
    assert g.status == GroupStatus.CANCELLED
    wf = e.service.get_workflow("w1")
    assert wf.status == WorkflowStatus.CANCELLED
    assert wf.tasks["s"].status == TaskStatus.CANCELLED
    # 已 CANCELLED 的组上再取消也是幂等 no-op
    assert e.service.cancel_group("g").status == GroupStatus.CANCELLED


def test_cancel_terminal_succeeded_group_is_refused(env):
    e = env()
    e.service.submit_group("g", [_wf("w1", _pass("a"))])
    e.run()
    with pytest.raises(TerminalStateError):
        e.service.cancel_group("g")
    with pytest.raises(GroupNotFoundError) as exc:
        e.service.cancel_group("ghost")
    assert exc.value.error_code == "GROUP_NOT_FOUND"


# ================= 重启恢复 =================
def test_recovery_keeps_group_terminal_state_and_member_conclusions(env):
    e = env()
    e.service.submit_group(
        "g",
        [
            _wf("w-ok", _pass("a")),
            _wf("w-bad", TaskDef(id="x", action="fail", params={"kind": "non_retryable"})),
        ],
        group_key="k",
    )
    e.run()
    assert e.service.get_group("g").status == GroupStatus.FAILED

    # 模拟进程重启:同一状态文件上的全新调度器
    sch2 = e.new_scheduler_on_same_store()
    sch2.run_until_idle(timeout=8)
    g = e.service.get_group("g")
    assert g.status == GroupStatus.FAILED  # 组终态不丢失
    assert "w-bad=FAILED" in g.reason
    members = e.service.get_group_members("g")
    assert members["w-ok"].status == WorkflowStatus.SUCCEEDED
    assert members["w-ok"].tasks["a"].attempt == 1  # 成功任务绝不重跑
    assert members["w-bad"].status == WorkflowStatus.FAILED


def test_recovery_resumes_unfinished_members_to_same_conclusion(env):
    e = env()
    e.service.submit_group(
        "g",
        [
            _wf("w-done", _pass("a")),
            _wf(
                "w-retry",
                TaskDef(
                    id="flaky",
                    action="flaky",
                    params={"fail_times": 1},
                    retry_policy=RetryPolicy(max_retries=2, base_delay_seconds=0.01),
                ),
            ),
        ],
        group_key="k",
    )
    # 只推进一部分:w-done 完成,w-retry 尚未开始(模拟崩溃前的中间态)
    e.scheduler.tick()
    deadline = time.time() + 5
    while time.time() < deadline and e.service.get_workflow("w-done").status != WorkflowStatus.SUCCEEDED:
        e.scheduler.tick()
        time.sleep(0.01)
    assert e.service.get_workflow("w-done").status == WorkflowStatus.SUCCEEDED

    # 重启:全新调度器从状态文件恢复
    sch2 = e.new_scheduler_on_same_store()
    sch2.run_until_idle(timeout=8)
    g = e.service.get_group("g")
    assert g.status == GroupStatus.SUCCEEDED  # 继续推进到同一结论
    members = e.service.get_group_members("g")
    assert members["w-done"].tasks["a"].attempt == 1  # 已成功的不重跑
    assert members["w-retry"].tasks["flaky"].attempt == 2  # 未完成的继续重试到成功
    assert members["w-retry"].status == WorkflowStatus.SUCCEEDED


def test_recovery_honours_persisted_group_cancel(env):
    e = env()
    e.service.submit_group(
        "g",
        [_wf("w1", TaskDef(id="s", action="sleep", params={"seconds": 0.2}), _pass("b", ["s"]))],
        group_key="k",
    )
    e.scheduler.start()
    try:
        time.sleep(0.05)
        e.service.cancel_group("g")
    finally:
        e.scheduler.stop()  # 模拟进程退出(取消标志已持久化)

    sch2 = e.new_scheduler_on_same_store()
    sch2.run_until_idle(timeout=8)
    g = e.service.get_group("g")
    assert g.status == GroupStatus.CANCELLED  # 组取消跨重启生效
    wf = e.service.get_workflow("w1")
    assert wf.status == WorkflowStatus.CANCELLED
    # 未开始的任务 b 有明确的 CANCELLED 终态,不是悄悄消失
    assert wf.tasks["b"].status == TaskStatus.CANCELLED


# ================= 并发上限 =================
class _ConcurrencyProbe:
    """记录并发进入数的探针执行器包装。"""

    def __init__(self):
        self.active = 0
        self.peak = 0
        self.executions: dict[str, int] = {}
        self.guard = threading.Lock()
        from orchestrator.executor import Executor

        self._inner = Executor()

    def execute(self, action, params, attempt, task_id="", cancelled=None):
        with self.guard:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.executions[task_id] = self.executions.get(task_id, 0) + 1
        time.sleep(0.03)
        try:
            return self._inner.execute(action, params, attempt, task_id, cancelled)
        finally:
            with self.guard:
                self.active -= 1


def test_group_members_respect_global_concurrency_cap(env):
    e = env(max_concurrency=2)
    probe = _ConcurrencyProbe()
    e.scheduler.executor = probe
    # 4 个工作流 x 3 个任务 = 12 个任务,上限 2
    workflows = [
        _wf(f"w{i}", *[_pass(f"t{i}-{j}") for j in range(3)]) for i in range(4)
    ]
    e.service.submit_group("g", workflows, group_key="k")
    e.run(timeout=10)
    assert e.service.get_group("g").status == GroupStatus.SUCCEEDED
    assert probe.peak <= 2, f"组内并发突破上限: peak={probe.peak}"
    assert probe.peak == 2
    assert all(c == 1 for c in probe.executions.values())
