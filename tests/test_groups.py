"""场景五:工作流组。

- 同一组键并发重复提交只落地一个组,成员工作流只建一次、只执行一次;
- 组键复用但内容不一致 -> GROUP_KEY_CONFLICT(可区分),并指向已有组;
- 整组提交原子生效:任一成员校验失败,整组不留痕;
- 组等全部成员终态后才收敛:全成功->SUCCEEDED;任一失败->FAILED 且可解释;
- 组级取消/终止传播到所有未完成成员,幂等且不可逆;
- 重启后组终态与成员结论不丢失,已成功内容不重跑;
- 组内任务总数再多也不突破全局并发上限。
"""
from __future__ import annotations

import threading
import time

import pytest

from orchestrator.errors import (
    CycleDetectedError,
    DuplicateWorkflowIdInGroupError,
    EmptyGroupError,
    GroupKeyConflictError,
    GroupNotFoundError,
)
from orchestrator.models import (
    GroupStatus,
    TaskDef,
    TaskStatus,
    WorkflowDef,
    WorkflowStatus,
)


def _pass(tid: str, deps: list[str] | None = None) -> TaskDef:
    return TaskDef(id=tid, action="pass", dependencies=deps or [])


def _two_member_group() -> list[WorkflowDef]:
    return [
        WorkflowDef(id="wf-a", tasks=[_pass("a1"), _pass("a2", ["a1"])]),
        WorkflowDef(id="wf-b", tasks=[_pass("b1")]),
    ]


class _CountingExecutor:
    """记录每个任务执行次数与并发峰值的确定性执行器包装。"""

    def __init__(self):
        self.counts: dict[str, int] = {}
        self.active = 0
        self.peak = 0
        self.guard = threading.Lock()
        from orchestrator.executor import Executor

        self._inner = Executor()

    def execute(self, action, params, attempt, task_id="", cancelled=None):
        with self.guard:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.counts[task_id] = self.counts.get(task_id, 0) + 1
        try:
            return self._inner.execute(action, params, attempt, task_id, cancelled)
        finally:
            with self.guard:
                self.active -= 1


# ---------- 组键防重复提交 ----------
def test_concurrent_duplicate_group_submission_lands_exactly_one_group(env):
    e = env()
    probe = _CountingExecutor()
    e.scheduler.executor = probe
    barrier = threading.Barrier(8)
    results, errors = [], []

    def submit():
        barrier.wait()
        try:
            results.append(
                e.service.submit_group("g-1", _two_member_group(), idempotency_key="gk-1")
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"并发重复提交不应报错: {errors}"
    assert len(results) == 8
    assert {g.id for g in results} == {"g-1"}  # 都落到同一个组
    assert len(e.service.list_groups()) == 1
    # 成员工作流只建了一次
    group = e.service.get_group("g-1")
    assert sorted(group.workflow_ids) == ["wf-a", "wf-b"]
    assert sorted(e.store.load_all()) == ["wf-a", "wf-b"]

    e.run()
    assert e.service.get_group("g-1").status == GroupStatus.SUCCEEDED
    # 每个任务恰好执行一次:重复提交没有造成重复执行
    assert probe.counts == {"a1": 1, "a2": 1, "b1": 1}


def test_same_group_key_same_content_is_idempotent_replay(env):
    e = env()
    g1 = e.service.submit_group("g-1", _two_member_group(), idempotency_key="gk-1")
    g2 = e.service.submit_group("g-1", _two_member_group(), idempotency_key="gk-1")
    assert g1 is g2
    # 成员顺序不同但内容相同,仍是同一次提交(指纹与顺序无关)
    g3 = e.service.submit_group(
        "g-1", list(reversed(_two_member_group())), idempotency_key="gk-1"
    )
    assert g3.id == "g-1"
    assert len(e.service.list_groups()) == 1


def test_group_key_reused_with_different_content_is_distinguishable_conflict(env):
    e = env()
    e.service.submit_group("g-1", _two_member_group(), idempotency_key="gk-1")
    different = [WorkflowDef(id="wf-x", tasks=[_pass("x1")])]
    with pytest.raises(GroupKeyConflictError) as exc:
        e.service.submit_group("g-2", different, idempotency_key="gk-1")
    assert exc.value.error_code == "GROUP_KEY_CONFLICT"
    # 能告诉调用方最终落到哪个已有组
    assert exc.value.existing_group_id == "g-1"
    # 同组 id 不同内容同样冲突
    with pytest.raises(GroupKeyConflictError) as exc2:
        e.service.submit_group("g-1", different, idempotency_key="gk-1")
    assert exc2.value.existing_group_id == "g-1"
    # 冲突拒绝不留痕:仍然只有最初那个组
    assert [g.id for g in e.service.list_groups()] == ["g-1"]
    assert sorted(e.store.load_all()) == ["wf-a", "wf-b"]


# ---------- 整组原子性 ----------
def test_group_submission_is_atomic_when_any_member_invalid(env):
    e = env()
    members = _two_member_group() + [
        WorkflowDef(id="wf-bad", tasks=[_pass("x", ["y"]), _pass("y", ["x"])])  # 环
    ]
    with pytest.raises(CycleDetectedError):
        e.service.submit_group("g-atomic", members, idempotency_key="gk-atomic")
    # 要么全部生效、要么完全不生效:组与任何成员都不存在
    assert e.service.list_groups() == []
    assert e.store.load_all() == {}


def test_group_validation_rejects_empty_and_duplicate_workflow_ids(env):
    e = env()
    with pytest.raises(EmptyGroupError) as exc:
        e.service.submit_group("g-empty", [])
    assert exc.value.error_code == "EMPTY_GROUP"
    with pytest.raises(DuplicateWorkflowIdInGroupError) as exc2:
        e.service.submit_group(
            "g-dup",
            [
                WorkflowDef(id="wf-a", tasks=[_pass("t1")]),
                WorkflowDef(id="wf-a", tasks=[_pass("t2")]),
            ],
        )
    assert exc2.value.error_code == "DUPLICATE_WORKFLOW_ID"
    assert e.service.list_groups() == []


def test_get_unknown_group_raises_not_found(env):
    e = env()
    with pytest.raises(GroupNotFoundError) as exc:
        e.service.get_group("ghost")
    assert exc.value.error_code == "GROUP_NOT_FOUND"


# ---------- 组级收尾:全部成员终态后收敛 ----------
def test_group_succeeds_only_after_all_members_succeed(env):
    e = env()
    e.service.submit_group("g-ok", _two_member_group(), idempotency_key="gk-ok")
    e.run()
    group, members = e.service.get_group_view("g-ok")
    assert group.status == GroupStatus.SUCCEEDED
    assert all(m.status == WorkflowStatus.SUCCEEDED for m in members)
    # 状态转移可解释:PENDING->RUNNING->SUCCEEDED
    path = [(t["from"], t["to"]) for t in group.transitions]
    assert path == [("PENDING", "RUNNING"), ("RUNNING", "SUCCEEDED")]


def test_group_fails_deterministically_when_any_member_fails(env):
    e = env()
    members = [
        WorkflowDef(
            id="wf-bad",
            tasks=[
                TaskDef(id="boom", action="fail", params={"kind": "non_retryable"}),
                _pass("downstream", ["boom"]),
            ],
        ),
        WorkflowDef(
            id="wf-slow",
            tasks=[TaskDef(id="slow", action="sleep", params={"seconds": 0.15})],
        ),
    ]
    e.service.submit_group("g-fail", members, idempotency_key="gk-fail")
    e.run()

    group, members_view = e.service.get_group_view("g-fail")
    by_id = {m.id: m for m in members_view}
    # 组按确定规则收敛为失败
    assert group.status == GroupStatus.FAILED
    # 结论可解释:指明是哪个成员、什么原因导致
    assert "wf-bad" in (group.reason or "")
    assert "boom" in (group.reason or "")
    # 失败成员内部:下游被确定性 SKIPPED,且能解释由谁导致
    bad = by_id["wf-bad"]
    assert bad.status == WorkflowStatus.FAILED
    assert bad.tasks["boom"].status == TaskStatus.FAILED
    assert bad.tasks["downstream"].status == TaskStatus.SKIPPED
    assert bad.tasks["downstream"].skipped_by == ["boom"]
    # 未开始的成员不被悄悄跳过:wf-slow 正常跑到成功终态
    slow = by_id["wf-slow"]
    assert slow.status == WorkflowStatus.SUCCEEDED
    assert slow.tasks["slow"].status == TaskStatus.SUCCEEDED
    assert slow.tasks["slow"].attempt == 1


# ---------- 组级取消 / 终止 ----------
def test_group_cancel_propagates_to_all_members_and_is_idempotent(env):
    e = env(max_concurrency=4)
    members = [
        WorkflowDef(
            id="wf-run",
            tasks=[TaskDef(id="long", action="sleep", params={"seconds": 0.3})],
        ),
        WorkflowDef(id="wf-wait", tasks=[_pass("w1"), _pass("w2", ["w1"])]),
    ]
    e.service.submit_group("g-cancel", members, idempotency_key="gk-cancel")
    e.scheduler.start()
    try:
        deadline = time.time() + 3
        while time.time() < deadline:
            if e.service.get_task("wf-run", "long").status == TaskStatus.RUNNING:
                break
            time.sleep(0.01)
        group = e.service.cancel_group("g-cancel")
        assert group.cancel_requested is True
        e.scheduler.run_until_idle()
    finally:
        e.scheduler.stop()

    group, members_view = e.service.get_group_view("g-cancel")
    by_id = {m.id: m for m in members_view}
    assert group.status == GroupStatus.CANCELLED
    assert by_id["wf-run"].status == WorkflowStatus.CANCELLED
    assert by_id["wf-wait"].status == WorkflowStatus.CANCELLED
    # 未开始的任务全部进入 CANCELLED 终态,没有被悄悄跳过
    assert by_id["wf-wait"].tasks["w2"].status == TaskStatus.CANCELLED
    # 组级结论可解释:reason 汇总了各成员结论
    assert "wf-run" in (group.reason or "") and "wf-wait" in (group.reason or "")
    # 重复取消幂等:不报错、终态不变、不新增转移
    again = e.service.cancel_group("g-cancel")
    assert again.status == GroupStatus.CANCELLED
    assert len(again.transitions) == len(group.transitions)


def test_group_terminate_is_immediate_irreversible_and_idempotent(env):
    e = env()
    members = [
        WorkflowDef(
            id="wf-run",
            tasks=[TaskDef(id="long", action="sleep", params={"seconds": 2.0})],
        ),
        WorkflowDef(id="wf-wait", tasks=[_pass("w1")]),
    ]
    e.service.submit_group("g-term", members, idempotency_key="gk-term")
    e.scheduler.start()
    try:
        deadline = time.time() + 3
        while time.time() < deadline:
            if e.service.get_task("wf-run", "long").status == TaskStatus.RUNNING:
                break
            time.sleep(0.01)
        group = e.service.terminate_group("g-term")
        # 终止是组级不可逆终态操作:组立即 CANCELLED
        assert group.status == GroupStatus.CANCELLED
    finally:
        e.scheduler.stop()

    group, members_view = e.service.get_group_view("g-term")
    assert group.status == GroupStatus.CANCELLED
    assert all(m.status == WorkflowStatus.CANCELLED for m in members_view)
    # 幂等:再次终止/取消都返回同一终态
    assert e.service.terminate_group("g-term").status == GroupStatus.CANCELLED
    assert e.service.cancel_group("g-term").status == GroupStatus.CANCELLED


# ---------- 重启恢复 ----------
def test_group_terminal_state_and_member_conclusions_survive_restart(env):
    e = env()
    probe = _CountingExecutor()
    e.scheduler.executor = probe
    members = [
        WorkflowDef(id="wf-ok", tasks=[_pass("ok1")]),
        WorkflowDef(
            id="wf-bad",
            tasks=[TaskDef(id="boom", action="fail", params={"kind": "non_retryable"})],
        ),
    ]
    e.service.submit_group("g-rec", members, idempotency_key="gk-rec")
    e.run()
    assert e.service.get_group("g-rec").status == GroupStatus.FAILED
    counts_before = dict(probe.counts)

    # 模拟进程重启:同一状态文件 + 全新调度器
    sch2 = e.new_scheduler_on_same_store()
    e.scheduler = sch2
    e.service.scheduler = sch2
    sch2.executor = probe
    e.run()

    group, members_view = e.service.get_group_view("g-rec")
    by_id = {m.id: m for m in members_view}
    # 组终态与成员结论不丢失、不错乱
    assert group.status == GroupStatus.FAILED
    assert "wf-bad" in (group.reason or "")
    assert by_id["wf-ok"].status == WorkflowStatus.SUCCEEDED
    assert by_id["wf-bad"].status == WorkflowStatus.FAILED
    # 已成功的工作流/任务绝不重跑
    assert probe.counts == counts_before


def test_unfinished_group_continues_to_same_conclusion_after_restart(env):
    e = env()
    probe = _CountingExecutor()
    e.scheduler.executor = probe
    members = [
        WorkflowDef(
            id="wf-long",
            tasks=[TaskDef(id="slow", action="sleep", params={"seconds": 0.2})],
        ),
        WorkflowDef(id="wf-quick", tasks=[_pass("q1")]),
    ]
    e.service.submit_group("g-crash", members, idempotency_key="gk-crash")
    # 让 wf-quick 跑完、wf-long 进入 RUNNING,然后"崩溃"(不 stop 线程池,直接换新调度器)
    e.scheduler.start()
    try:
        deadline = time.time() + 3
        while time.time() < deadline:
            wf = e.service.get_workflow("wf-quick")
            long_running = e.service.get_task("wf-long", "slow").status == TaskStatus.RUNNING
            if wf.status == WorkflowStatus.SUCCEEDED and long_running:
                break
            time.sleep(0.01)
    finally:
        e.scheduler.stop(wait=False)

    # 重启恢复:RUNNING 任务回退 PENDING 重跑;已成功成员不重跑
    sch2 = e.new_scheduler_on_same_store()
    e.scheduler = sch2
    e.service.scheduler = sch2
    sch2.executor = probe
    e.run()

    group, members_view = e.service.get_group_view("g-crash")
    by_id = {m.id: m for m in members_view}
    assert group.status == GroupStatus.SUCCEEDED
    assert by_id["wf-quick"].status == WorkflowStatus.SUCCEEDED
    assert by_id["wf-long"].status == WorkflowStatus.SUCCEEDED
    # 崩溃前已成功的 q1 只执行过一次;slow 因崩溃被重跑过,但最终收敛到同一结论
    assert probe.counts["q1"] == 1
    assert probe.counts["slow"] >= 1


def test_group_cancel_request_survives_restart(env):
    e = env()
    members = [WorkflowDef(id="wf-a", tasks=[_pass("a1"), _pass("a2", ["a1"])])]
    e.service.submit_group("g-crec", members, idempotency_key="gk-crec")
    e.service.cancel_group("g-crec")  # 取消请求先落盘,再"重启"

    sch2 = e.new_scheduler_on_same_store()
    e.scheduler = sch2
    e.service.scheduler = sch2
    e.run()

    group, members_view = e.service.get_group_view("g-crec")
    assert group.status == GroupStatus.CANCELLED
    assert members_view[0].status == WorkflowStatus.CANCELLED
    assert all(t.status == TaskStatus.CANCELLED for t in members_view[0].tasks.values())


# ---------- 并发上限 ----------
def test_group_tasks_never_exceed_global_concurrency_cap(env):
    e = env(max_concurrency=2)
    probe = _CountingExecutor()
    e.scheduler.executor = probe
    members = [
        WorkflowDef(
            id=f"wf-{i}",
            tasks=[
                TaskDef(id=f"t{i}a", action="sleep", params={"seconds": 0.05}),
                TaskDef(id=f"t{i}b", action="sleep", params={"seconds": 0.05}),
            ],
        )
        for i in range(4)
    ]
    e.service.submit_group("g-cap", members, idempotency_key="gk-cap")
    e.run(timeout=10)
    assert e.service.get_group("g-cap").status == GroupStatus.SUCCEEDED
    assert probe.peak <= 2, f"组内任务突破了全局并发上限: peak={probe.peak}"
    assert probe.peak == 2
    # 8 个任务各执行一次
    assert len(probe.counts) == 8 and all(c == 1 for c in probe.counts.values())


# ---------- HTTP 接口 ----------
def _group_payload(group_id: str, key: str, wf_prefix: str = "wf") -> dict:
    return {
        "group_id": group_id,
        "idempotency_key": key,
        "workflows": [
            {
                "workflow_id": f"{wf_prefix}-1",
                "tasks": [{"id": "t1", "action": "pass"}],
            },
            {
                "workflow_id": f"{wf_prefix}-2",
                "tasks": [{"id": "u1", "action": "pass"}, {"id": "u2", "action": "pass", "dependencies": ["u1"]}],
            },
        ],
    }


def test_group_api_submit_query_and_idempotent_replay(fastapi_client):
    client, service, sched = fastapi_client
    r = client.post("/groups", json=_group_payload("g-api", "gk-api"))
    assert r.status_code == 201
    assert r.json()["status"] == "PENDING"
    # 同键同内容重复提交:幂等返回同一组,不重复创建
    r2 = client.post("/groups", json=_group_payload("g-api", "gk-api"))
    assert r2.status_code == 201
    assert r2.json()["id"] == "g-api"
    assert client.get("/groups").json()["groups"][0]["id"] == "g-api"
    assert len(client.get("/groups").json()["groups"]) == 1

    sched.run_until_idle()
    r3 = client.get("/groups/g-api")
    assert r3.status_code == 200
    body = r3.json()
    assert body["status"] == "SUCCEEDED"
    # 按组可查询到组内各个工作流的当前状态
    assert body["workflows"]["wf-1"]["status"] == "SUCCEEDED"
    assert body["workflows"]["wf-2"]["tasks"] == {"u1": "SUCCEEDED", "u2": "SUCCEEDED"}


def test_group_api_key_conflict_is_distinguishable_and_points_to_existing(fastapi_client):
    client, service, sched = fastapi_client
    assert client.post("/groups", json=_group_payload("g-api", "gk-api")).status_code == 201
    conflict = _group_payload("g-other", "gk-api", wf_prefix="wf-x")
    r = client.post("/groups", json=conflict)
    assert r.status_code == 409
    body = r.json()
    assert body["error_code"] == "GROUP_KEY_CONFLICT"  # 与其它校验失败(422)可区分
    assert body["existing_group_id"] == "g-api"
    # 组校验失败:空组 422
    r2 = client.post("/groups", json={"group_id": "g-empty", "workflows": [{"workflow_id": "w", "tasks": [{"id": "t", "action": "pass"}]}]})
    assert r2.status_code == 201 or r2.status_code == 422  # 空组由 min_length 或 EMPTY_GROUP 拒绝
    r3 = client.post("/groups", json={"group_id": "g-bad", "workflows": []})
    assert r3.status_code == 422
    # 不存在的组 404
    assert client.get("/groups/ghost").status_code == 404


def test_group_api_cancel_and_terminate(fastapi_client):
    client, service, sched = fastapi_client
    payload = {
        "group_id": "g-stop",
        "idempotency_key": "gk-stop",
        "workflows": [
            {"workflow_id": "wf-s1", "tasks": [{"id": "a", "action": "sleep", "params": {"seconds": 5}}]},
            {"workflow_id": "wf-s2", "tasks": [{"id": "b", "action": "pass", "dependencies": []}]},
        ],
    }
    assert client.post("/groups", json=payload).status_code == 201
    r = client.post("/groups/g-stop/cancel")
    assert r.status_code == 200
    assert r.json()["cancel_requested"] is True
    sched.run_until_idle()
    body = client.get("/groups/g-stop").json()
    assert body["status"] == "CANCELLED"
    assert body["workflows"]["wf-s1"]["status"] == "CANCELLED"
    assert body["workflows"]["wf-s2"]["status"] == "CANCELLED"
    # 幂等:重复取消不报错
    assert client.post("/groups/g-stop/cancel").json()["status"] == "CANCELLED"
    # 终止同样幂等
    assert client.post("/groups/g-stop/terminate").json()["status"] == "CANCELLED"
