"""场景五:HTTP 端到端。错误原因映射为可区分的状态码;状态查询含转移记录。"""
from __future__ import annotations

import time


def _wf_body(wf_id="wf-1", tasks=None, key=None):
    body = {"workflow_id": wf_id, "tasks": tasks if tasks is not None else [{"id": "a", "action": "pass"}]}
    if key is not None:
        body["idempotency_key"] = key
    return body


def _wait_status(client, wf_id, expected, timeout=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = client.get(f"/workflows/{wf_id}").json()
        if data["status"] == expected:
            return data
        time.sleep(0.02)
    raise AssertionError(f"workflow did not reach {expected}, last={data['status']}")


def test_health(fastapi_client):
    client, _, _ = fastapi_client
    assert client.get("/health").json()["status"] == "ok"


def test_submit_and_query_success_workflow(fastapi_client):
    client, _, _ = fastapi_client
    body = _wf_body(
        tasks=[
            {"id": "a", "action": "pass"},
            {"id": "b", "action": "pass", "dependencies": ["a"]},
        ]
    )
    r = client.post("/workflows", json=body)
    assert r.status_code == 201
    assert r.json()["status"] in ("PENDING", "RUNNING")  # 提交后可能已被后台调度
    data = _wait_status(client, "wf-1", "SUCCEEDED")
    assert data["tasks"]["b"]["status"] == "SUCCEEDED"
    assert data["tasks"]["b"]["transitions"][-1]["to"] == "SUCCEEDED"


def test_cycle_returns_422_with_cycle_detail(fastapi_client):
    client, _, _ = fastapi_client
    body = _wf_body(
        tasks=[
            {"id": "a", "action": "pass", "dependencies": ["b"]},
            {"id": "b", "action": "pass", "dependencies": ["a"]},
        ]
    )
    r = client.post("/workflows", json=body)
    assert r.status_code == 422
    err = r.json()
    assert err["error_code"] == "CYCLE_DETECTED"
    assert set(err["cycle"]) == {"a", "b"}


def test_missing_dependency_returns_422_distinguishable(fastapi_client):
    client, _, _ = fastapi_client
    body = _wf_body(tasks=[{"id": "a", "action": "pass", "dependencies": ["ghost"]}])
    r = client.post("/workflows", json=body)
    assert r.status_code == 422
    err = r.json()
    assert err["error_code"] == "MISSING_DEPENDENCY"
    assert err["missing_dependency"] == "ghost"


def test_idempotency_conflict_returns_409(fastapi_client):
    client, _, _ = fastapi_client
    body = _wf_body(wf_id="wf-a", tasks=[{"id": "a", "action": "pass"}], key="k1")
    assert client.post("/workflows", json=body).status_code == 201
    body2 = _wf_body(wf_id="wf-b", tasks=[{"id": "b", "action": "pass"}], key="k1")
    r = client.post("/workflows", json=body2)
    assert r.status_code == 409
    err = r.json()
    assert err["error_code"] == "IDEMPOTENCY_KEY_CONFLICT"
    assert err["existing_workflow_id"] == "wf-a"


def test_duplicate_submit_same_key_same_id_is_idempotent(fastapi_client):
    client, _, _ = fastapi_client
    body = _wf_body(wf_id="wf-1", tasks=[{"id": "a", "action": "pass"}], key="dup")
    # 两次提交之间任务可能已被后台推进,因此只断言:都被接受、终态一致、只执行一次
    assert client.post("/workflows", json=body).status_code == 201
    assert client.post("/workflows", json=body).status_code == 201
    data = _wait_status(client, "wf-1", "SUCCEEDED")
    # 完成后再重复提交,拿到的仍是同一个已成功工作流
    r3 = client.post("/workflows", json=body)
    assert r3.status_code == 201
    assert r3.json()["status"] == "SUCCEEDED"
    assert [x["to"] for x in data["tasks"]["a"]["transitions"]].count("RUNNING") == 1
    assert data["tasks"]["a"]["attempt"] == 1


def test_workflow_not_found_404(fastapi_client):
    client, _, _ = fastapi_client
    assert client.get("/workflows/nope").status_code == 404


def test_terminate_endpoint_is_terminal_and_idempotent(fastapi_client):
    client, _, _ = fastapi_client
    body = _wf_body(
        tasks=[
            {"id": "a", "action": "sleep", "params": {"seconds": 5}},
            {"id": "b", "action": "pass", "dependencies": ["a"]},
        ]
    )
    client.post("/workflows", json=body)
    # 等 a 进入 RUNNING 后终止
    deadline = time.time() + 4
    while time.time() < deadline:
        t = client.get("/workflows/wf-1/tasks/a").json()
        if t["status"] == "RUNNING":
            break
        time.sleep(0.02)
    assert t["status"] == "RUNNING"
    r = client.post("/workflows/wf-1/terminate")
    assert r.status_code == 200
    assert r.json()["status"] == "CANCELLED"
    # 终态不可逆:再次终止仍返回 CANCELLED
    assert client.post("/workflows/wf-1/terminate").json()["status"] == "CANCELLED"
    data = client.get("/workflows/wf-1").json()
    assert data["tasks"]["b"]["status"] == "CANCELLED"


# ================= 工作流组 HTTP 端到端 =================
def _group_body(group_id="g-1", key="gk-1", workflows=None):
    return {
        "group_id": group_id,
        "group_key": key,
        "workflows": workflows
        or [
            {"workflow_id": "w1", "tasks": [{"id": "a", "action": "pass"}]},
            {"workflow_id": "w2", "tasks": [{"id": "b", "action": "pass"}]},
        ],
    }


def _wait_group_status(client, group_id, expected, timeout=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = client.get(f"/groups/{group_id}").json()
        if data["status"] == expected:
            return data
        time.sleep(0.02)
    raise AssertionError(f"group did not reach {expected}, last={data['status']}")


def test_group_submit_query_and_member_statuses(fastapi_client):
    client, _, _ = fastapi_client
    r = client.post("/groups", json=_group_body())
    assert r.status_code == 201
    assert r.json()["workflow_ids"] == ["w1", "w2"]
    data = _wait_group_status(client, "g-1", "SUCCEEDED")
    # 按组查询到组内各工作流当前状态
    assert data["members"]["w1"]["status"] == "SUCCEEDED"
    assert data["members"]["w2"]["status"] == "SUCCEEDED"
    assert data["transitions"][-1]["to"] == "SUCCEEDED"
    listing = client.get("/groups").json()
    assert listing["groups"] == [{"id": "g-1", "status": "SUCCEEDED"}]


def test_group_key_conflict_returns_409_with_existing_group(fastapi_client):
    client, _, _ = fastapi_client
    assert client.post("/groups", json=_group_body()).status_code == 201
    conflict = _group_body(
        group_id="g-2",
        workflows=[{"workflow_id": "w9", "tasks": [{"id": "x", "action": "pass"}]}],
    )
    r = client.post("/groups", json=conflict)
    assert r.status_code == 409
    err = r.json()
    assert err["error_code"] == "GROUP_KEY_CONFLICT"
    assert err["existing_group_id"] == "g-1"
    # 幂等重放(同键同内容)不是冲突:返回已落地的组
    replay = _group_body(group_id="g-3")
    r2 = client.post("/groups", json=replay)
    assert r2.status_code == 201
    assert r2.json()["id"] == "g-1"


def test_group_validation_error_distinguishable_from_key_conflict(fastapi_client):
    client, _, _ = fastapi_client
    bad = _group_body(
        workflows=[
            {
                "workflow_id": "w1",
                "tasks": [
                    {"id": "a", "action": "pass", "dependencies": ["b"]},
                    {"id": "b", "action": "pass", "dependencies": ["a"]},
                ],
            }
        ]
    )
    r = client.post("/groups", json=bad)
    assert r.status_code == 422  # 校验失败:422,与 409 组键冲突可区分
    assert r.json()["error_code"] == "CYCLE_DETECTED"
    assert client.get("/groups").json()["groups"] == []  # 全有或全无:无任何落地


def test_group_not_found_and_terminal_cancel(fastapi_client):
    client, _, _ = fastapi_client
    r = client.get("/groups/ghost")
    assert r.status_code == 404
    assert r.json()["error_code"] == "GROUP_NOT_FOUND"
    client.post("/groups", json=_group_body())
    _wait_group_status(client, "g-1", "SUCCEEDED")
    r = client.post("/groups/g-1/cancel")
    assert r.status_code == 409  # 已终态(非取消)的组拒绝取消
    assert r.json()["error_code"] == "TERMINAL_STATE"


def test_group_terminate_is_immediate_and_idempotent(fastapi_client):
    client, _, _ = fastapi_client
    body = _group_body(
        workflows=[{"workflow_id": "w1", "tasks": [{"id": "s", "action": "sleep", "params": {"seconds": 1.0}}]}]
    )
    client.post("/groups", json=body)
    time.sleep(0.1)
    r = client.post("/groups/g-1/terminate")
    assert r.status_code == 200
    assert r.json()["status"] == "CANCELLED"  # 组立即终态
    r2 = client.post("/groups/g-1/terminate")  # 重复终止幂等
    assert r2.status_code == 200
    assert r2.json()["status"] == "CANCELLED"
    data = client.get("/groups/g-1").json()
    assert data["members"]["w1"]["status"] == "CANCELLED"
