"""HTTP API 集成：提交、可区分错误码、查询、取消终态不可逆。"""
from __future__ import annotations

import logging
import time

from fastapi.testclient import TestClient

from app.api import create_app
from app.handlers.builtin import get_gate

log = logging.getLogger("orchestrator")


def _client(harness):
    app = create_app(harness.service, autostart=False)
    return TestClient(app)


def test_submit_and_query_flow(harness):
    client = _client(harness)
    payload = {
        "idempotency_key": "api-1",
        "max_concurrency": 2,
        "tasks": [
            {"name": "a", "handler": "success"},
            {"name": "b", "handler": "success", "deps": ["a"]},
        ],
    }
    r = client.post("/workflows", json=payload)
    assert r.status_code == 201
    wf_id = r.json()["id"]

    # 等待完成
    deadline = time.time() + 5
    while time.time() < deadline:
        wf = client.get(f"/workflows/{wf_id}").json()
        if wf["state"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.02)
    assert wf["state"] == "SUCCEEDED"

    # 幂等查询
    r2 = client.get("/workflows/idempotency/api-1")
    assert r2.status_code == 200
    assert r2.json()["id"] == wf_id

    # 重复提交不重复执行
    r3 = client.post("/workflows", json=payload)
    assert r3.status_code == 201
    assert r3.json()["id"] == wf_id


def test_distinguishable_error_codes(harness):
    client = _client(harness)

    # 环
    r = client.post("/workflows", json={
        "idempotency_key": "bad-cycle",
        "tasks": [
            {"name": "a", "handler": "success", "deps": ["b"]},
            {"name": "b", "handler": "success", "deps": ["a"]},
        ],
    })
    assert r.status_code == 400
    assert r.json()["error"] == "CYCLIC_DEPENDENCY"

    # 未知依赖
    r = client.post("/workflows", json={
        "idempotency_key": "bad-dep",
        "tasks": [{"name": "a", "handler": "success", "deps": ["nope"]}],
    })
    assert r.status_code == 400
    assert r.json()["error"] == "UNKNOWN_DEPENDENCY"

    # 幂等冲突
    ok = {"idempotency_key": "k", "tasks": [{"name": "a", "handler": "success"}]}
    client.post("/workflows", json=ok)
    r = client.post("/workflows", json={
        "idempotency_key": "k",
        "tasks": [{"name": "a", "handler": "fail_always"}],
    })
    assert r.status_code == 409
    assert r.json()["error"] == "IDEMPOTENCY_CONFLICT"

    # 不存在
    assert client.get("/workflows/wf-missing").status_code == 404
    assert client.get("/workflows/idempotency/nope").status_code == 404
    log.info("可区分错误码校验通过: 400/409/404 均带稳定 error 字段")


def test_cancel_workflow_terminal_irreversible(harness):
    gate = get_gate("api-gate")
    gate.reset()
    client = _client(harness)
    r = client.post("/workflows", json={
        "idempotency_key": "api-cancel",
        "tasks": [
            {"name": "x", "handler": "gate", "payload": {"gate": "api-gate"}},
            {"name": "y", "handler": "success", "deps": ["x"]},
        ],
    })
    wf_id = r.json()["id"]
    time.sleep(0.15)

    r = client.post(f"/workflows/{wf_id}/cancel")
    assert r.status_code == 200
    time.sleep(0.15)
    wf = client.get(f"/workflows/{wf_id}").json()
    assert wf["state"] == "CANCELLED"
    # 工作流级取消：RUNNING 的 x 协作式 CANCELLED；未开始的 y 直接 CANCELLED
    # （整体取消意图，区别于“因上游失败而 SKIPPED”）
    states = {t["name"]: t["state"] for t in wf["tasks"].values()}
    assert states == {"x": "CANCELLED", "y": "CANCELLED"}
    y_reason = wf["tasks"]["y"]["failure_reason"]
    assert y_reason == "WORKFLOW_CANCELLED"

    # 终态不可逆：再次取消必须 409/ALREADY_TERMINAL
    r = client.post(f"/workflows/{wf_id}/cancel")
    assert r.status_code == 409
    assert r.json()["error"] == "ALREADY_TERMINAL"

    # 释放闸门也不能让已取消的任务复活
    gate.release()
    time.sleep(0.15)
    wf = client.get(f"/workflows/{wf_id}").json()
    assert wf["state"] == "CANCELLED"
    assert wf["tasks"]["x"]["state"] == "CANCELLED"
