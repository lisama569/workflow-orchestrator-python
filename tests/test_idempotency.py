"""幂等：相同幂等键重复提交绝不产生重复执行；键复用且定义不同必须冲突拒绝。"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from app.errors import WorkflowError
from app.models import (
    FailureReason,
    RetryPolicy,
    TaskDef,
    WorkflowDef,
)

log = logging.getLogger("orchestrator")


def _def(key: str = "idem-1", fail_times: int | None = None,
         extra: bool = False, conc: int = 4) -> WorkflowDef:
    payload = {} if fail_times is None else {"fail_times": fail_times}
    tasks = [
        TaskDef(
            name="t",
            handler="flaky" if fail_times is not None else "success",
            payload=payload,
            retry_policy=RetryPolicy(
                max_retries=3, base_delay=0.01, backoff_factor=1
            ),
        )
    ]
    if extra:
        tasks.append(TaskDef(name="t2", handler="success"))
    return WorkflowDef(idempotency_key=key, tasks=tasks, max_concurrency=conc)


def _print_trace(wf) -> None:
    log.info("工作流 %s (key=%s) 事件轨迹:", wf.id, wf.idempotency_key)
    for ev in wf.events:
        log.info("  #%s %-4s %-9s -> %-9s | %s | %s",
                 ev.seq, ev.task or "-",
                 ev.from_state or "-", ev.to_state, ev.reason, ev.detail)


def test_duplicate_submit_same_definition_no_second_execution(harness):
    scheduler = harness.scheduler
    service = harness.service

    wf1 = service.submit(_def())
    # 完成后再次以同一幂等键提交：返回同一工作流，任务不重跑
    final = scheduler.wait_for_terminal(wf1.id, timeout=5)
    assert final.state.value == "SUCCEEDED"
    attempts_after_first = final.tasks["t"].attempts

    wf2 = service.submit(_def())
    assert wf2.id == wf1.id, "相同幂等键必须返回同一工作流"
    assert wf2.tasks["t"].state.value == "SUCCEEDED"
    assert wf2.tasks["t"].attempts == attempts_after_first == 1
    _print_trace(wf2)


def test_concurrent_duplicate_submit_creates_single_workflow(harness):
    service = harness.service
    definition = _def(key="idem-concurrent")

    ids: set[str] = set()
    errors = []

    def do_submit():
        try:
            ids.add(service.submit(definition).id)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: do_submit(), range(8)))

    assert not errors, errors
    assert ids == {next(iter(ids))}
    assert len(service.list_workflows()) == 1
    wf = harness.scheduler.wait_for_terminal(next(iter(ids)), timeout=5)
    assert wf.tasks["t"].attempts == 1, "并发重复提交不得产生重复执行"


def test_idempotency_key_reuse_with_different_payload_conflicts(harness):
    service = harness.service
    wf = service.submit(_def(key="idem-conf", fail_times=0))
    harness.scheduler.wait_for_terminal(wf.id, timeout=5)

    # 同键、不同任务集合 -> IDEMPOTENCY_CONFLICT
    try:
        service.submit(_def(key="idem-conf", fail_times=0, extra=True))
        raise AssertionError("expected IDEMPOTENCY_CONFLICT")
    except WorkflowError as exc:
        assert exc.reason is FailureReason.IDEMPOTENCY_CONFLICT
        assert exc.status_code == 409
        log.info("冲突判定: %s -> %s", exc.reason, exc)

    # 同键、同任务但不同 max_concurrency -> 同样冲突
    try:
        service.submit(_def(key="idem-conf", fail_times=0, conc=2))
        raise AssertionError("expected IDEMPOTENCY_CONFLICT")
    except WorkflowError as exc:
        assert exc.reason is FailureReason.IDEMPOTENCY_CONFLICT


def test_duplicate_submit_while_running_returns_same(harness):
    from app.handlers.builtin import get_gate

    # gate 任务会阻塞在执行中，期间重复提交必须直接返回同一个 RUNNING 工作流
    gate = get_gate("g1")
    gate.reset()
    definition = WorkflowDef(
        idempotency_key="idem-running",
        tasks=[TaskDef(name="g", handler="gate", payload={"gate": "g1"})],
    )
    wf1 = harness.service.submit(definition)
    import time

    time.sleep(0.1)
    wf2 = harness.service.submit(definition)
    assert wf2.id == wf1.id
    assert wf2.tasks["g"].state.value == "RUNNING"
    assert wf2.tasks["g"].attempts == 1
    gate.release()
    final = harness.scheduler.wait_for_terminal(wf1.id, timeout=5)
    assert final.state.value == "SUCCEEDED"
    assert final.tasks["g"].attempts == 1
