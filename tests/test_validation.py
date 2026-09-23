"""提交期校验：环 / 未知依赖 / 重名必须被拒绝且原因可区分。"""
from __future__ import annotations

import logging

import pytest

from app.errors import SubmissionError
from app.models import FailureReason, TaskDef, WorkflowDef

log = logging.getLogger("orchestrator")


def _print_events(wf) -> None:
    for ev in wf.events:
        log.info("event#%s task=%s %s->%s reason=%s %s",
                 ev.seq, ev.task, ev.from_state, ev.to_state, ev.reason, ev.detail)


def test_cyclic_dependency_rejected(harness):
    definition = WorkflowDef(
        idempotency_key="cycle-1",
        tasks=[
            TaskDef(name="a", handler="success", deps=["b"]),
            TaskDef(name="b", handler="success", deps=["a"]),
        ],
    )
    with pytest.raises(SubmissionError) as exc:
        harness.service.submit(definition)
    assert exc.value.reason is FailureReason.CYCLIC_DEPENDENCY
    log.info("判定依据: %s -> %s", exc.value.reason, exc.value)


def test_self_dependency_rejected(harness):
    definition = WorkflowDef(
        idempotency_key="cycle-self",
        tasks=[TaskDef(name="a", handler="success", deps=["a"])],
    )
    with pytest.raises(SubmissionError) as exc:
        harness.service.submit(definition)
    assert exc.value.reason is FailureReason.CYCLIC_DEPENDENCY


def test_unknown_dependency_rejected(harness):
    definition = WorkflowDef(
        idempotency_key="unk-1",
        tasks=[TaskDef(name="a", handler="success", deps=["ghost"])],
    )
    with pytest.raises(SubmissionError) as exc:
        harness.service.submit(definition)
    assert exc.value.reason is FailureReason.UNKNOWN_DEPENDENCY
    assert "ghost" in str(exc.value)


def test_duplicate_task_name_rejected(harness):
    definition = WorkflowDef(
        idempotency_key="dup-1",
        tasks=[
            TaskDef(name="a", handler="success"),
            TaskDef(name="a", handler="fail_always"),
        ],
    )
    with pytest.raises(SubmissionError) as exc:
        harness.service.submit(definition)
    assert exc.value.reason is FailureReason.DUPLICATE_TASK_NAME


def test_three_node_cycle_rejected(harness):
    definition = WorkflowDef(
        idempotency_key="cycle-3",
        tasks=[
            TaskDef(name="a", handler="success", deps=["c"]),
            TaskDef(name="b", handler="success", deps=["a"]),
            TaskDef(name="c", handler="success", deps=["b"]),
        ],
    )
    with pytest.raises(SubmissionError) as exc:
        harness.service.submit(definition)
    assert exc.value.reason is FailureReason.CYCLIC_DEPENDENCY
