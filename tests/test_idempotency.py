"""场景一:幂等重复提交不得产生重复执行;非法依赖图必须被拒绝且原因可区分。"""
from __future__ import annotations

import pytest

from orchestrator.errors import (
    CycleDetectedError,
    DuplicateIdempotencyKeyError,
    DuplicateTaskIdError,
    EmptyWorkflowError,
    MissingDependencyError,
)
from orchestrator.models import (
    RetryPolicy,
    TaskDef,
    TaskStatus,
    WorkflowStatus,
)
from orchestrator.service import OrchestratorService


def _pass(tid: str, deps: list[str] | None = None) -> TaskDef:
    return TaskDef(id=tid, action="pass", dependencies=deps or [])


# ---------- 幂等重复提交 ----------
def test_duplicate_submission_same_idempotency_key_returns_same_workflow(env):
    e = env()
    wf1 = e.service.submit_workflow("wf-1", [_pass("a")], idempotency_key="key-1")
    wf2 = e.service.submit_workflow("wf-1", [_pass("a")], idempotency_key="key-1")
    assert wf1 is wf2
    e.run()
    # 第三次重复提交发生在执行完成后:仍返回同一个已成功工作流
    wf3 = e.service.submit_workflow("wf-1", [_pass("a")], idempotency_key="key-1")
    assert wf3.status == WorkflowStatus.SUCCEEDED
    t = wf3.tasks["a"]
    assert t.status == TaskStatus.SUCCEEDED
    # 只执行过一次:只有一条 PENDING->RUNNING 和一条 RUNNING->SUCCEEDED
    assert [x["to"] for x in t.transitions].count("RUNNING") == 1
    assert t.attempt == 1


def test_same_idempotency_key_different_workflow_id_rejected(env):
    e = env()
    e.service.submit_workflow("wf-a", [_pass("a")], idempotency_key="shared-key")
    with pytest.raises(DuplicateIdempotencyKeyError) as exc:
        e.service.submit_workflow("wf-b", [_pass("b")], idempotency_key="shared-key")
    assert exc.value.error_code == "IDEMPOTENCY_KEY_CONFLICT"
    assert exc.value.existing_workflow_id == "wf-a"


def test_repeated_submission_without_key_same_wf_id_is_conflict(env):
    e = env()
    e.service.submit_workflow("wf-1", [_pass("a")])
    with pytest.raises(DuplicateIdempotencyKeyError):
        e.service.submit_workflow("wf-1", [_pass("a")])


# ---------- 校验:空 / 重复 id / 缺依赖 / 环 ----------
def test_empty_workflow_rejected(env):
    e = env()
    with pytest.raises(EmptyWorkflowError) as exc:
        e.service.submit_workflow("wf", [])
    assert exc.value.error_code == "EMPTY_WORKFLOW"
    assert e.store.load_all() == {}  # 被拒绝即无任何持久化/执行


def test_duplicate_task_id_rejected(env):
    e = env()
    with pytest.raises(DuplicateTaskIdError) as exc:
        OrchestratorService.validate([_pass("a"), _pass("a")])
    assert exc.value.error_code == "DUPLICATE_TASK_ID"
    assert exc.value.task_id == "a"


def test_missing_dependency_rejected_with_distinguishable_reason(env):
    e = env()
    with pytest.raises(MissingDependencyError) as exc:
        OrchestratorService.validate([TaskDef(id="a", action="pass", dependencies=["ghost"])])
    assert exc.value.error_code == "MISSING_DEPENDENCY"
    assert exc.value.task_id == "a"
    assert exc.value.missing_dependency == "ghost"


def test_self_dependency_is_a_cycle(env):
    e = env()
    with pytest.raises(CycleDetectedError) as exc:
        OrchestratorService.validate([TaskDef(id="a", action="pass", dependencies=["a"])])
    assert exc.value.error_code == "CYCLE_DETECTED"
    assert exc.value.cycle[0] == exc.value.cycle[-1] == "a"


def test_long_cycle_rejected_and_cycle_path_explained(env):
    e = env()
    tasks = [
        TaskDef(id="a", action="pass", dependencies=["c"]),
        TaskDef(id="b", action="pass", dependencies=["a"]),
        TaskDef(id="c", action="pass", dependencies=["b"]),
    ]
    with pytest.raises(CycleDetectedError) as exc:
        OrchestratorService.validate(tasks)
    assert exc.value.error_code == "CYCLE_DETECTED"
    cycle = exc.value.cycle
    assert cycle[0] == cycle[-1]  # 首尾相同,清晰指出环
    assert set(cycle) == {"a", "b", "c"}


def test_error_codes_are_distinguishable(env):
    """环与缺依赖必须给出不同错误码,调用方才能区分处理。"""
    assert MissingDependencyError("a", "b").error_code != CycleDetectedError(["a"]).error_code
