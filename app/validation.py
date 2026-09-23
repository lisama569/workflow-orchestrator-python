"""工作流提交校验：任务名唯一、依赖存在、依赖图无环。

失败原因按固定顺序判定，保证同一非法定义得到确定且可区分的结论：
    1. DUPLICATE_TASK_NAME   工作流内任务名重复
    2. UNKNOWN_DEPENDENCY    依赖指向不存在的任务
    3. CYCLIC_DEPENDENCY     依赖图存在环（Kahn 拓扑判定）
"""
from __future__ import annotations

from collections import deque

from app.errors import SubmissionError
from app.models import FailureReason, WorkflowDef


def validate_workflow(definition: WorkflowDef) -> None:
    names = [task.name for task in definition.tasks]
    if len(names) != len(set(names)):
        dup = sorted({n for n in names if names.count(n) > 1})
        raise SubmissionError(
            FailureReason.DUPLICATE_TASK_NAME,
            f"duplicate task names: {','.join(dup)}",
        )

    name_set = set(names)
    for task in definition.tasks:
        for dep in task.deps:
            if dep not in name_set:
                raise SubmissionError(
                    FailureReason.UNKNOWN_DEPENDENCY,
                    f"task '{task.name}' depends on unknown task '{dep}'",
                )
            if dep == task.name:
                raise SubmissionError(
                    FailureReason.CYCLIC_DEPENDENCY,
                    f"task '{task.name}' depends on itself",
                )

    _ensure_acyclic({task.name: list(task.deps) for task in definition.tasks})


def _ensure_acyclic(graph: dict[str, list[str]]) -> None:
    """Kahn 算法：能完成拓扑排序则无环；剩余节点全部位于环上。"""
    # 边的方向是 dep -> name，name 的入度 = 其依赖数
    indegree = {name: len(deps) for name, deps in graph.items()}
    dependents: dict[str, list[str]] = {name: [] for name in graph}
    for name, deps in graph.items():
        for dep in deps:
            dependents[dep].append(name)

    queue = deque(sorted(name for name, deg in indegree.items() if deg == 0))
    resolved = 0
    while queue:
        name = queue.popleft()
        resolved += 1
        for child in dependents[name]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if resolved != len(graph):
        in_cycle = sorted(
            name for name, deg in indegree.items() if deg > 0
        )
        raise SubmissionError(
            FailureReason.CYCLIC_DEPENDENCY,
            f"dependency cycle detected involving: {','.join(in_cycle)}",
        )
