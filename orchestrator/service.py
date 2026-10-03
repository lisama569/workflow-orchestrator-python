"""门面服务:校验提交、幂等去重、查询、取消与终止。"""
from __future__ import annotations

import hashlib
import json
import logging
import time

from .errors import (
    CycleDetectedError,
    DuplicateIdempotencyKeyError,
    DuplicateTaskIdError,
    DuplicateWorkflowIdError,
    EmptyGroupError,
    EmptyWorkflowError,
    GroupKeyConflictError,
    GroupNotFoundError,
    MissingDependencyError,
    TaskNotFoundError,
    TerminalStateError,
    WorkflowIdConflictError,
    WorkflowNotFoundError,
)
from .models import (
    GroupStatus,
    GroupWorkflowDef,
    RetryPolicy,
    TaskDef,
    TaskState,
    TaskStatus,
    WorkflowGroupState,
    WorkflowState,
    WorkflowStatus,
)
from .scheduler import Scheduler
from .store import JsonStore

logger = logging.getLogger("orchestrator.service")


class OrchestratorService:
    """对外门面:所有状态变更经此入口,保证校验/幂等/持久化顺序确定。"""

    def __init__(self, store: JsonStore, scheduler: Scheduler) -> None:
        self.store = store
        self.scheduler = scheduler

    # ================= 提交 =================
    def submit_workflow(
        self,
        workflow_id: str,
        tasks: list[TaskDef],
        idempotency_key: str | None = None,
    ) -> WorkflowState:
        tasks = list(tasks or [])
        self.validate(tasks)

        existing_by_id = self.store.get(workflow_id)
        if existing_by_id is not None:
            # 同一工作流 id + 同一幂等键:视为重复提交,返回既有对象,不产生执行。
            if idempotency_key is not None and existing_by_id.idempotency_key == idempotency_key:
                logger.info(
                    "duplicate submission ignored: workflow=%s idempotency_key=%s -> existing workflow",
                    workflow_id,
                    idempotency_key,
                )
                return existing_by_id
            raise DuplicateIdempotencyKeyError(workflow_id)

        if idempotency_key is not None:
            for wf in self.store.load_all().values():
                if wf.idempotency_key == idempotency_key:
                    logger.info(
                        "duplicate submission ignored: idempotency_key=%s -> existing workflow=%s",
                        idempotency_key,
                        wf.id,
                    )
                    raise DuplicateIdempotencyKeyError(wf.id)

        now = time.time()
        wf = self.build_state(workflow_id, tasks, idempotency_key, now)
        self.store.save_workflow(wf)
        logger.info(
            "workflow=%s accepted: %d task(s), idempotency_key=%s",
            workflow_id,
            len(tasks),
            idempotency_key,
        )
        self.scheduler.notify()
        return wf

    # ================= 组提交 =================
    def submit_group(
        self,
        group_id: str,
        workflows: list[GroupWorkflowDef],
        group_key: str | None = None,
    ) -> WorkflowGroupState:
        """一次提交一组工作流。

        - 校验(空组/组内工作流 id 重复/各工作流任务图)全部通过后才触碰存储,
          校验失败即整组拒绝,不产生任何持久化;
        - 组 + 全部成员工作流在一次 store 事务内落盘:要么全部生效,要么不生效;
        - group_key 防重:同键同内容 -> 幂等返回已落地的组(不重复建工作流);
          同键不同内容 -> GroupKeyConflictError(可与校验失败区分),
          并携带 existing_group_id 指明最终落到的组。
        """
        workflows = list(workflows or [])
        self.validate_group(workflows)
        spec_hash = self.group_spec_hash(workflows)
        now = time.time()

        def _txn(
            wfs: dict[str, WorkflowState], groups: dict[str, WorkflowGroupState]
        ) -> WorkflowGroupState:
            existing = groups.get(group_id)
            if existing is None and group_key is not None:
                existing = next(
                    (g for g in groups.values() if g.group_key == group_key), None
                )
            if existing is not None:
                if existing.spec_hash == spec_hash and existing.group_key == group_key:
                    logger.info(
                        "duplicate group submission ignored: group=%s group_key=%s -> existing group",
                        existing.id,
                        group_key,
                    )
                    return existing
                raise GroupKeyConflictError(existing.id)
            for w in workflows:
                if w.workflow_id in wfs:
                    raise WorkflowIdConflictError(w.workflow_id)
            group = WorkflowGroupState(
                id=group_id,
                group_key=group_key,
                spec_hash=spec_hash,
                workflow_ids=[w.workflow_id for w in workflows],
                status=GroupStatus.PENDING,
                created_at=now,
                updated_at=now,
            )
            groups[group.id] = group
            for w in workflows:
                wfs[w.workflow_id] = self.build_state(
                    w.workflow_id, w.tasks, w.idempotency_key, now
                )
            return group

        group = self.store.transact(_txn)
        logger.info(
            "group=%s accepted: %d workflow(s) %s, group_key=%s",
            group.id,
            len(workflows),
            [w.workflow_id for w in workflows],
            group_key,
        )
        self.scheduler.notify()
        return group

    @staticmethod
    def validate_group(workflows: list[GroupWorkflowDef]) -> None:
        """组级校验:纯函数,失败即整组拒绝,与存储无关。"""
        if not workflows:
            raise EmptyGroupError()
        ids = [w.workflow_id for w in workflows]
        if len(set(ids)) != len(ids):
            dup = next(i for i in ids if ids.count(i) > 1)
            raise DuplicateWorkflowIdError(dup)
        for w in workflows:
            OrchestratorService.validate(list(w.tasks or []))

    @staticmethod
    def group_spec_hash(workflows: list[GroupWorkflowDef]) -> str:
        """组提交内容的规范化指纹:与成员顺序无关,内容一致则指纹一致。"""
        payload = [
            {
                "workflow_id": w.workflow_id,
                "idempotency_key": w.idempotency_key,
                "tasks": [t.to_dict() for t in w.tasks],
            }
            for w in sorted(workflows, key=lambda x: x.workflow_id)
        ]
        blob = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    # ================= 组查询 =================
    def get_group(self, group_id: str) -> WorkflowGroupState:
        group = self.store.get_group(group_id)
        if group is None:
            raise GroupNotFoundError(group_id)
        return group

    def get_group_members(self, group_id: str) -> dict[str, WorkflowState]:
        """按组查询组内各工作流的当前状态(保持组内提交顺序)。"""
        group = self.get_group(group_id)
        all_wfs = self.store.load_all()
        return {wid: all_wfs[wid] for wid in group.workflow_ids if wid in all_wfs}

    # ================= 校验 =================
    @staticmethod
    def validate(tasks: list[TaskDef]) -> None:
        if not tasks:
            raise EmptyWorkflowError()
        ids = [t.id for t in tasks]
        if len(set(ids)) != len(ids):
            dup = next(i for i in ids if ids.count(i) > 1)
            raise DuplicateTaskIdError(dup)
        id_set = set(ids)
        for t in tasks:
            for dep in t.dependencies:
                if dep not in id_set:
                    raise MissingDependencyError(t.id, dep)
        deps = {t.id: list(t.dependencies) for t in tasks}
        cycle = OrchestratorService.detect_cycle(ids, deps)
        if cycle is not None:
            raise CycleDetectedError(cycle)

    @staticmethod
    def detect_cycle(task_ids: list[str], deps: dict[str, list[str]]) -> list[str] | None:
        """DFS 三色法检测环;发现回边时返回环上节点序列(首尾同节点)。"""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {tid: WHITE for tid in task_ids}
        stack: list[str] = []

        def dfs(node: str) -> list[str] | None:
            color[node] = GRAY
            stack.append(node)
            for nxt in deps.get(node, []):
                if color.get(nxt) == GRAY:
                    idx = stack.index(nxt)
                    return stack[idx:] + [nxt]
                if color.get(nxt) == WHITE:
                    found = dfs(nxt)
                    if found is not None:
                        return found
            stack.pop()
            color[node] = BLACK
            return None

        for tid in task_ids:
            if color[tid] == WHITE:
                found = dfs(tid)
                if found is not None:
                    return found
        return None

    # ================= 查询 =================
    def get_workflow(self, workflow_id: str) -> WorkflowState:
        wf = self.store.get(workflow_id)
        if wf is None:
            raise WorkflowNotFoundError(workflow_id)
        return wf

    def get_task(self, workflow_id: str, task_id: str) -> TaskState:
        wf = self.get_workflow(workflow_id)
        t = wf.tasks.get(task_id)
        if t is None:
            raise TaskNotFoundError(workflow_id, task_id)
        return t

    # ================= 取消 / 终止 =================
    def cancel_workflow(self, workflow_id: str) -> WorkflowState:
        self.get_workflow(workflow_id)  # 不存在 -> WorkflowNotFoundError
        self.scheduler.request_cancel(workflow_id)
        return self.get_workflow(workflow_id)

    def terminate_workflow(self, workflow_id: str) -> WorkflowState:
        wf = self.get_workflow(workflow_id)
        if wf.status == WorkflowStatus.CANCELLED:
            return wf  # 终态不可逆:重复终止幂等返回
        self.scheduler.request_terminate(workflow_id)
        return self.get_workflow(workflow_id)

    # ================= 组级取消 / 终止 =================
    def cancel_group(self, group_id: str) -> WorkflowGroupState:
        self.get_group(group_id)  # 不存在 -> GroupNotFoundError
        self.scheduler.request_group_cancel(group_id)
        return self.get_group(group_id)

    def terminate_group(self, group_id: str) -> WorkflowGroupState:
        group = self.get_group(group_id)
        if group.status == GroupStatus.CANCELLED:
            return group  # 终态不可逆:重复终止幂等返回
        self.scheduler.request_group_terminate(group_id)
        return self.get_group(group_id)

    # ================= 构造 =================
    @staticmethod
    def build_state(
        workflow_id: str,
        tasks: list[TaskDef],
        idempotency_key: str | None,
        now: float,
    ) -> WorkflowState:
        states: dict[str, TaskState] = {}
        for td in tasks:
            max_attempts = 1 + td.retry_policy.max_retries if td.retryable else 1
            states[td.id] = TaskState(
                id=td.id,
                action=td.action,
                params=dict(td.params),
                dependencies=list(td.dependencies),
                retryable=td.retryable,
                retry_policy=RetryPolicy(**td.retry_policy.__dict__),
                status=TaskStatus.PENDING,
                max_attempts=max_attempts,
            )
        return WorkflowState(
            id=workflow_id,
            idempotency_key=idempotency_key,
            tasks=states,
            status=WorkflowStatus.PENDING,
            created_at=now,
            updated_at=now,
        )
