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
    DuplicateWorkflowIdInGroupError,
    EmptyGroupError,
    EmptyWorkflowError,
    GroupKeyConflictError,
    GroupNotFoundError,
    MissingDependencyError,
    TaskNotFoundError,
    TerminalStateError,
    WorkflowNotFoundError,
)
from .models import (
    GroupState,
    GroupStatus,
    RetryPolicy,
    TaskDef,
    TaskState,
    TaskStatus,
    WorkflowDef,
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

    # ================= 工作流组:提交 =================
    def submit_group(
        self,
        group_id: str,
        workflows: list[WorkflowDef],
        idempotency_key: str | None = None,
    ) -> GroupState:
        """整组提交:要么全部生效、要么完全不生效。

        幂等语义(同一组键 idempotency_key):
          - 键 + 内容都一致 -> 返回已落地的同一个组,不重复创建/执行;
          - 键被复用但内容不一致 -> GroupKeyConflictError(409),携带 existing_group_id。
        组 id 本身复用但内容/键不一致同样按冲突拒绝。
        """
        workflows = list(workflows or [])
        self.validate_group(workflows)
        fingerprint = self.group_fingerprint(workflows)
        now = time.time()

        def _tx(wfs: dict[str, WorkflowState], groups: dict[str, GroupState]) -> GroupState:
            # 1) 同组 id 已存在:内容+键一致 => 幂等返回;否则冲突。
            existing = groups.get(group_id)
            if existing is not None:
                if (
                    idempotency_key is not None
                    and existing.idempotency_key == idempotency_key
                    and existing.fingerprint == fingerprint
                ):
                    logger.info(
                        "duplicate group submission ignored: group=%s idempotency_key=%s",
                        group_id,
                        idempotency_key,
                    )
                    return existing
                raise GroupKeyConflictError(existing.id)
            # 2) 组键被其它组占用:内容一致 => 幂等返回那个组;否则冲突并指向它。
            if idempotency_key is not None:
                for g in groups.values():
                    if g.idempotency_key == idempotency_key:
                        if g.fingerprint == fingerprint:
                            logger.info(
                                "duplicate group submission ignored: idempotency_key=%s -> existing group=%s",
                                idempotency_key,
                                g.id,
                            )
                            return g
                        raise GroupKeyConflictError(g.id)
            # 3) 成员工作流 id 不得与既有工作流冲突(组尚不存在,任何命中都是外部冲突)。
            for wd in workflows:
                if wd.id in wfs:
                    raise DuplicateIdempotencyKeyError(wd.id)
            # 4) 全部校验通过:组 + 全部成员工作流一次落盘(要么全生效,要么全不生效)。
            group = GroupState(
                id=group_id,
                idempotency_key=idempotency_key,
                fingerprint=fingerprint,
                workflow_ids=[wd.id for wd in workflows],
                status=GroupStatus.PENDING,
                created_at=now,
                updated_at=now,
            )
            groups[group_id] = group
            for wd in workflows:
                wfs[wd.id] = self.build_state(wd.id, wd.tasks, idempotency_key=None, now=now)
            logger.info(
                "group=%s accepted: %d workflow(s) [%s], idempotency_key=%s",
                group_id,
                len(workflows),
                ", ".join(wd.id for wd in workflows),
                idempotency_key,
            )
            return group

        group = self.store.transact(_tx)
        self.scheduler.notify()
        return group

    # ================= 工作流组:校验与指纹 =================
    @staticmethod
    def validate_group(workflows: list[WorkflowDef]) -> None:
        """组级校验:非空、组内工作流 id 唯一、每个工作流内部走原有单工作流校验。"""
        if not workflows:
            raise EmptyGroupError()
        ids = [w.id for w in workflows]
        if len(set(ids)) != len(ids):
            dup = next(i for i in ids if ids.count(i) > 1)
            raise DuplicateWorkflowIdInGroupError(dup)
        for wd in workflows:
            OrchestratorService.validate(wd.tasks)

    @staticmethod
    def group_fingerprint(workflows: list[WorkflowDef]) -> str:
        """组提交内容的规范哈希:与成员顺序无关,内容任何差异都会改变指纹。"""
        canonical = sorted((wd.to_dict() for wd in workflows), key=lambda d: d["id"])
        blob = json.dumps(canonical, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    # ================= 工作流组:查询 =================
    def get_group(self, group_id: str) -> GroupState:
        g = self.store.get_group(group_id)
        if g is None:
            raise GroupNotFoundError(group_id)
        return g

    def list_groups(self) -> list[GroupState]:
        return list(self.store.load_groups().values())

    def get_group_view(self, group_id: str) -> tuple[GroupState, list[WorkflowState]]:
        """组 + 组内各成员工作流当前状态(按提交顺序)。"""
        group = self.get_group(group_id)
        members = [self.get_workflow(wid) for wid in group.workflow_ids]
        return group, members

    # ================= 工作流组:取消 / 终止 =================
    def cancel_group(self, group_id: str) -> GroupState:
        self.get_group(group_id)  # 不存在 -> GroupNotFoundError
        self.scheduler.request_cancel_group(group_id)
        return self.get_group(group_id)

    def terminate_group(self, group_id: str) -> GroupState:
        group = self.get_group(group_id)
        if group.status == GroupStatus.CANCELLED:
            return group  # 终态不可逆:重复终止幂等返回
        self.scheduler.request_terminate_group(group_id)
        return self.get_group(group_id)
