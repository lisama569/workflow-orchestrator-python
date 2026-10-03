"""本地 JSON 文件状态存储。

- 单文件保存全部工作流与工作组,结构 {"version":2,"workflows":{...},"groups":{...}}。
  兼容 v1 文件(无 groups 段,按空组集合加载)。
- 写盘走 "临时文件 + fsync + os.replace",任何时刻读到的都是完整快照,
  进程崩溃不会留下半截 JSON。
- RLock 保证并发线程下 读-改-写 的原子性(mutate / transact)。
- transact 可在一次持锁内同时修改组与多个成员工作流并一次落盘,
  保证"整组提交要么全部生效、要么完全不生效"。
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from collections.abc import Callable
from typing import TypeVar

from .models import GroupState, WorkflowState

logger = logging.getLogger("orchestrator.store")

T = TypeVar("T")
_STORE_VERSION = 2


class JsonStore:
    """工作流与工作组状态的本地持久化存储。"""

    def __init__(self, path: str = "workflow_state.json") -> None:
        self.path = path
        self._lock = threading.RLock()
        self._cache: dict[str, WorkflowState] | None = None
        self._groups: dict[str, GroupState] | None = None

    # ---------- 内部 ----------
    def _load_locked(self) -> dict[str, WorkflowState]:
        if self._cache is not None:
            return self._cache
        self._load_file_locked()
        return self._cache  # type: ignore[return-value]

    def _load_groups_locked(self) -> dict[str, GroupState]:
        if self._groups is not None:
            return self._groups
        self._load_file_locked()
        return self._groups  # type: ignore[return-value]

    def _load_file_locked(self) -> None:
        """一次性加载整个状态文件(工作流 + 组);两侧缓存同时填充。"""
        workflows: dict[str, WorkflowState] = {}
        groups: dict[str, GroupState] = {}
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            for wid, wd in (raw.get("workflows") or {}).items():
                workflows[wid] = WorkflowState.from_dict(wd)
            for gid, gd in (raw.get("groups") or {}).items():
                groups[gid] = GroupState.from_dict(gd)
            logger.info(
                "loaded %d workflow(s), %d group(s) from %s",
                len(workflows),
                len(groups),
                self.path,
            )
        else:
            logger.info("no state file at %s; starting empty", self.path)
        if self._cache is None:
            self._cache = workflows
        if self._groups is None:
            self._groups = groups

    def _persist_locked(self) -> None:
        payload = {
            "version": _STORE_VERSION,
            "workflows": {wid: wf.to_dict() for wid, wf in (self._cache or {}).items()},
            "groups": {gid: g.to_dict() for gid, g in (self._groups or {}).items()},
        }
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".wf-state-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    # ---------- 对外 API ----------
    def save_workflow(self, wf: WorkflowState) -> None:
        """持久化单个工作流(原子写整盘快照)。"""
        with self._lock:
            cache = self._load_locked()
            cache[wf.id] = wf
            self._persist_locked()

    def load_all(self) -> dict[str, WorkflowState]:
        """启动时加载全部工作流;无文件时返回空 dict。"""
        with self._lock:
            return dict(self._load_locked())

    def get(self, workflow_id: str) -> WorkflowState | None:
        """读取单个工作流;不存在返回 None。"""
        with self._lock:
            return self._load_locked().get(workflow_id)

    def mutate(
        self,
        workflow_id: str,
        fn: Callable[[WorkflowState], T],
        default: Callable[[], WorkflowState] | None = None,
    ) -> T:
        """持锁执行 读-改-写,fn 返回值原样透传,fn 结束后立即原子落盘。

        default: 工作流不存在时的构造器;不提供则抛 KeyError。
        """
        with self._lock:
            cache = self._load_locked()
            wf = cache.get(workflow_id)
            if wf is None:
                if default is None:
                    raise KeyError(workflow_id)
                wf = default()
                cache[workflow_id] = wf
            result = fn(wf)
            self._persist_locked()
            return result

    def delete(self, workflow_id: str) -> bool:
        """删除工作流(主要给测试用),返回是否曾存在。"""
        with self._lock:
            cache = self._load_locked()
            existed = cache.pop(workflow_id, None) is not None
            if existed:
                self._persist_locked()
            return existed

    # ---------- 工作组 ----------
    def save_group(self, group: GroupState) -> None:
        """持久化单个组(原子写整盘快照)。"""
        with self._lock:
            groups = self._load_groups_locked()
            groups[group.id] = group
            self._persist_locked()

    def get_group(self, group_id: str) -> GroupState | None:
        """读取单个组;不存在返回 None。"""
        with self._lock:
            return self._load_groups_locked().get(group_id)

    def load_groups(self) -> dict[str, GroupState]:
        """加载全部组;无文件时返回空 dict。"""
        with self._lock:
            return dict(self._load_groups_locked())

    def mutate_group(
        self,
        group_id: str,
        fn: Callable[[GroupState], T],
        default: Callable[[], GroupState] | None = None,
    ) -> T:
        """持锁对单个组执行 读-改-写 并落盘(语义同 mutate)。"""
        with self._lock:
            groups = self._load_groups_locked()
            g = groups.get(group_id)
            if g is None:
                if default is None:
                    raise KeyError(group_id)
                g = default()
                groups[group_id] = g
            result = fn(g)
            self._persist_locked()
            return result

    def transact(
        self,
        fn: Callable[[dict[str, WorkflowState], dict[str, GroupState]], T],
    ) -> T:
        """一次持锁内同时读写 工作流表 + 组表,fn 结束后一次原子落盘。

        用于整组提交(组 + 全部成员工作流要么全部生效、要么完全不生效)、
        组级取消/终止(组标志与成员任务状态一次落盘)等跨实体不变量。
        fn 抛异常时不落盘,状态保持原样。
        """
        with self._lock:
            workflows = self._load_locked()
            groups = self._load_groups_locked()
            result = fn(workflows, groups)
            self._persist_locked()
            return result
