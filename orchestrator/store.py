"""本地 JSON 文件状态存储。

- 单文件保存全部工作流与工作流组,结构 {"version":2,"workflows":{...},"groups":{...}}
  (兼容读取无 groups 字段的 version 1 文件)。
- 写盘走 "临时文件 + fsync + os.replace",任何时刻读到的都是完整快照,
  进程崩溃不会留下半截 JSON。
- RLock 保证并发线程下 读-改-写 的原子性(mutate / transact);
  transact 在一次持锁内同时修改工作流与组并一次落盘,用于整组提交的全有或全无。
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from collections.abc import Callable
from typing import TypeVar

from .models import WorkflowGroupState, WorkflowState

logger = logging.getLogger("orchestrator.store")

T = TypeVar("T")
_STORE_VERSION = 2


class JsonStore:
    """工作流 / 工作流组状态的本地持久化存储。"""

    def __init__(self, path: str = "workflow_state.json") -> None:
        self.path = path
        self._lock = threading.RLock()
        self._cache: dict[str, WorkflowState] | None = None
        self._groups_cache: dict[str, WorkflowGroupState] | None = None

    # ---------- 内部 ----------
    def _load_locked(self) -> dict[str, WorkflowState]:
        if self._cache is not None:
            return self._cache
        data: dict[str, WorkflowState] = {}
        groups: dict[str, WorkflowGroupState] = {}
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            for wid, wd in (raw.get("workflows") or {}).items():
                data[wid] = WorkflowState.from_dict(wd)
            for gid, gd in (raw.get("groups") or {}).items():
                groups[gid] = WorkflowGroupState.from_dict(gd)
            logger.info(
                "loaded %d workflow(s) and %d group(s) from %s",
                len(data),
                len(groups),
                self.path,
            )
        else:
            logger.info("no state file at %s; starting empty", self.path)
        self._cache = data
        self._groups_cache = groups
        return data

    def _load_groups_locked(self) -> dict[str, WorkflowGroupState]:
        if self._groups_cache is None:
            self._load_locked()
        return self._groups_cache  # type: ignore[return-value]

    def _persist_locked(self) -> None:
        payload = {
            "version": _STORE_VERSION,
            "workflows": {wid: wf.to_dict() for wid, wf in self._cache.items()},
            "groups": {gid: g.to_dict() for gid, g in self._load_groups_locked().items()},
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

    # ---------- 工作流组 ----------
    def get_group(self, group_id: str) -> WorkflowGroupState | None:
        """读取单个组;不存在返回 None。"""
        with self._lock:
            return self._load_groups_locked().get(group_id)

    def load_groups(self) -> dict[str, WorkflowGroupState]:
        """加载全部组;无文件时返回空 dict。"""
        with self._lock:
            return dict(self._load_groups_locked())

    def find_group_by_key(self, group_key: str) -> WorkflowGroupState | None:
        """按防重键查找已落地的组(组数量小,线性扫描即可)。"""
        with self._lock:
            for g in self._load_groups_locked().values():
                if g.group_key == group_key:
                    return g
            return None

    def transact(
        self,
        fn: Callable[[dict[str, WorkflowState], dict[str, WorkflowGroupState]], T],
    ) -> T:
        """在一次持锁内对工作流与组做 读-改-写,fn 结束后一次性原子落盘。

        用于整组提交(组 + 全部成员工作流要么全部生效、要么不生效)
        与组级取消/终止(组标志与成员取消在同一快照内生效)。
        fn 抛异常时不落盘,状态保持原样。
        """
        with self._lock:
            workflows = self._load_locked()
            groups = self._load_groups_locked()
            result = fn(workflows, groups)
            self._persist_locked()
            return result
