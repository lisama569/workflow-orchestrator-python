"""本地 JSON 文件状态存储。

- 单文件保存全部工作流,结构 {"version":1,"workflows":{...}}。
- 写盘走 "临时文件 + fsync + os.replace",任何时刻读到的都是完整快照,
  进程崩溃不会留下半截 JSON。
- RLock 保证并发线程下 读-改-写 的原子性(mutate)。
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from collections.abc import Callable
from typing import TypeVar

from .models import WorkflowState

logger = logging.getLogger("orchestrator.store")

T = TypeVar("T")
_STORE_VERSION = 1


class JsonStore:
    """工作流状态的本地持久化存储。"""

    def __init__(self, path: str = "workflow_state.json") -> None:
        self.path = path
        self._lock = threading.RLock()
        self._cache: dict[str, WorkflowState] | None = None

    # ---------- 内部 ----------
    def _load_locked(self) -> dict[str, WorkflowState]:
        if self._cache is not None:
            return self._cache
        data: dict[str, WorkflowState] = {}
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            for wid, wd in (raw.get("workflows") or {}).items():
                data[wid] = WorkflowState.from_dict(wd)
            logger.info("loaded %d workflow(s) from %s", len(data), self.path)
        else:
            logger.info("no state file at %s; starting empty", self.path)
        self._cache = data
        return data

    def _persist_locked(self) -> None:
        payload = {
            "version": _STORE_VERSION,
            "workflows": {wid: wf.to_dict() for wid, wf in self._cache.items()},
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
