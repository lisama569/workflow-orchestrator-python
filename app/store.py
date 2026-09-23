"""本地 JSON 状态存储。

布局：每个工作流一个 JSON 文件 <state_dir>/<workflow_id>.json。
- 进程内一把可重入锁保护内存态与文件写入的串行化；
- 写盘采用“临时文件 + os.replace”原子替换，崩溃最多丢失最后一次未落盘
  的转移，绝不会写出半截 JSON；
- 任务 RUNNING 状态在执行前同步落盘，因此进程崩溃后能在重启时看到
  RUNNING 残留并执行恢复重放。
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from app.models import WorkflowEntity


class JsonStateStore:
    def __init__(self, state_dir: str):
        self._dir = Path(state_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._data: dict[str, WorkflowEntity] = {}
        self._loaded = False

    # ---- 读取 / 恢复 ----

    def load_all(self) -> dict[str, WorkflowEntity]:
        """启动恢复：读取全部工作流文件，之后内容缓存于内存。"""
        with self._lock:
            if self._loaded:
                return self._data
            data: dict[str, WorkflowEntity] = {}
            for path in sorted(self._dir.glob("*.json")):
                with path.open("r", encoding="utf-8") as fh:
                    raw = json.load(fh)
                wf = WorkflowEntity.model_validate(raw)
                data[wf.id] = wf
            self._data = data
            self._loaded = True
            return self._data

    # ---- 事务 ----

    @contextmanager
    def transaction(self) -> Iterator[dict[str, WorkflowEntity]]:
        """串行化的读改写事务。

        进入时持锁并返回以 workflow_id 为键的可变映射；退出（无异常）时
        把映射中每个工作流原子写回各自文件。调用方在 with 块内对
        WorkflowEntity 的就地修改都会被持久化。
        """
        data = self.load_all()
        with self._lock:
            # with 块内抛出异常则跳过下面的落盘，本次改动整体不持久化
            # （内存对象可能已被部分修改；同进程内调用方不应继续使用）。
            yield data
            for wf in data.values():
                self._persist(wf)

    def lock(self) -> threading.RLock:
        """暴露底层锁，供“只调度不落盘”的内部临界区使用。"""
        return self._lock

    def persist(self, wf: WorkflowEntity) -> None:
        """在已持锁的前提下显式落盘（例如执行前固化 RUNNING）。"""
        with self._lock:
            self._persist(wf)

    # ---- 内部 ----

    def _persist(self, wf: WorkflowEntity) -> None:
        path = self._dir / f"{wf.id}.json"
        payload = wf.model_dump_json(indent=2)
        fd, tmp_name = tempfile.mkstemp(
            dir=self._dir, prefix=f".{wf.id}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
            raise
