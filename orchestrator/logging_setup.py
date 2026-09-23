"""日志配置:统一输出任务标识、状态转移与判定依据。"""
from __future__ import annotations

import logging

_DEFAULT_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def setup_logging(level: int = logging.INFO) -> None:
    """配置根日志(幂等:重复调用不重复加 handler)。"""
    root = logging.getLogger()
    root.setLevel(level)
    if not any(getattr(h, "_orch_handler", False) for h in root.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT))
        handler._orch_handler = True  # type: ignore[attr-defined]
        root.addHandler(handler)


def log_transition(
    logger: logging.Logger,
    task_id: str,
    frm: str,
    to: str,
    reason: str,
) -> None:
    """统一格式: task=<id> transition <FROM>-><TO> reason=<判定依据>。"""
    logger.info("task=%s transition %s->%s reason=%s", task_id, frm, to, reason)
