"""本地工作流调度服务入口。

运行:
  uv run uvicorn main:app --host 127.0.0.1 --port 8000
环境变量:
  WF_STATE_PATH      状态文件路径(默认 workflow_state.json)
  WF_MAX_CONCURRENCY 全局并发上限(默认 4)
"""
from __future__ import annotations

import os

from orchestrator.api import create_app
from orchestrator.logging_setup import setup_logging

setup_logging()

app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "8000")),
    )
