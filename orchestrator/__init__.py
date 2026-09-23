"""本地轻量工作流调度服务。

模块划分:
  models   - 状态枚举与数据模型
  errors   - 可区分的失败原因
  store    - 本地 JSON 状态存储(崩溃可恢复)
  executor - 任务执行器(本地模拟)
  scheduler- 调度循环(依赖推进/退避重试/并发上限)
  service  - 提交/查询/取消/终止门面
  api      - FastAPI HTTP 接口
"""

__all__ = ["__version__"]
__version__ = "0.1.0"
