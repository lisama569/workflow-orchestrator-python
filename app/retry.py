"""退避策略计算。"""
from __future__ import annotations

from app.models import RetryPolicy


def backoff_delay(policy: RetryPolicy, failure_count: int) -> float:
    """第 failure_count 次可重试失败后的退避秒数。

    delay = min(base_delay * backoff_factor ** (failure_count - 1), max_delay)
    failure_count 从 1 开始；为 0 时无退避。
    """
    if failure_count <= 0:
        return 0.0
    raw = policy.base_delay * (policy.backoff_factor ** (failure_count - 1))
    return min(raw, policy.max_delay)
