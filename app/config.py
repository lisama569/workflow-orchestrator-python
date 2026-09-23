"""全局配置。"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    state_dir: str = os.environ.get("WF_STATE_DIR", ".state")
    tick_interval: float = float(os.environ.get("WF_TICK_INTERVAL", "0.05"))
