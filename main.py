"""本地运行入口：uv run uvicorn main:app"""
from app.api import create_app

app = create_app()
