"""FastAPI 应用入口"""

import sys
from pathlib import Path

# 确保 backend/ 目录在 Python 路径中（支持本地直接运行 python -m backend.main）
_backend_dir = Path(__file__).parent
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))

import time
import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from config import get_settings
from database import init_db, close_db, get_session_factory
from models import Task, TaskLog
from sqlalchemy import select
from api.routes import router as api_router


# 启动时间
_start_time = time.monotonic()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """应用生命周期管理"""
    # 启动
    await init_db()
    print("[INFO] 数据库初始化完成")
    yield
    # 关闭
    await close_db()
    print("[INFO] 数据库连接已关闭")


def create_app() -> FastAPI:
    """创建FastAPI应用"""
    settings = get_settings()

    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        description="Danbooru 训练数据下载服务 — REST API + Web UI",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ========== API路由（必须在 SPA fallback 之前）==========
    app.include_router(api_router, prefix=settings.API_PREFIX)

    # ========== 健康检查 ==========
    @app.get("/health")
    async def health():
        uptime = time.monotonic() - _start_time
        return {
            "status": "ok",
            "version": settings.APP_VERSION,
            "uptime": round(uptime, 2),
        }

    @app.websocket("/ws/progress/{task_id}")
    async def ws_progress(websocket: WebSocket, task_id: int):
        """实时推送任务进度，前端/Agent 可直接订阅。"""
        await websocket.accept()
        try:
            factory = await get_session_factory()
            while True:
                async with factory() as session:
                    result = await session.execute(select(Task).where(Task.id == task_id))
                    task = result.scalar_one_or_none()
                    if task is None:
                        await websocket.send_json({"error": "任务不存在"})
                        return
                    await websocket.send_json({
                        "task_id": task.id,
                        "status": task.status,
                        "progress": task.progress,
                        "processed_count": task.processed_count,
                        "total_count": task.total_count,
                        "error_count": task.error_count,
                        "updated_at": task.updated_at.isoformat() if task.updated_at else None,
                    })
                await asyncio.sleep(2)
        except WebSocketDisconnect:
            return

    @app.websocket("/ws/logs/{task_id}")
    async def ws_logs(websocket: WebSocket, task_id: int):
        """实时推送任务日志增量。"""
        await websocket.accept()
        last_id = 0
        try:
            factory = await get_session_factory()
            while True:
                async with factory() as session:
                    result = await session.execute(
                        select(TaskLog)
                        .where(TaskLog.task_id == task_id, TaskLog.id > last_id)
                        .order_by(TaskLog.id.asc())
                        .limit(100)
                    )
                    logs = result.scalars().all()
                    for log in logs:
                        last_id = max(last_id, log.id)
                        await websocket.send_json({
                            "id": log.id,
                            "task_id": log.task_id,
                            "level": log.level,
                            "message": log.message,
                            "created_at": log.created_at.isoformat() if log.created_at else None,
                        })
                await asyncio.sleep(2)
        except WebSocketDisconnect:
            return

    # ========== 根路径跳转 /docs /redoc ==========
    @app.get("/")
    async def root():
        return RedirectResponse(url="/docs")

    # ========== 前端静态文件 + SPA fallback（最后，防止拦截 API 路由）==========
    try:
        assets_dir = Path("frontend/dist/assets")
        if assets_dir.exists():
            app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

        @app.get("/{full_path:path}")
        async def spa_fallback(full_path: str):
            fe_path = Path("frontend/dist") / f"{full_path}.html"
            if fe_path.exists():
                from fastapi.responses import FileResponse
                return FileResponse(str(fe_path))
            fe_index = Path("frontend/dist/index.html")
            if fe_index.exists():
                from fastapi.responses import FileResponse
                return FileResponse(str(fe_index))
            return {"error": "Frontend not built. Run: cd frontend && npm run build"}
    except Exception:
        pass

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    settings = get_settings()
    uvicorn.run(
        "backend.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        log_level="info",
    )
