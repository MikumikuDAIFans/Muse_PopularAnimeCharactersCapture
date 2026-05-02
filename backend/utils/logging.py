"""日志工具"""

import logging
import sys
from datetime import UTC, datetime
from typing import Optional


def setup_logging(
    name: str = "muse",
    level: int = logging.INFO,
    format_string: Optional[str] = None,
) -> logging.Logger:
    """配置日志"""
    if format_string is None:
        format_string = (
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(format_string, datefmt="%Y-%m-%d %H:%M:%S")
    )
    logger.addHandler(handler)
    return logger


class TaskLogger:
    """任务日志记录器（同时输出到控制台和数据库）"""

    def __init__(self, task_id: int, session=None) -> None:
        self.task_id = task_id
        self._session = session
        self._buffer: list = []
        self._buffer_size = 50

    def _format_message(self, level: str, message: str) -> str:
        ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
        return f"[{ts}] [{level}] {message}"

    def _write(self, level: str, message: str) -> None:
        formatted = self._format_message(level, message)
        print(formatted, flush=True)

        # 缓冲写入数据库
        self._buffer.append({
            "task_id": self.task_id,
            "level": level,
            "message": message,
        })

        if len(self._buffer) >= self._buffer_size:
            self.flush()

    def flush(self) -> None:
        """写入缓冲区到数据库"""
        if not self._buffer or self._session is None:
            return

        from models import TaskLog

        try:
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # ���步上下文中批量写入
                for entry in self._buffer:
                    self._session.add(TaskLog(**entry))
                self._session.flush()
            else:
                for entry in self._buffer:
                    self._session.add(TaskLog(**entry))
                self._session.commit()
            self._buffer.clear()
        except Exception as exc:
            print(f"[WARN] 日志写入失败: {exc}", flush=True)

    def info(self, message: str) -> None:
        self._write("INFO", message)

    def warn(self, message: str) -> None:
        self._write("WARN", message)

    def error(self, message: str) -> None:
        self._write("ERROR", message)