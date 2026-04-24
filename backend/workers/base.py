"""Worker基类"""

import asyncio
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class WorkerConfig:
    """Worker配置"""
    max_workers: int = 4
    poll_interval: float = 1.0
    batch_size: int = 100


@dataclass
class WorkerProgress:
    """Worker进度"""
    worker_id: str
    processed: int = 0
    total: int = 0
    error_count: int = 0
    status: str = "idle"
    last_update: float = field(default_factory=time.monotonic)
    message: str = ""

    @property
    def progress(self) -> float:
        if self.total <= 0:
            return 0.0
        return min(self.processed / self.total, 1.0)


class BaseWorker(ABC):
    """Worker基类"""

    def __init__(self, config: Optional[WorkerConfig] = None) -> None:
        self.config = config or WorkerConfig()
        self._stop_event = threading.Event()
        self._progress: Dict[str, WorkerProgress] = {}
        self._progress_lock = threading.Lock()

    @abstractmethod
    def run(self) -> None:
        """执行Worker主逻辑"""
        pass

    @abstractmethod
    def stop(self) -> None:
        """停止Worker"""
        pass

    def update_progress(self, worker_id: str, **kwargs) -> None:
        """更新进度"""
        with self._progress_lock:
            if worker_id not in self._progress:
                self._progress[worker_id] = WorkerProgress(worker_id=worker_id)
            prog = self._progress[worker_id]
            for key, value in kwargs.items():
                if hasattr(prog, key):
                    setattr(prog, key, value)
            prog.last_update = time.monotonic()

    def get_progress(self) -> List[WorkerProgress]:
        """获取所有Worker进度"""
        with self._progress_lock:
            return list(self._progress.values())

    def is_stopped(self) -> bool:
        """检查是否收到停止信号"""
        return self._stop_event.is_set()


class AsyncWorker(ABC):
    """异步Worker基类"""

    def __init__(self, config: Optional[WorkerConfig] = None) -> None:
        self.config = config or WorkerConfig()
        self._stop_event = asyncio.Event()
        self._progress: Dict[str, WorkerProgress] = {}
        self._progress_lock = asyncio.Lock()

    @abstractmethod
    async def run(self) -> None:
        """异步执行主逻辑"""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """停止Worker"""
        pass

    async def update_progress(self, worker_id: str, **kwargs) -> None:
        """异步更新进度"""
        async with self._progress_lock:
            if worker_id not in self._progress:
                self._progress[worker_id] = WorkerProgress(worker_id=worker_id)
            prog = self._progress[worker_id]
            for key, value in kwargs.items():
                if hasattr(prog, key):
                    setattr(prog, key, value)
            prog.last_update = time.monotonic()

    async def get_progress(self) -> List[WorkerProgress]:
        """获取所有Worker进度"""
        async with self._progress_lock:
            return list(self._progress.values())

    def request_stop(self) -> None:
        """请求停止"""
        self._stop_event.set()

    def is_stopped(self) -> bool:
        return self._stop_event.is_set()