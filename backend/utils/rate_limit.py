"""速率限制工具"""

import time
import threading
from typing import Optional


class RateLimiter:
    """线程安全的令牌桶速率限制器"""

    def __init__(self, rate_per_sec: float, burst: float = 1.0) -> None:
        """
        Args:
            rate_per_sec: 每秒允许的请求数
            burst: 突发容量（允许一次发送的最大令牌数）
        """
        self.rate = float(rate_per_sec)
        self.burst = float(burst)
        self._tokens = float(burst)
        self._lock = threading.Lock()
        self._last_update = time.monotonic()

    def _refill(self) -> None:
        """补充令牌"""
        now = time.monotonic()
        elapsed = now - self._last_update
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._last_update = now

    def wait(self) -> None:
        """等待获取令牌（阻塞）"""
        if self.rate <= 0:
            return

        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            sleep_time = (1.0 - self._tokens) / self.rate
            self._tokens = 0.0
            self._last_update = time.monotonic()

        time.sleep(sleep_time)

    async def wait_async(self) -> None:
        """异步等待获取令牌"""
        if self.rate <= 0:
            return

        import asyncio
        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            sleep_time = (1.0 - self._tokens) / self.rate
            self._tokens = 0.0
            self._last_update = time.monotonic()

        await asyncio.sleep(sleep_time)

    def try_acquire(self) -> bool:
        """尝试获取令牌（非阻塞）"""
        if self.rate <= 0:
            return True

        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    def reset(self) -> None:
        """重置令牌桶"""
        with self._lock:
            self._tokens = float(self.burst)
            self._last_update = time.monotonic()