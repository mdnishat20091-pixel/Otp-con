"""Application Logging: Console + শেষ কয়েকটি লাইন মেমরিতে (অ্যাডমিনের /logs-এর জন্য)।"""
from __future__ import annotations

import logging
from collections import deque


class RingBufferHandler(logging.Handler):
    def __init__(self, capacity: int = 300) -> None:
        super().__init__()
        self.buffer: deque[str] = deque(maxlen=capacity)
        self.error_count = 0

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.buffer.append(self.format(record))
            if record.levelno >= logging.ERROR:
                self.error_count += 1
        except Exception:
            pass


_ring = RingBufferHandler()


def setup_logging(level: str = "INFO") -> None:
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.handlers.clear()
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    _ring.setFormatter(fmt)
    root.addHandler(console)
    root.addHandler(_ring)


def recent_logs(n: int = 25) -> list[str]:
    return list(_ring.buffer)[-n:]


def error_count() -> int:
    return _ring.error_count
