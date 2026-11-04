from contextlib import contextmanager
from threading import BoundedSemaphore


_API_SLOTS = BoundedSemaphore(10)


@contextmanager
def api_request_slot(timeout_sec: float = 600):
    if not _API_SLOTS.acquire(timeout=timeout_sec):
        raise RuntimeError("动环接口并发繁忙，请稍后重试")
    try:
        yield
    finally:
        _API_SLOTS.release()
