"""Host-local generation exclusion; lock ownership is released by the OS on exit."""
import hashlib
import os
import tempfile
import threading
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from pathlib import Path


_SLOTS = threading.BoundedSemaphore(5)
_SLOT_WAIT_TIMEOUT_SEC = 60


@contextmanager
def generation_lock(building, duty_date, duty_shift):
    key = hashlib.sha256(f"{building}|{duty_date}|{duty_shift}".encode("utf-8")).hexdigest()
    root = Path(tempfile.gettempdir()) / "panorama_handover_generation_locks"
    root.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(root / f"{key}.lock"), os.O_CREAT | os.O_RDWR, 0o600)
    locked = False
    try:
        if os.fstat(fd).st_size == 0:
            os.write(fd, b"0")
        os.lseek(fd, 0, os.SEEK_SET)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
        except OSError as exc:
            if exc.errno not in {11, 13, 35, 36} and getattr(exc, "winerror", None) not in {32, 33, 36}:
                raise
            raise RuntimeError(f"{building} {duty_date}/{duty_shift} 正在生成，本次重复请求未执行，请等待原任务完成") from exc
        yield
    finally:
        try:
            if locked:
                os.lseek(fd, 0, os.SEEK_SET)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
        # Keep the file: unlinking allows other processes to lock a different inode.


def guard_generation(method):
    @wraps(method)
    def generate(self, **kwargs):
        building = str(kwargs.get("building") or "").strip().upper()
        date = str(kwargs.get("duty_date") or "").strip()
        shift = str(kwargs.get("duty_shift") or "").strip().lower()
        if not date or not shift:
            inferred_date, inferred_shift = self._infer_duty_by_now()
            date, shift = date or inferred_date, shift or inferred_shift
        date = datetime.strptime(date, "%Y-%m-%d").date().isoformat()
        kwargs.update(building=building, duty_date=date, duty_shift=shift)
        with generation_lock(building, date, shift):
            if not _SLOTS.acquire(timeout=_SLOT_WAIT_TIMEOUT_SEC):
                raise TimeoutError(f"{building} {date}/{shift} 生成资源繁忙，等待{_SLOT_WAIT_TIMEOUT_SEC}秒超时；本次未生成，请稍后重试")
            try:
                return method(self, **kwargs)
            finally:
                _SLOTS.release()
    return generate
