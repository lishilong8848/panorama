from __future__ import annotations

import threading
from typing import Any, Callable, Dict


class InspectionRetriggerWatchdog:
    """Retry persisted inspection dispatches outside scheduler callbacks."""

    def __init__(self, *, dispatch: Callable[[], Dict[str, Any]], interval_sec: float = 10.0,
                 emit_log: Callable[[str], None] | None = None) -> None:
        self._dispatch = dispatch
        self._interval = max(1.0, float(interval_sec))
        self._emit_log = emit_log or (lambda text: None)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> Dict[str, Any]:
        if self._thread and self._thread.is_alive():
            return {"ok": True, "running": True, "reason": "already_started"}
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="InspectionRetriggerWatchdog", daemon=True)
        self._thread.start()
        return {"ok": True, "running": True, "reason": "started"}

    def stop(self, timeout: float = 10.0) -> Dict[str, Any]:
        self._stop.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=timeout)
        alive = bool(thread and thread.is_alive())
        if not alive:
            self._thread = None
        return {"ok": True, "running": alive, "stopped": not alive}

    def is_stopped(self) -> bool:
        return self._stop.is_set()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self._dispatch()
            except Exception as exc:  # noqa: BLE001 - the next tick retries
                self._emit_log(f"[巡检数据处理] 补传巡检异常: {exc}")
