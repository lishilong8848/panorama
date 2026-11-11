from __future__ import annotations

import threading
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

from handover_log_module.service.training_management_payload_builder import TrainingManagementPayloadBuilder


class StationHTrainingPreviewService:
    """Share the monthly source read among H review page requests on this process."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._key: tuple[str, str] | None = None
        self._expires_at = 0.0
        self._items: dict[str, tuple[datetime, dict[str, Any]]] = {}

    def read(
        self, builder: TrainingManagementPayloadBuilder, *, duty_date: str, emit_log: Callable[[str], None],
    ) -> dict[str, tuple[datetime, dict[str, Any]]]:
        key = builder.preview_cache_key(duty_date)
        if key is None:
            return {}
        with self._lock:
            if self._key == key and time.monotonic() < self._expires_at:
                return self._items
            error = ""
            try:
                items = builder.preload(duty_date=duty_date, emit_log=emit_log)
            except Exception as exc:  # noqa: BLE001
                items = {}
                error = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
            self._key = key
            self._items = items
            self._expires_at = time.monotonic() + (60 if error else 300)
        if error:
            emit_log(f"[交接班][H楼审核页] 培训接口自动拉取失败，暂用已保存内容: {error}")
        return items
