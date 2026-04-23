from __future__ import annotations

import re
from typing import Any

from fastapi import HTTPException


_TIME_PATTERN = re.compile(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$")


def normalize_scheduler_time(value: Any, *, field_name: str = "run_time") -> str:
    raw = str(value or "").strip()
    match = _TIME_PATTERN.fullmatch(raw)
    if not match:
        raise HTTPException(status_code=400, detail=f"{field_name} 必须是 HH:MM 或 HH:MM:SS")

    hour = int(match.group(1))
    minute = int(match.group(2))
    second = int(match.group(3) or 0)
    if hour > 23 or minute > 59 or second > 59:
        raise HTTPException(status_code=400, detail=f"{field_name} 必须是有效时间 HH:MM 或 HH:MM:SS")
    return f"{hour:02d}:{minute:02d}:{second:02d}"
