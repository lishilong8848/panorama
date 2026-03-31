from __future__ import annotations

from dataclasses import dataclass


@dataclass
class LogEvent:
    offset: int
    line: str
