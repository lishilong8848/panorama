from __future__ import annotations

from datetime import datetime
from typing import List


class LogBus:
    def __init__(self, max_lines: int = 5000) -> None:
        self.max_lines = max(100, int(max_lines))
        self.lines: List[str] = []

    def append(self, text: str) -> None:
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {text}"
        self.lines.append(line)
        overflow = len(self.lines) - self.max_lines
        if overflow > 0:
            del self.lines[:overflow]

    def tail(self, limit: int = 200) -> List[str]:
        return self.lines[-max(1, int(limit)):]
