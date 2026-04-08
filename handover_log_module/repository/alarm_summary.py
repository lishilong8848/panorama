from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class AlarmSummary:
    total_count: int
    unrecovered_count: int
    accept_description: str
    used_host: str
    used_mode: str
    queried_tables: List[str]
    source: str = "alarm_json"
    building: str = ""
    source_kind: str = ""
    selection_scope: str = ""
    selected_downloaded_at: str = ""
    query_start: str = ""
    query_end: str = ""
    coverage_ok: bool = True
    fallback_used: bool = False
    error: str = ""
