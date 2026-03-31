from __future__ import annotations

from typing import List, Literal, Optional, TypedDict


class RunFromDownloadRequest(TypedDict, total=False):
    buildings: List[str]
    end_time: str
    duty_date: str
    duty_shift: Literal["day", "night"]


class RunFromFileRequest(TypedDict):
    building: str
    data_file: str
    end_time: Optional[str]
