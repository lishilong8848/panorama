from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class RawRow:
    row_index: int
    b_text: str
    c_text: str
    d_name: str
    e_raw: Any
    value: Optional[float]
    b_norm: str = ""
    c_norm: str = ""


@dataclass
class MetricHit:
    metric_key: str
    row_index: int
    d_name: str
    value: Optional[float]
    b_norm: str = ""
    c_norm: str = ""
    b_text: str = ""
    c_text: str = ""


@dataclass
class FillValue:
    metric_key: str
    cell: str
    text: str
    from_rows: List[int] = field(default_factory=list)


@dataclass
class BuildingResult:
    building: str
    data_file: str
    output_file: str = ""
    capacity_output_file: str = ""
    capacity_status: str = ""
    capacity_error: str = ""
    capacity_warnings: List[str] = field(default_factory=list)
    success: bool = False
    fills: List[FillValue] = field(default_factory=list)
    missing_metrics: List[str] = field(default_factory=list)
    cloud_sheet_sync: Dict[str, Any] = field(default_factory=dict)
    review_session: Dict[str, Any] = field(default_factory=dict)
    alarm_summary: Dict[str, Any] = field(default_factory=dict)
    batch_key: str = ""
    confirmed: bool = False
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "building": self.building,
            "data_file": self.data_file,
            "output_file": self.output_file,
            "capacity_output_file": self.capacity_output_file,
            "capacity_status": self.capacity_status,
            "capacity_error": self.capacity_error,
            "capacity_warnings": list(self.capacity_warnings or []),
            "success": self.success,
            "fills": [asdict(x) for x in self.fills],
            "missing_metrics": self.missing_metrics,
            "cloud_sheet_sync": dict(self.cloud_sheet_sync or {}),
            "review_session": dict(self.review_session or {}),
            "alarm_summary": dict(self.alarm_summary or {}),
            "batch_key": self.batch_key,
            "confirmed": self.confirmed,
            "errors": self.errors,
        }


@dataclass
class RunSummary:
    mode: str
    success_count: int = 0
    failed_count: int = 0
    results: List[BuildingResult] = field(default_factory=list)
    start_time: str = ""
    end_time: str = ""
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "results": [item.to_dict() for item in self.results],
            "errors": self.errors,
        }
