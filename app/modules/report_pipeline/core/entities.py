from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List


@dataclass
class TimeWindow:
    date: str
    start_time: str
    end_time: str


@dataclass
class JobResult:
    success: bool
    summary: str
    payload: Dict[str, Any] | List[Dict[str, Any]] | None = None


@dataclass
class PipelinePhaseResult:
    phase: str
    status: str
    started_at: str
    finished_at: str
    duration_ms: int
    message: str = ""
    error: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "phase": self.phase,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": int(self.duration_ms),
            "message": self.message,
            "error": self.error,
            "metadata": dict(self.metadata),
        }


@dataclass
class NetworkSwitchReport:
    target_ssid: str
    success: bool
    error_type: str = ""
    error: str = ""
    duration_ms: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "target_ssid": self.target_ssid,
            "success": bool(self.success),
            "error_type": self.error_type,
            "error": self.error,
            "duration_ms": int(self.duration_ms),
            "metadata": dict(self.metadata),
        }


@dataclass
class AlarmExportSummary:
    uploaded_count: int
    raw_count: int
    skipped_count: int
    success_buildings: List[str] = field(default_factory=list)
    failed_buildings: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "uploaded_count": int(self.uploaded_count),
            "raw_count": int(self.raw_count),
            "skipped_count": int(self.skipped_count),
            "success_buildings": list(self.success_buildings),
            "failed_buildings": list(self.failed_buildings),
        }


@dataclass
class JobResultV2:
    status: str
    summary: str
    phase_results: List[PipelinePhaseResult] = field(default_factory=list)
    payload: Dict[str, Any] = field(default_factory=dict)
    retryable: bool = False
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def to_dict(self) -> Dict[str, Any]:
        data = dict(self.payload)
        data.update(
            {
                "status": self.status,
                "summary": self.summary,
                "retryable": bool(self.retryable),
                "created_at": self.created_at,
                "phase_results": [item.to_dict() for item in self.phase_results],
            }
        )
        return data
