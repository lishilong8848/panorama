from __future__ import annotations

from typing import Any, Dict


def norm_log_value(value: Any, default: str = "-") -> str:
    text = str(value or "").strip()
    return text if text else default


def build_success_line(
    *,
    feature: str,
    stage: str,
    detail: str = "",
    building: str | None = None,
    file_path: str | None = None,
    upload_date: str | None = None,
    run_id: str | None = None,
) -> str:
    return (
        f"[文件上传成功] 功能={norm_log_value(feature)} 阶段={norm_log_value(stage)} "
        f"楼栋={norm_log_value(building)} 文件={norm_log_value(file_path)} "
        f"日期={norm_log_value(upload_date)} run_id={norm_log_value(run_id)} "
        f"详情={norm_log_value(detail)}"
    )


def build_failure_line(
    *,
    feature: str,
    stage: str,
    error: str,
    building: str | None = None,
    file_path: str | None = None,
    upload_date: str | None = None,
    run_id: str | None = None,
    error_type: str | None = None,
) -> str:
    error_text = " ".join(str(error or "").split())
    base = (
        f"[文件流程失败] 功能={norm_log_value(feature)} 阶段={norm_log_value(stage)} "
        f"楼栋={norm_log_value(building)} 文件={norm_log_value(file_path)} "
        f"日期={norm_log_value(upload_date)} run_id={norm_log_value(run_id)}"
    )
    if str(error_type or "").strip():
        base += f" error_type={norm_log_value(error_type)}"
    return f"{base} 错误={norm_log_value(error_text)}"


def build_structured_context(
    *,
    feature: str,
    stage: str,
    building: str | None = None,
    file_path: str | None = None,
    upload_date: str | None = None,
    run_id: str | None = None,
    error_type: str | None = None,
    duration_ms: int | None = None,
) -> Dict[str, Any]:
    context: Dict[str, Any] = {
        "feature": norm_log_value(feature),
        "stage": norm_log_value(stage),
        "building": norm_log_value(building),
        "file": norm_log_value(file_path),
        "date": norm_log_value(upload_date),
        "run_id": norm_log_value(run_id),
    }
    if str(error_type or "").strip():
        context["error_type"] = norm_log_value(error_type)
    if duration_ms is not None:
        context["duration_ms"] = int(duration_ms)
    return context
