from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


FAMILY_HANDOVER_LOG = "handover_log_family"
FAMILY_HANDOVER_CAPACITY_REPORT = "handover_capacity_report_family"
FAMILY_MONTHLY_REPORT = "monthly_report_family"
FAMILY_ALARM_EVENT = "alarm_event_family"

SOURCE_TYPE_FOLDERS = {
    FAMILY_HANDOVER_LOG: "交接班日志源文件",
    FAMILY_HANDOVER_CAPACITY_REPORT: "交接班容量报表源文件",
    FAMILY_MONTHLY_REPORT: "全景平台月报源文件",
    FAMILY_ALARM_EVENT: "告警信息源文件",
}

OUTPUT_TYPE_HANDOVER_LOG = "handover_log_output"
OUTPUT_TYPE_HANDOVER_CAPACITY = "handover_capacity_output"
OUTPUT_TYPE_MONTHLY_EVENT = "monthly_event_output"
OUTPUT_TYPE_MONTHLY_CHANGE = "monthly_change_output"

OUTPUT_TYPE_LABELS = {
    OUTPUT_TYPE_HANDOVER_LOG: "交接班日志",
    OUTPUT_TYPE_HANDOVER_CAPACITY: "交接班容量报表",
    OUTPUT_TYPE_MONTHLY_EVENT: "事件月度统计表",
    OUTPUT_TYPE_MONTHLY_CHANGE: "变更月度统计表",
}


@dataclass(frozen=True)
class ArtifactPathInfo:
    type_folder: str
    month_segment: str
    bucket_segment: str
    file_name: str
    relative_path: Path


def sanitize_windows_path_part(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "_"
    sanitized = re.sub(r'[<>:"/\\\\|?*]+', "_", text)
    sanitized = sanitized.strip(" .")
    return sanitized or "_"


def handover_shift_text(duty_shift: str) -> str:
    normalized = str(duty_shift or "").strip().lower()
    if normalized == "day":
        return "白班"
    if normalized == "night":
        return "夜班"
    return "交接班"


def month_segment_from_value(value: str, *, now: datetime | None = None) -> str:
    digits = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
    if len(digits) >= 6:
        return digits[:6]
    return (now or datetime.now()).strftime("%Y%m")


def _parse_datetime(text: str, formats: tuple[str, ...]) -> datetime | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def latest_bucket_segment(bucket_key: str, *, now: datetime | None = None) -> str:
    parsed = _parse_datetime(str(bucket_key or "").strip(), ("%Y-%m-%d %H", "%Y%m%d%H"))
    if parsed is not None:
        return parsed.strftime("%Y%m%d--%H")
    digits = "".join(ch for ch in str(bucket_key or "").strip() if ch.isdigit())
    if len(digits) >= 10:
        return f"{digits[:8]}--{digits[8:10]}"
    return (now or datetime.now()).strftime("%Y%m%d--%H")


def manual_alarm_bucket_segment(bucket_key: str, *, now: datetime | None = None) -> str:
    parsed = _parse_datetime(
        str(bucket_key or "").strip(),
        ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H%M%S", "%Y%m%d%H%M%S"),
    )
    if parsed is not None:
        return parsed.strftime("%Y%m%d--%H%M%S--manual")
    digits = "".join(ch for ch in str(bucket_key or "").strip() if ch.isdigit())
    if len(digits) >= 14:
        return f"{digits[:8]}--{digits[8:14]}--manual"
    current = now or datetime.now()
    return current.strftime("%Y%m%d--%H%M%S--manual")


def source_bucket_segment(
    *,
    source_family: str,
    bucket_kind: str,
    bucket_key: str = "",
    duty_date: str = "",
    duty_shift: str = "",
    now: datetime | None = None,
) -> str:
    normalized_family = str(source_family or "").strip()
    normalized_kind = str(bucket_kind or "").strip().lower() or "latest"
    if normalized_kind == "manual" and normalized_family == FAMILY_ALARM_EVENT:
        return manual_alarm_bucket_segment(bucket_key, now=now)
    if normalized_kind == "latest":
        return latest_bucket_segment(bucket_key, now=now)

    duty_digits = "".join(ch for ch in str(duty_date or "").strip() if ch.isdigit())[:8]
    if len(duty_digits) != 8:
        duty_digits = (now or datetime.now()).strftime("%Y%m%d")

    if normalized_family in {FAMILY_HANDOVER_LOG, FAMILY_HANDOVER_CAPACITY_REPORT}:
        return f"{duty_digits}--{handover_shift_text(duty_shift)}"
    if normalized_family == FAMILY_MONTHLY_REPORT:
        return f"{duty_digits}--月报"
    if normalized_family == FAMILY_ALARM_EVENT:
        return duty_digits
    return duty_digits


def build_source_artifact_path(
    *,
    source_family: str,
    building: str,
    suffix: str,
    bucket_kind: str,
    bucket_key: str = "",
    duty_date: str = "",
    duty_shift: str = "",
    now: datetime | None = None,
) -> ArtifactPathInfo:
    normalized_family = str(source_family or "").strip()
    type_folder = SOURCE_TYPE_FOLDERS.get(normalized_family, sanitize_windows_path_part(normalized_family))
    bucket_segment = source_bucket_segment(
        source_family=normalized_family,
        bucket_kind=bucket_kind,
        bucket_key=bucket_key,
        duty_date=duty_date,
        duty_shift=duty_shift,
        now=now,
    )
    month_segment = month_segment_from_value(bucket_segment, now=now)
    extension = str(suffix or "").strip() or ".xlsx"
    file_name = (
        f"{bucket_segment}--{type_folder}--{sanitize_windows_path_part(str(building or '').strip())}{extension}"
    )
    relative_path = Path(type_folder) / month_segment / bucket_segment / file_name
    return ArtifactPathInfo(
        type_folder=type_folder,
        month_segment=month_segment,
        bucket_segment=bucket_segment,
        file_name=file_name,
        relative_path=relative_path,
    )


def output_bucket_segment(
    *,
    output_type: str,
    duty_date: str = "",
    duty_shift: str = "",
    target_month: str = "",
    now: datetime | None = None,
) -> str:
    normalized_type = str(output_type or "").strip()
    if normalized_type in {OUTPUT_TYPE_HANDOVER_LOG, OUTPUT_TYPE_HANDOVER_CAPACITY}:
        duty_digits = "".join(ch for ch in str(duty_date or "").strip() if ch.isdigit())[:8]
        if len(duty_digits) != 8:
            duty_digits = (now or datetime.now()).strftime("%Y%m%d")
        return f"{duty_digits}--{handover_shift_text(duty_shift)}"
    month_text = "".join(ch for ch in str(target_month or "").strip() if ch.isdigit())[:6]
    if len(month_text) != 6:
        month_text = (now or datetime.now()).strftime("%Y%m")
    return f"{month_text}--月度"


def build_output_artifact_path(
    *,
    output_type: str,
    building: str,
    suffix: str = ".xlsx",
    duty_date: str = "",
    duty_shift: str = "",
    target_month: str = "",
    now: datetime | None = None,
) -> ArtifactPathInfo:
    normalized_type = str(output_type or "").strip()
    type_folder = OUTPUT_TYPE_LABELS.get(normalized_type, sanitize_windows_path_part(normalized_type))
    bucket_segment = output_bucket_segment(
        output_type=normalized_type,
        duty_date=duty_date,
        duty_shift=duty_shift,
        target_month=target_month,
        now=now,
    )
    month_segment = month_segment_from_value(bucket_segment, now=now)
    extension = str(suffix or "").strip() or ".xlsx"
    file_name = (
        f"{bucket_segment}--{type_folder}--{sanitize_windows_path_part(str(building or '').strip())}{extension}"
    )
    relative_path = Path(month_segment) / bucket_segment / file_name
    return ArtifactPathInfo(
        type_folder=type_folder,
        month_segment=month_segment,
        bucket_segment=bucket_segment,
        file_name=file_name,
        relative_path=relative_path,
    )


def with_index(path: Path, index: int) -> Path:
    if int(index or 0) <= 1:
        return path
    return path.with_name(f"{path.stem}_{int(index)}{path.suffix}")


def build_output_base_path(
    *,
    output_root: Path,
    output_type: str,
    building: str,
    suffix: str = ".xlsx",
    duty_date: str = "",
    duty_shift: str = "",
    target_month: str = "",
    now: datetime | None = None,
) -> Path:
    info = build_output_artifact_path(
        output_type=output_type,
        building=building,
        suffix=suffix,
        duty_date=duty_date,
        duty_shift=duty_shift,
        target_month=target_month,
        now=now,
    )
    return Path(output_root) / info.relative_path


def handover_log_output_patterns(building: str) -> tuple[re.Pattern[str], re.Pattern[str]]:
    escaped_building = re.escape(str(building or "").strip())
    legacy = re.compile(
        rf"^{escaped_building}_(?P<date>\d{{8}})_交接班日志(?:_(?P<seq>\d+))?\.xlsx$",
        re.IGNORECASE,
    )
    canonical = re.compile(
        rf"^(?P<date>\d{{8}})--(?P<shift>白班|夜班)--交接班日志--{escaped_building}(?:_(?P<seq>\d+))?\.xlsx$",
        re.IGNORECASE,
    )
    return legacy, canonical


def monthly_output_patterns(output_type: str, building: str) -> tuple[re.Pattern[str], re.Pattern[str]]:
    normalized_type = str(output_type or "").strip()
    label = OUTPUT_TYPE_LABELS.get(normalized_type, sanitize_windows_path_part(normalized_type))
    escaped_building = re.escape(str(building or "").strip())
    escaped_label = re.escape(label)
    legacy = re.compile(
        rf"^{escaped_building}_(?P<month>\d{{6}})_{escaped_label}(?:_(?P<seq>\d+))?\.xlsx$",
        re.IGNORECASE,
    )
    canonical = re.compile(
        rf"^(?P<month>\d{{6}})--月度--{escaped_label}--{escaped_building}(?:_(?P<seq>\d+))?\.xlsx$",
        re.IGNORECASE,
    )
    return legacy, canonical
