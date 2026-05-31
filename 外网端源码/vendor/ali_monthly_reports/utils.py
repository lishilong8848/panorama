from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


def require_config_values(config: Any, keys: list[str]) -> None:
    missing = [key for key in keys if not str(getattr(config, key, "")).strip()]
    if missing:
        raise ValueError(f"缺少配置: {', '.join(missing)}")


def sanitize_download_filename(filename: str, fallback_name: str) -> str:
    raw_name = Path(filename).name.strip() or fallback_name
    safe_name = "".join("_" if ord(char) < 32 or char in '<>:"/\\|?*' else char for char in raw_name).strip(" .")
    return safe_name or fallback_name


def make_unique_path(directory: Path, filename: str) -> Path:
    candidate = directory / filename
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return directory / f"{stem}_{timestamp}{suffix}"


def generated_file_payload(file_path: Path) -> dict[str, str]:
    return {
        "name": file_path.name,
        "path": str(file_path),
        "size": f"{file_path.stat().st_size / 1024:.1f}KB",
    }


def remove_url_query_param(url: str, param_name: str) -> str:
    parsed = urlparse(url)
    query = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True) if key != param_name]
    return urlunparse(parsed._replace(query=urlencode(query)))


def month_bounds(month: str) -> tuple[datetime, datetime]:
    year, mon = map(int, month.split("-"))
    return datetime(year, mon, 1), datetime(year + (mon == 12), 1 if mon == 12 else mon + 1, 1)


def parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw /= 1000
        return datetime.fromtimestamp(raw)
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def first_present(record: dict[str, Any], *names: str) -> Any:
    for name in names:
        value = record.get(name)
        if value not in (None, ""):
            return value
    return ""


def cell_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (list, tuple, set)):
        parts = [cell_text(item) for item in value]
        return "\n".join(part for part in parts if part)
    if isinstance(value, dict):
        for key in ("text", "name", "en_name", "value"):
            if key in value:
                return cell_text(value[key])
        parts = [cell_text(item) for item in value.values()]
        return " ".join(part for part in parts if part)
    return str(value).strip()


def date_text(value: Any) -> str:
    parsed = parse_dt(value)
    if parsed:
        return parsed.strftime("%Y-%m-%d")
    return cell_text(value)


def is_value_in_month(value: Any, month_start: datetime, month_end: datetime) -> bool:
    parsed = parse_dt(value)
    return bool(parsed and month_start <= parsed < month_end)


def is_truthy_cell(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, (list, tuple, set)):
        return any(is_truthy_cell(item) for item in value)
    return cell_text(value).lower() in {"1", "true", "yes", "y", "是", "离职", "已离职", "异动"}


def filter_records_by_year_month(records: list[dict[str, Any]], year: str, month_number: int) -> list[dict[str, Any]]:
    month_num = f"{month_number:02d}"
    filtered_records: list[dict[str, Any]] = []
    for record in records:
        fields = record.get("fields", {})
        record_month = str(fields.get("月份", ""))
        record_year = str(fields.get("年度", ""))
        month_matches = record_month in {month_num, str(month_number)}
        year_matches = year in record_year or record_year in {"", "None"}
        if month_matches and year_matches:
            filtered_records.append(record)
    return filtered_records
