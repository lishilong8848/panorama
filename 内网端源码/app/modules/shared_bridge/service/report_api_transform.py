from __future__ import annotations

import math
import re
from datetime import datetime, timedelta
from typing import Any


_TIME_RE = re.compile(r"^\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}(?::\d{2})?$")
_MISSING_SAMPLES = {"", "-", "--", "—", "nan", "inf", "infinity", "null", "none", "n/a", "无数据", "暂无数据"}
_STATUS_SAMPLES = {"制冷", "预冷", "板换", "停机", "运行", "停止", "合闸", "分闸", "开启", "关闭"}
_BRANCH_FAMILIES = {"branch_power_family", "branch_current_family", "branch_switch_family"}

# C楼 CMDB device 25_0_220, model 0_2370. Key by the full legacy ID, never by suffix alone.
_C_UPS_LABELS = {
    "25_0_220_1_601_0": "电池_正极电压_V",
    "25_0_220_1_624_0": "B_输出电压_V",
    "25_0_220_1_1002_0": "UPS_输出总有功功率_KW",
}


def _header_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not _TIME_RE.fullmatch(text):
        return None
    try:
        return datetime.fromisoformat(text.replace("/", "-"))
    except ValueError:
        return None


def is_usable_report_sample(value: Any, family: str) -> bool:
    """Whether a time-column cell is evidence of a real report sample (including zero)."""
    if value is None or isinstance(value, bool):
        return False
    text = str(value).strip()
    if family == "branch_switch_family" and text in {"合闸", "分闸"}:
        return True
    if family in {"handover_log_family", "handover_capacity_report_family",
                  "air_conditioner_temperature_humidity_family", "chiller_mode_switch_family"} and text in _STATUS_SAMPLES:
        return True
    if family == "air_conditioner_temperature_humidity_family":
        text = re.sub(r"\s*(?:℃|°C|%)$", "", text, flags=re.IGNORECASE)
    if "," in text:
        if not re.fullmatch(r"[+-]?\d{1,3}(?:,\d{3})+(?:\.\d+)?", text):
            return False
        text = text.replace(",", "")
    try:
        return "_" not in text and math.isfinite(float(text))
    except ValueError:
        return False


def _can_fill_sample_zero(family: str, point: str, metric: str) -> bool:
    if family in _BRANCH_FAMILIES or family == "building_full_cabinet_power_family":
        # A zero in the existing branch switch contract means unknown, not open or closed.
        return True
    if family == "air_conditioner_temperature_humidity_family":
        return "温度" in metric or "湿度" in metric
    if family not in {"handover_log_family", "handover_capacity_report_family", "chiller_mode_switch_family"}:
        return False
    parts = point.rsplit("_", 3)
    return not (len(parts) == 4 and parts[1] == "2" or re.search(
        r"状态|模式|启停|开机|关机|开关|告警|报警|故障|电度|电量|累计|总量", metric))


def _fill_sample_gaps(body, row_ids, header_index, columns, family):
    branch = family in _BRANCH_FAMILIES
    metric_col, device_col = (2, 1) if branch else (3, 2)
    if columns[0] <= metric_col:
        return []
    samples = []
    device = ""
    for index, raw in enumerate(body):
        if len(raw) <= metric_col or not str(raw[metric_col] or "").strip():
            continue
        if raw[device_col]:
            device = str(raw[device_col]).strip()
        metric = str(raw[metric_col]).strip()
        point = row_ids.get(header_index + 1 + index, "")
        if branch and not all(str(raw[i] or "").strip() for i in range(3)):
            continue
        gaps = [i for i in columns if i >= len(raw) or raw[i] is None
                or str(raw[i]).strip().lower().lstrip("+-") in _MISSING_SAMPLES]
        if not gaps:
            continue
        fill_zero = _can_fill_sample_zero(family, point, metric)
        if not branch and any(i < len(raw) and str(raw[i]).strip() in _STATUS_SAMPLES for i in columns):
            fill_zero = False
        row = [*raw, *([None] * max(0, columns[-1] + 1 - len(raw)))]
        for column in gaps:
            row[column] = 0 if fill_zero else None
        body[index] = row
        samples.append({"point_id": point, "device": device, "metric": metric, "count": len(gaps),
                        "row_number": index + 4, "first_sample_missing": columns[0] in gaps,
                        "zero_filled": fill_zero})
    return samples


def normalize_report_rows(
    grid: list[list[Any]],
    row_ids: dict[int, str],
    *,
    expected_point_ids: list[str] | None = None,
    zero_fill_building: str = "",
    source_family: str = "",
    filled_ids: list[str] | None = None,
    missing_samples: list[dict] | None = None,
    query_start: str = "",
    query_end: str = "",
    time_tolerance_sec: int = 0,
) -> list[list[Any]]:
    if not isinstance(grid, list) or not grid or any(not isinstance(row, list) for row in grid):
        raise ValueError("报表接口未返回有效数据")
    candidates: list[tuple[int, int, list[datetime]]] = []
    for index, row in enumerate(grid[:20]):
        times = [parsed for value in row if (parsed := _header_time(value)) is not None]
        if times:
            candidates.append((len(times), index, times))
    if not candidates:
        raise ValueError("报表接口缺少时间表头")
    _, header_index, timestamps = max(candidates, key=lambda item: (item[0], item[1]))
    if len(set(timestamps)) != len(timestamps):
        raise ValueError("报表接口时间表头重复")
    if query_start and query_end:
        try:
            start = datetime.strptime(query_start, "%Y-%m-%d %H:%M:%S")
            end = datetime.strptime(query_end, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise ValueError("报表接口查询时间无效") from None
        tolerance = timedelta(seconds=max(0, int(time_tolerance_sec)))
        if end < start or any(value < start - tolerance or value > end + tolerance for value in timestamps):
            raise ValueError("报表接口返回时间超出查询窗口")
    body = grid[header_index + 1:]
    if not body:
        raise ValueError("报表接口没有数据行")
    real_row_count = len(body)
    header = grid[header_index]
    columns = [i for i, cell in enumerate(header) if _header_time(cell) is not None]
    has_real_sample = any(
        (expected_point_ids is None or r in row_ids) and i < len(row)
        and is_usable_report_sample(row[i], source_family)
        for r, row in enumerate(body, header_index + 1) for i in columns
    ) if missing_samples is not None or expected_point_ids is not None or source_family in {
        "handover_log_family", "handover_capacity_report_family"
    } else False
    if source_family in {"handover_log_family", "handover_capacity_report_family"} and not has_real_sample:
        raise ValueError("交接班报表接口没有可用的实际采样")
    actual = [value for index, value in row_ids.items() if index > header_index and value]
    if expected_point_ids is not None:
        expected = list(expected_point_ids)
        if not expected or len(expected) != len(set(expected)):
            raise ValueError("报表配置的测点列表无效")
        if any(not header_index < index < len(grid) or not value for index, value in row_ids.items()):
            raise ValueError("报表接口测点ID不在有效数据行")
        actual_set = set(actual)
        missing = [point for point in expected if point not in actual_set]
        if (not actual or len(actual) != len(actual_set) or actual_set.difference(expected)
                or (missing and not zero_fill_building)):
            raise ValueError(f"报表接口返回测点不完整或存在重复：配置={len(expected)}，返回={len(actual)}，"
                             f"缺少={len(missing)}，重复={len(actual)-len(set(actual))}，"
                             f"额外={len(set(actual)-set(expected))}，缺少ID示例={','.join(missing[:5])}")
        if missing:
            if source_family in _BRANCH_FAMILIES:
                # Their consumer joins by row and checks room, rack and branch/PDU, not hidden ID.
                raise ValueError("支路报表缺少测点且无法确认业务行键，不允许补行")
            capacity = source_family == "handover_capacity_report_family"
            if columns[0] < 4:
                raise ValueError("报表接口没有可识别的测点标签列")
            if any(r not in row_ids and any(v is not None and str(v).strip() for v in row)
                   for r, row in enumerate(body, header_index + 1)):
                raise ValueError("报表接口存在无ID的非空数据行，无法确认缺少的测点")
            if not has_real_sample:
                raise ValueError("报表没有可用的实际采样，不允许全表补零")
            if len(body) + len(missing) + 3 > 50000 or (len(body) + len(missing)) * len(header) > 4_000_000:
                raise ValueError("补零后报表超过行数或单元格限制")
            present_labels = set()
            c_capacity = capacity and zero_fill_building == "C楼"
            if c_capacity:
                device = ""
                for row in body:
                    if len(row) >= 4:
                        device = str(row[2] or device).strip()
                        present_labels.add((device, str(row[3] or "").strip()))
            # Append rather than disturb existing row positions or inherit a merged device label.
            for point in missing:
                row = [None] * len(header)
                if capacity:
                    row[1:4] = [zero_fill_building, "未返回测点", point]
                else:
                    # Business readers match or forward-fill B:D. An ID alone is not a device name.
                    row[0] = point
                label = _C_UPS_LABELS.get(point) if c_capacity else None
                if label and ("C-218-UPS-101", label) not in present_labels:
                    row[1:4] = ["南通阿里保税A区C楼/C楼/二层/变电所M2 C-218", "C-218-UPS-101", label]
                for i in columns:
                    row[i] = 0
                body.append(row)
            if filled_ids is not None:
                filled_ids.extend(missing)
    if missing_samples is not None:
        actual_rows = body[:real_row_count]
        gaps = _fill_sample_gaps(actual_rows, row_ids, header_index, columns, source_family)
        if gaps and not has_real_sample:
            raise ValueError("报表没有可用的实际采样，不允许全表补零")
        if gaps:
            body[:len(actual_rows)] = actual_rows
            missing_samples.extend(gaps)
    if not actual and not any(any(str(cell or "").strip() for cell in row) for row in body):
        raise ValueError("报表接口没有有效数据行")
    return [[], list(grid[header_index]), [], *[list(row) for row in body]]
