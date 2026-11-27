"""Adapt an original BMS handover export to the existing external report contract."""
from datetime import datetime
from pathlib import Path
import re
from zipfile import ZipFile

from handover_log_module.repository.excel_reader import load_workbook_quietly
from app.modules.shared_bridge.service.report_api_transform import (
    ReportNoSamples, _header_time, is_usable_report_sample, normalize_report_rows,
)


HANDOVER_FAMILIES = {"handover_log_family", "handover_capacity_report_family"}


def handover_sample_times(header):
    columns = [(i, stamp) for i, value in enumerate(header) if (stamp := _header_time(value)) is not None]
    times = [stamp for _, stamp in columns]
    if ([i for i, _ in columns] != list(range(4, 4 + len(columns)))
            or not times or times != sorted(set(times))):
        raise ValueError("交接班源表 E 列起的采样时间必须连续排列且不能重复")
    return times


def handover_excel_payload(path: Path, *, family: str, building: str,
                           query_start="", query_end="", business_date=""):
    if family not in HANDOVER_FAMILIES or path.suffix.lower() != ".xlsx":
        raise ValueError("不是交接班原始 Excel 源表")
    with ZipFile(path) as archive:
        if sum(i.file_size for i in archive.infolist()) > 128 * 1024 * 1024:
            raise ValueError("交接班 Excel 过大")
    wb = load_workbook_quietly(path, read_only=True, data_only=True, keep_links=False)
    try:
        ws = wb.worksheets[0]
        # FineReport can declare A1:A1 even for a sheet containing hundreds of rows.
        ws.reset_dimensions()
        grid = []
        for row in ws.values:
            if len(grid) >= 10000 or len(row) > 128:
                raise ValueError("交接班 Excel 行列数超限")
            grid.append([v.isoformat(" ") if isinstance(v, datetime) else v for v in row])
        title = ws.title
    finally:
        wb.close()
    rows = normalize_report_rows(grid, {}, query_start=query_start, query_end=query_end, time_tolerance_sec=300)
    # The HTML adapter adds a separator; an original Excel already has one at row 3.
    if len(rows) > 3 and not any(v is not None and str(v).strip() for v in rows[3]):
        del rows[3]
    times = handover_sample_times(rows[1])
    floors = {floor for row in rows[3:] if len(row) > 1
              for floor in re.findall(r"([A-E]楼)", str(row[1] or ""))}
    if floors != {building}:
        raise ValueError(f"交接班 Excel 位置列楼栋不匹配：预期{building}，实际{sorted(floors)}")
    if not any(len(row) > 4 and row[3] and any(is_usable_report_sample(v, family)
               for v in row[4:4 + len(times)]) for row in rows[3:]):
        raise ReportNoSamples("交接班 Excel 没有有效测点读数")
    return {
        "schema_version": 1, "source_family": family, "building": building, "source_format": "xlsx",
        "business_date": business_date, "query_start": times[0].strftime("%Y-%m-%d %H:%M:%S"),
        "query_end": times[-1].strftime("%Y-%m-%d %H:%M:%S"), "scale_label": "5分钟",
        "requested_point_count": None, "point_ids_verified": False,
        "returned_point_count": sum(bool(len(row) > 3 and row[3]) for row in rows[3:]),
        "sheets": [{"name": title, "rows": rows}],
    }
