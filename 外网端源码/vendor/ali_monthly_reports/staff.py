from __future__ import annotations

from itertools import zip_longest
from pathlib import Path
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter

from .utils import cell_text, date_text, first_present, is_truthy_cell, is_value_in_month, month_bounds


def format_staff_sheet(ws, widths: list[int]) -> None:
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center")
    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(vertical="center", wrap_text=True)
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    for index, width in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(index)].width = width


def write_staff_roster_workbook(
    active_records: list[dict[str, Any]],
    all_records: list[dict[str, Any]],
    month: str,
    output_path: Path,
) -> dict[str, int]:
    month_start, month_end = month_bounds(month)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    active_ws = wb.active
    active_ws.title = "在职人员基础信息"
    active_ws.append(["人员姓名", "岗位", "入职时间"])
    for record in active_records:
        active_ws.append(
            [
                cell_text(first_present(record, "姓名", "人员姓名", "员工姓名")),
                cell_text(first_present(record, "岗位")),
                date_text(first_present(record, "入职日期", "入职时间")),
            ]
        )

    new_hire_names = [
        name
        for record in all_records
        if is_value_in_month(first_present(record, "入职日期", "入职时间"), month_start, month_end)
        for name in [cell_text(first_present(record, "姓名", "人员姓名", "员工姓名"))]
        if name
    ]
    leaver_names = [
        name
        for record in all_records
        if is_truthy_cell(first_present(record, "离职/异动情况", "离职情况"))
        and is_value_in_month(first_present(record, "离职/异动日期", "离职日期"), month_start, month_end)
        for name in [cell_text(first_present(record, "姓名", "人员姓名", "员工姓名"))]
        if name
    ]

    changes_ws = wb.create_sheet("本月人员变动")
    changes_ws.append(["本月新增人员姓名", "离职人员姓名"])
    for new_hire_name, leaver_name in zip_longest(new_hire_names, leaver_names, fillvalue=""):
        changes_ws.append([new_hire_name, leaver_name])

    format_staff_sheet(active_ws, [18, 24, 14])
    format_staff_sheet(changes_ws, [24, 18])
    wb.save(output_path)
    wb.close()
    return {
        "active_count": len(active_records),
        "new_hire_count": len(new_hire_names),
        "leaver_count": len(leaver_names),
    }
