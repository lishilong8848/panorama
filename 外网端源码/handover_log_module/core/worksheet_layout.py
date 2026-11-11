from __future__ import annotations

import math
from copy import copy
from unicodedata import east_asian_width

from openpyxl.cell.cell import MergedCell
from openpyxl.utils.cell import column_index_from_string
from openpyxl.worksheet.worksheet import Worksheet


def wrap_and_fit_rows(worksheet: Worksheet) -> None:
    """Enable wrapping and grow rows without changing the template's minimum heights."""
    merged = {(area.min_row, area.min_col): area for area in worksheet.merged_cells.ranges}
    widths = {}
    for dimension in worksheet.column_dimensions.values():
        start = dimension.min or column_index_from_string(dimension.index)
        for index in range(start, (dimension.max or start) + 1):
            widths[index] = 0 if dimension.hidden else dimension.width
    default_width = worksheet.sheet_format.defaultColWidth or 8.43
    default_height = worksheet.sheet_format.defaultRowHeight or 15

    def row_height(index: int) -> float:
        dimension = worksheet.row_dimensions.get(index)
        if dimension and dimension.hidden:
            return 0
        return (dimension.height if dimension else None) or default_height

    for row in worksheet.iter_rows():
        for cell in row:
            if isinstance(cell, MergedCell):
                continue
            if not cell.alignment.wrap_text or cell.alignment.shrink_to_fit:
                alignment = copy(cell.alignment)
                alignment.wrap_text = True
                alignment.shrink_to_fit = False
                cell.alignment = alignment
            if cell.value is None or cell.data_type == "f" or not row_height(cell.row):
                continue
            area = merged.get((cell.row, cell.column))
            max_col = area.max_col if area else cell.column
            max_row = area.max_row if area else cell.row
            width = sum(widths.get(index, default_width) for index in range(cell.column, max_col + 1))
            if width <= 0:
                continue
            font_size = float(cell.font.sz or 11)
            # Excel cannot auto-fit merged cells; estimate lines using the combined visible width.
            units = max(1, int(width * 0.85 * 11 / font_size) - 1)
            lines = sum(
                max(1, math.ceil(sum(2 if east_asian_width(char) in {"W", "F"} else 1 for char in line) / units))
                for line in str(cell.value).split("\n")
            )
            height = max(default_height, lines * font_size * 1.35 + 4)
            remaining = height - sum(row_height(index) for index in range(cell.row + 1, max_row + 1))
            if remaining > row_height(cell.row):
                worksheet.row_dimensions[cell.row].height = min(409, remaining)
