from openpyxl.utils import column_index_from_string, get_column_letter
from openpyxl.worksheet.page import PageMargins
from openpyxl.worksheet.pagebreak import ColBreak, RowBreak
from openpyxl.worksheet.properties import PageSetupProperties


HANDOVER_LOG_SHEET = "交接班日志"
HANDOVER_LOG_PRINT_MARGINS = {
    "left": 0.629861111111111,
    "right": 0.629861111111111,
    "top": 0.432638888888889,
    "bottom": 0.393055555555556,
    "header": 0.31496062992126,
    "footer": 0.31496062992126,
}


def _fill_printable_width(worksheet, last_row, last_column, character_width_pt) -> bool:
    margins = HANDOVER_LOG_PRINT_MARGINS
    width_limit = (297 / 25.4 - margins["left"] - margins["right"]) * 72
    height_limit = (210 / 25.4 - margins["top"] - margins["bottom"]) * 72
    default_height = worksheet.sheet_format.defaultRowHeight or 15
    height = 0.0
    for row in range(1, last_row + 1):
        dim = worksheet.row_dimensions.get(row)
        if dim is None or not dim.hidden:
            height += (dim.height if dim else None) or default_height

    default_width = worksheet.sheet_format.defaultColWidth or 8.43
    columns = {index: (get_column_letter(index), default_width, True) for index in range(1, last_column + 1)}
    for key, dim in worksheet.column_dimensions.items():
        start = dim.min or column_index_from_string(key)
        end = dim.max or start
        for index in range(start, min(end, last_column) + 1):
            columns[index] = (key, 0 if dim.hidden else dim.width, end <= last_column)
    current_width = sum(width for _, width, _ in columns.values())
    # A small allowance puts long sheets in width-limited single-page fit.
    needed_width = width_limit * max(1, height / height_limit) / character_width_pt
    if not current_width or current_width >= 0.99 * needed_width:
        return False
    flexible = {(key, width) for key, width, allowed in columns.values() if width and allowed}
    if not flexible:
        return False
    fixed_width = sum(width for _, width, allowed in columns.values() if not allowed)
    factor = min((needed_width * 1.02 - fixed_width) / (current_width - fixed_width),
                 255 / max(width for _, width in flexible))
    if factor <= 1:
        return False
    for key, width in flexible:
        worksheet.column_dimensions[key].width = width * factor
    return True


def apply_handover_log_print_profile(
    worksheet, *, last_row=None, last_column=None, fill_width=False, character_width_pt=6,
) -> bool:
    """Keep the legacy body margins and explicitly fit the final body on one page."""
    if last_column is None:
        # The stock template has a formatted but empty I column. Include any
        # real extension of the body rather than treating blank styles as data.
        last_column = max(
            8,
            max((cell.column for row in worksheet.iter_rows(min_col=9, max_col=max(9, worksheet.max_column))
                 for cell in row if cell.value not in (None, "")), default=0),
            max((area.max_col for area in worksheet.merged_cells.ranges), default=0),
        )
    last_row = last_row or worksheet.max_row
    print_area = f"A1:{get_column_letter(last_column)}{last_row}"
    setup = worksheet.page_setup
    options = worksheet.print_options
    old_area = worksheet.print_area
    worksheet.print_area = print_area
    properties = worksheet.sheet_properties.pageSetUpPr
    changed = (
        old_area != worksheet.print_area
        or worksheet.page_margins != PageMargins(**HANDOVER_LOG_PRINT_MARGINS)
        or properties is None or properties.fitToPage is not True
        or (setup.paperSize, setup.orientation, setup.fitToWidth, setup.fitToHeight, setup.scale)
        != (int(worksheet.PAPERSIZE_A4), "landscape", 1, 1, None)
        or options.horizontalCentered is not True or options.verticalCentered is not False
        or bool(worksheet.row_breaks.brk or worksheet.col_breaks.brk)
    )
    worksheet.page_margins = PageMargins(**HANDOVER_LOG_PRINT_MARGINS)
    if properties is None:
        properties = worksheet.sheet_properties.pageSetUpPr = PageSetupProperties()
    properties.fitToPage = True
    setup.paperSize = worksheet.PAPERSIZE_A4
    setup.orientation = "landscape"
    setup.fitToWidth = setup.fitToHeight = 1
    setup.scale = None
    options.horizontalCentered = True
    options.verticalCentered = False
    worksheet.row_breaks = RowBreak()
    worksheet.col_breaks = ColBreak()
    width_changed = fill_width and _fill_printable_width(worksheet, last_row, last_column, character_width_pt)
    return changed or width_changed
