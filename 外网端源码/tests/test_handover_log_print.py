import pytest
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.pagebreak import Break

from app.modules.handover_review.api.routes import _normalize_log_download_workbook
from handover_log_module.core.handover_log_print import (
    HANDOVER_LOG_PRINT_MARGINS,
    HANDOVER_LOG_SHEET,
    apply_handover_log_print_profile,
)


def test_log_print_profile_follows_new_rows_and_real_columns():
    book = Workbook()
    sheet = book.active
    sheet["I59"].number_format = "@"
    sheet.print_area = "A1:H20"
    sheet.page_setup.scale = 79
    sheet.row_breaks.append(Break(id=25))
    sheet.col_breaks.append(Break(id=4))

    assert apply_handover_log_print_profile(sheet)
    assert sheet.print_area.endswith("!$A$1:$H$59")
    assert sheet.sheet_properties.pageSetUpPr.fitToPage is True
    assert (sheet.page_setup.fitToWidth, sheet.page_setup.fitToHeight) == (1, 1)
    assert sheet.page_setup.orientation == "landscape"
    assert sheet.page_setup.scale is None
    assert not sheet.row_breaks.brk and not sheet.col_breaks.brk
    assert apply_handover_log_print_profile(sheet) is False

    sheet["I71"] = "last-column value"
    assert apply_handover_log_print_profile(sheet)
    assert sheet.print_area.endswith("!$A$1:$I$71")


def test_historical_log_download_only_normalizes_the_body_copy(tmp_path):
    source = tmp_path / "original.xlsx"
    prepared = tmp_path / "download.xlsx"
    book = Workbook()
    cover = book.active
    cover.title = "封面"
    cover.page_margins.left = 0.9
    sheet = book.create_sheet(HANDOVER_LOG_SHEET)
    sheet["H77"] = "footer"
    sheet.column_dimensions["A"].width = 13
    sheet.column_dimensions["H"].width = 19
    sheet.row_dimensions[77].height = 31
    book.save(source)
    original = source.read_bytes()

    assert _normalize_log_download_workbook(source, sheet_name=HANDOVER_LOG_SHEET, save_to=lambda: prepared) == prepared
    assert source.read_bytes() == original
    with_prepared = load_workbook(prepared)
    try:
        output = with_prepared[HANDOVER_LOG_SHEET]
        assert output["H77"].value == "footer"
        assert output.print_area.endswith("!$A$1:$H$77")
        assert output.column_dimensions["H"].width > 19
        assert output.column_dimensions["H"].width / output.column_dimensions["A"].width == pytest.approx(19 / 13)
        assert output.row_dimensions[77].height == 31
        assert all(getattr(output.page_margins, key) == value for key, value in HANDOVER_LOG_PRINT_MARGINS.items())
        assert with_prepared["封面"].page_margins.left == 0.9
    finally:
        with_prepared.close()
    assert _normalize_log_download_workbook(prepared, sheet_name=HANDOVER_LOG_SHEET, save_to=lambda: None) == prepared
