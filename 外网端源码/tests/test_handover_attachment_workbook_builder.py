from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path

import openpyxl
import pytest
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from PIL import Image as PILImage

from handover_log_module.service import handover_attachment_workbook_builder as builder

SHEET_TITLE = "H楼"
EMU_PER_PX = 9525


def _write_mock_template(path: Path) -> Path:
    """构造与真实 H楼模板同构的本地小工作簿（带合并区域与格式）。"""
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.title = SHEET_TITLE
    worksheet["A1"] = "南通数据中心H楼交接班日志"
    worksheet.merge_cells("A1:H1")
    worksheet["A1"].font = Font(name="宋体", size=16, bold=True)
    worksheet["A1"].alignment = Alignment(horizontal="center", vertical="center")
    worksheet["A1"].fill = PatternFill("solid", fgColor="1F4E79")
    for merged in ("B2:D2", "F2:H2", "B4:D4"):
        worksheet.merge_cells(merged)
    thin = Side(style="thin", color="7F7F7F")
    for coordinate in ("B2", "F2", "B4", "B12", "H12"):
        cell = worksheet[coordinate]
        cell.font = Font(name="宋体", size=9, bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = Border(left=thin, right=thin, top=thin, bottom=thin)
        cell.fill = PatternFill("solid", fgColor="FFF2CC")
    worksheet["C2"].fill = PatternFill("solid", fgColor="DDDDDD")
    worksheet["B12"] = 0
    worksheet["H12"] = 0
    worksheet["B12"].number_format = "0.00"
    worksheet.row_dimensions[2].height = 24
    worksheet.column_dimensions["B"].width = 17.45
    worksheet.column_dimensions["J"].width = 8.43
    worksheet.print_area = "A1:H50"
    workbook.save(path)
    workbook.close()
    return path


def _make_image(path: Path, size=(320, 120)) -> bytes:
    PILImage.new("RGB", size, (180, 20, 20)).save(path, format="PNG")
    return path.read_bytes()


def _snapshot(path: Path) -> dict:
    workbook = openpyxl.load_workbook(path)
    try:
        worksheet = workbook[SHEET_TITLE]
        return {
            "merges": sorted(str(m) for m in worksheet.merged_cells.ranges),
            "styles": {
                c: (
                    worksheet[c].font.b, worksheet[c].font.sz, worksheet[c].fill.fgColor.rgb,
                    worksheet[c].alignment.horizontal, worksheet[c].border.left.style,
                    worksheet[c].number_format,
                )
                for c in ("A1", "B2", "F2", "B4", "B12", "H12")
            },
            "row_heights": {r: worksheet.row_dimensions[r].height for r in (1, 2)},
            "column_widths": {c: worksheet.column_dimensions[c].width for c in ("B", "J")},
            "print_area": str(worksheet.print_area),
        }
    finally:
        workbook.close()


def _read_zip_part(path: Path, prefix: str) -> list[bytes]:
    with zipfile.ZipFile(path) as archive:
        return [archive.read(n) for n in sorted(archive.namelist()) if n.startswith(prefix)]


def _image_ext(path: Path) -> tuple[int, int]:
    xml = b"".join(_read_zip_part(path, "xl/drawings/drawing")).decode("utf-8")
    anchor = re.search(r"<(?:\w+:)?from>(.*?)</(?:\w+:)?from>", xml, re.S)
    assert anchor is not None, xml
    assert re.search(r"<(?:\w+:)?col>(\d+)</", anchor.group(1)).group(1) == "9"  # J
    assert re.search(r"<(?:\w+:)?row>(\d+)</", anchor.group(1)).group(1) == "1"  # 2
    ext = re.search(r'<(?:\w+:)?ext cx="(\d+)" cy="(\d+)"', xml)
    assert ext is not None, xml
    return int(ext.group(1)) // EMU_PER_PX, int(ext.group(2)) // EMU_PER_PX


@pytest.fixture()
def template(tmp_path: Path) -> Path:
    return _write_mock_template(tmp_path / "H楼交接班日志空模板.xlsx")


def test_values_written_and_style_sheet_preserved(template: Path, tmp_path: Path) -> None:
    output = tmp_path / "out" / "H楼交接班日志.xlsx"
    before = _snapshot(template)

    result = builder.build_station_h_workbook(
        template, output,
        {"B2": "2026年11月9日", "F2": "白班", "B6": "26.5", "b16": "10:00", "H12": 9.75, "G6": None},
    )

    assert result == Path(str(output.resolve())) and output.is_file()
    after = _snapshot(output)
    assert {k: after[k] for k in ("merges", "styles", "row_heights", "column_widths", "print_area")} == {
        k: before[k] for k in ("merges", "styles", "row_heights", "column_widths", "print_area")
    }
    workbook = openpyxl.load_workbook(output)
    try:
        sheet = workbook[SHEET_TITLE]
        assert sheet["B2"].value == "2026年11月9日"
        assert sheet["F2"].value == "白班"
        assert sheet["B16"].value == "10:00"
        assert sheet["H12"].value == 9.75
        assert sheet["G6"].value in (None, "")
    finally:
        workbook.close()


def test_merged_cell_value_lands_on_top_left(template: Path, tmp_path: Path) -> None:
    output = tmp_path / "merged.xlsx"
    builder.build_station_h_workbook(template, output, {"C2": "date", "H2": "夜班", "D4": "常白岗"})

    workbook = openpyxl.load_workbook(output)
    try:
        sheet = workbook[SHEET_TITLE]
        assert (sheet["B2"].value, sheet["F2"].value, sheet["B4"].value) == ("date", "夜班", "常白岗")
        # 合并区域内部单元格保持只读 MergedCell，合并结构未被破坏。
        assert isinstance(sheet["C2"], openpyxl.cell.cell.MergedCell)
        assert sheet["C2"].value is None and sheet["H2"].value is None
    finally:
        workbook.close()
    assert sorted(str(m) for m in sheet.merged_cells.ranges) == _snapshot(template)["merges"]


def test_same_anchor_with_different_values_raises(template: Path, tmp_path: Path) -> None:
    output = tmp_path / "conflict.xlsx"
    output.write_bytes(b"old-official-content")

    with pytest.raises(builder.HandoverAttachmentWorkbookError, match="合并区域左上角 B2"):
        builder.build_station_h_workbook(template, output, {"B2": "a", "C2": "b"})

    # 冲突在保存前抛出：旧文件不被覆盖，也没有临时文件残留。
    assert output.read_bytes() == b"old-official-content"
    assert list(tmp_path.glob(".conflict*")) == []


def test_same_anchor_with_equal_values_is_not_a_conflict(template: Path, tmp_path: Path) -> None:
    output = tmp_path / "same.xlsx"
    builder.build_station_h_workbook(template, output, {"B2": "同值", "D2": "同值"})
    workbook = openpyxl.load_workbook(output)
    try:
        assert workbook[SHEET_TITLE]["B2"].value == "同值"
    finally:
        workbook.close()


def test_only_mapping_accepted(template: Path, tmp_path: Path) -> None:
    for bad in (None, [("B2", "x")], {1: "x"}, {"B": "x"}):
        with pytest.raises(builder.HandoverAttachmentWorkbookError):
            builder.build_station_h_workbook(template, tmp_path / "bad.xlsx", bad)  # type: ignore[arg-type]
    assert not (tmp_path / "bad.xlsx").exists()
    assert not hasattr(builder, "build_station_h_workbook_from_dict")


def test_image_kept_raw_anchored_j2_and_readable_size(template: Path, tmp_path: Path) -> None:
    output = tmp_path / "with_image.xlsx"
    original = _make_image(tmp_path / "focus.png", size=(1200, 400))

    builder.build_station_h_workbook(template, output, {"B2": "2026年11月9日"}, image_path=tmp_path / "focus.png")

    media = _read_zip_part(output, "xl/media/")
    assert len(media) == 1 and media[0] == original  # 原始字节，未重新编码
    with PILImage.open(io.BytesIO(media[0])) as saved:
        assert saved.size == (1200, 400)
    # 等比缩放到最大宽 900px，而不是被压进 J 列（约 64px）单格。
    assert _image_ext(output) == (900, 300)
    assert _read_zip_part(output, "xl/media/")  # 图片确实写入包内
    # 图片为浮动对象：不改变行列尺寸与打印范围。
    after = _snapshot(output)
    assert after["column_widths"]["J"] == _snapshot(template)["column_widths"]["J"]
    assert after["print_area"] == _snapshot(template)["print_area"]


def test_small_image_not_upscaled(template: Path, tmp_path: Path) -> None:
    output = tmp_path / "small.xlsx"
    _make_image(tmp_path / "small.png", size=(200, 100))
    builder.build_station_h_workbook(template, output, {}, image_path=tmp_path / "small.png")
    assert _image_ext(output) == (200, 100)


def test_image_omitted_or_invalid(template: Path, tmp_path: Path) -> None:
    plain = tmp_path / "no_image.xlsx"
    builder.build_station_h_workbook(template, plain, {"B2": "x"}, image_path=None)
    assert _read_zip_part(plain, "xl/media/") == []

    guarded = tmp_path / "guarded.xlsx"
    guarded.write_bytes(b"previous-official-file")
    (tmp_path / "bad.png").write_bytes(b"not an image")
    with pytest.raises(builder.HandoverAttachmentWorkbookError):
        builder.build_station_h_workbook(template, guarded, {"B2": "x"}, image_path=tmp_path / "bad.png")
    assert guarded.read_bytes() == b"previous-official-file"
    assert list(tmp_path.glob(".guarded*")) == []


def test_existing_output_replaced_atomically(template: Path, tmp_path: Path) -> None:
    output = tmp_path / "official.xlsx"
    builder.build_station_h_workbook(template, output, {"B2": "第一版"})
    first = output.read_bytes()

    builder.build_station_h_workbook(template, output, {"B2": "第二版"})

    assert output.read_bytes() != first
    workbook = openpyxl.load_workbook(output)
    try:
        assert workbook[SHEET_TITLE]["B2"].value == "第二版"
    finally:
        workbook.close()
    assert list(tmp_path.glob(".official*")) == []


def test_save_failure_or_corrupt_temp_preserves_old_file(template: Path, tmp_path: Path, monkeypatch) -> None:
    output = tmp_path / "protected.xlsx"
    output.write_bytes(b"old-official-content")

    def _boom(self, filename):
        raise OSError("disk full")

    monkeypatch.setattr(openpyxl.workbook.workbook.Workbook, "save", _boom)
    with pytest.raises(OSError):
        builder.build_station_h_workbook(template, output, {"B2": "x"})
    assert output.read_bytes() == b"old-official-content"
    assert list(tmp_path.glob(".protected*")) == []

    def _garbage(self, filename):
        Path(filename).write_bytes(b"PK\x03\x04-broken")

    monkeypatch.setattr(openpyxl.workbook.workbook.Workbook, "save", _garbage)
    with pytest.raises(builder.HandoverAttachmentWorkbookError):
        builder.build_station_h_workbook(template, output, {"B2": "x"})
    assert output.read_bytes() == b"old-official-content"
    assert list(tmp_path.glob(".protected*")) == []


def test_template_guards_and_single_sheet_rename(template: Path, tmp_path: Path) -> None:
    before = template.read_bytes()
    with pytest.raises(builder.HandoverAttachmentWorkbookError):
        builder.build_station_h_workbook(template, template, {"B2": "x"})
    assert template.read_bytes() == before

    missing = tmp_path / "never.xlsx"
    with pytest.raises(builder.HandoverAttachmentWorkbookError):
        builder.build_station_h_workbook(tmp_path / "missing.xlsx", missing, {"B2": "x"})
    assert not missing.exists()

    plain = tmp_path / "plain.xlsx"
    workbook = openpyxl.Workbook()
    workbook.active.merge_cells("B2:C2")
    workbook.active["B2"] = "anchor"
    workbook.save(plain)
    workbook.close()
    output = tmp_path / "renamed.xlsx"
    builder.build_station_h_workbook(plain, output, {"C2": "via-merge"})
    workbook = openpyxl.load_workbook(output)
    try:
        assert workbook.sheetnames == [SHEET_TITLE]
        assert workbook[SHEET_TITLE]["B2"].value == "via-merge"
        assert [str(m) for m in workbook[SHEET_TITLE].merged_cells.ranges] == ["B2:C2"]
    finally:
        workbook.close()


def test_no_network_or_feishu_imports() -> None:
    source = Path(builder.__file__).read_text(encoding="utf-8").lower()
    for token in ("requests", "httpx", "urllib.request", "feishu", "lark_oapi", "socket", "app_secret"):
        assert token not in source, f"模块不应引用 {token}"
    assert "openpyxl" in source
