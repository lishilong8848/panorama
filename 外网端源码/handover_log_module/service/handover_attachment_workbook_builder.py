"""H楼交接班日志附件工作簿构建器。

复制 ``H楼交接班日志空模板.xlsx``，把 ``cell_values`` 的值写入对应单元格，并按需
把独立值班关注点图片放在 ``J2``；模板的合并区域、样式、行列尺寸与打印设置保留。

* 写入合并区域内坐标时落到该区域左上角，同一锚点出现不同值
  直接报错，不静默覆盖。
* 图片按原始字节写入（不转码），等比缩放且宽度不超过 900px，锚定 ``J2``；浮动
  图片不改变行列尺寸，也不扩大打印范围。
* 先写同目录临时文件，zip 校验通过后 ``os.replace`` 覆盖；任何失败都清理临时
  文件，旧目标文件保持原样。
"""

from __future__ import annotations

import os
import tempfile
import uuid
import warnings
import zipfile
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import openpyxl
from openpyxl.drawing.image import Image as OpenpyxlImage
from openpyxl.drawing.spreadsheet_drawing import AnchorMarker, OneCellAnchor
from openpyxl.drawing.xdr import XDRPositiveSize2D
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from openpyxl.utils.cell import coordinate_from_string, column_index_from_string
from openpyxl.utils.exceptions import CellCoordinatesException
from openpyxl.utils.units import pixels_to_EMU
from PIL import Image as PILImage

from handover_log_module.core.worksheet_layout import wrap_and_fit_rows
from handover_log_module.core.station_h_operating_cells import STATION_H_CARRY_READING_CELLS

STATION_H_DEFAULT_SHEET_TITLE = "H楼"
STATION_H_TEMPLATE_FILE_NAME = "H楼交接班日志空模板.xlsx"
STATION_H_DUTY_FOCUS_IMAGE_CELL = "J2"
STATION_H_DUTY_FOCUS_IMAGE_MAX_WIDTH_PX = 900
_IMAGE_CELL_PADDING_PX = 2

# 与 HandoverCloudSheetSyncService.STATION_H_ALLOWED_CELLS 保持一致；另允许培训行 B31:G31。
STATION_H_CELL_KEYS = frozenset(
    {
        "B2", "F2", "C3", "D3", "G3", "H3", "B4", "F4", "B6", "G6",
        "B12", "D12", "F12", "H12", "B15", "B16", "H15", "H16",
        "H40", "H41", "H42", "H45", "H46", "H47", "H48",
        "B31", "D31", "E31", "F31", "G31",
    }
) | frozenset(STATION_H_CARRY_READING_CELLS)


class HandoverAttachmentWorkbookError(RuntimeError):
    """H楼交接班日志附件工作簿构建失败。"""


def _source_root() -> Path:
    # 本文件位于 <source_root>/handover_log_module/service/ 下。
    return Path(__file__).resolve().parents[2]


def default_station_h_template_path() -> Path:
    return _source_root() / STATION_H_TEMPLATE_FILE_NAME


@lru_cache(maxsize=8)
def _station_h_operating_defaults(path: Path, _mtime_ns: int) -> dict[str, Any]:
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        worksheet = workbook[STATION_H_DEFAULT_SHEET_TITLE] if STATION_H_DEFAULT_SHEET_TITLE in workbook else workbook.active
        return {cell: worksheet[cell].value for cell in STATION_H_CARRY_READING_CELLS}
    finally:
        workbook.close()


def read_station_h_operating_defaults(template_path: str | Path) -> dict[str, Any]:
    path = _resolve_template_path(template_path)
    return dict(_station_h_operating_defaults(path, path.stat().st_mtime_ns))

def _resolve_template_path(template_path: str | Path) -> Path:
    path = Path(str(template_path or "").strip())
    path = (path if path.is_absolute() else _source_root() / path).resolve()
    if not str(template_path or "").strip() or not path.is_file():
        raise HandoverAttachmentWorkbookError(f"H楼模板不存在: {path}")
    return path


def _resolve_output_path(output_path: str | Path) -> Path:
    raw = str(output_path or "").strip()
    if not raw:
        raise HandoverAttachmentWorkbookError("输出路径为空")
    # 不 resolve 软链接，避免替换时绕过原目标；只做绝对化与标准化。
    return Path(os.path.abspath(raw))


def _parse_cell(coordinate: str) -> tuple[int, int]:
    """``"b2"`` -> ``(2, 2)``（行, 列）。"""
    try:
        column, row = coordinate_from_string(str(coordinate).strip().upper())
        return row, column_index_from_string(column)
    except (CellCoordinatesException, ValueError, TypeError):
        raise HandoverAttachmentWorkbookError(f"单元格坐标无效: {coordinate!r}") from None


def _write_values(worksheet: Any, cell_values: Mapping[str, Any]) -> None:
    """写值；多个输入落到同一合并锚点时值必须一致，否则报错。"""
    if not isinstance(cell_values, Mapping):
        raise HandoverAttachmentWorkbookError(
            f"cell_values 必须是 Mapping，收到 {type(cell_values).__name__}"
        )
    # {(row, col): 合并区域左上角}，覆盖合并区域内的每一格。
    anchors: Dict[tuple[int, int], tuple[int, int]] = {}
    for merged in worksheet.merged_cells.ranges:
        for row in range(merged.min_row, merged.max_row + 1):
            for col in range(merged.min_col, merged.max_col + 1):
                anchors[(row, col)] = (merged.min_row, merged.min_col)
    planned: Dict[tuple[int, int], tuple[str, Any]] = {}
    for coordinate, value in cell_values.items():
        value = "" if value is None else value
        key = _parse_cell(coordinate)
        anchor = anchors.get(key, key)
        previous = planned.setdefault(anchor, (str(coordinate), value))
        if previous[1] != value:
            raise HandoverAttachmentWorkbookError(
                f"合并区域左上角 {get_column_letter(anchor[1])}{anchor[0]} 收到冲突的值: "
                f"{previous[0]}={previous[1]!r} / {coordinate}={value!r}"
            )
    for (row, column), (_, value) in planned.items():
        # 只改 .value：不新建样式、不触碰合并区域与行列尺寸。
        cell = worksheet.cell(row=row, column=column)
        cell.value = value
        if isinstance(value, str):
            cell.data_type = "s"


def _read_image(image_path: str | Path) -> tuple[bytes, tuple[int, int]]:
    path = Path(image_path)
    payload = path.read_bytes() if path.is_file() else b""
    try:
        with PILImage.open(BytesIO(payload)) as source:
            size = (int(source.size[0]), int(source.size[1]))
            source.verify()
    except Exception as exc:  # noqa: BLE001
        raise HandoverAttachmentWorkbookError(f"图片缺失或无法解析: {path} ({exc})") from exc
    if min(size) <= 0:
        raise HandoverAttachmentWorkbookError(f"图片尺寸无效: {path}")
    return payload, size


def _insert_duty_focus_image(worksheet: Any, payload: bytes, size: tuple[int, int]) -> None:
    """原始字节挂到 ``J2`` 左上角；等比缩放，宽度上限 900px。"""
    width, height = size
    scale = min(STATION_H_DUTY_FOCUS_IMAGE_MAX_WIDTH_PX / width, 1.0)
    row, column = _parse_cell(STATION_H_DUTY_FOCUS_IMAGE_CELL)
    image = OpenpyxlImage(BytesIO(payload))
    image.ref = BytesIO(payload)  # 保存时原样写入 xl/media/*，不重新编码
    image.anchor = OneCellAnchor(
        _from=AnchorMarker(
            col=column - 1, colOff=pixels_to_EMU(_IMAGE_CELL_PADDING_PX),
            row=row - 1, rowOff=pixels_to_EMU(_IMAGE_CELL_PADDING_PX),
        ),
        ext=XDRPositiveSize2D(
            pixels_to_EMU(max(1, round(width * scale))),
            pixels_to_EMU(max(1, round(height * scale))),
        ),
    )
    worksheet.add_image(image)


def _atomic_save_workbook(workbook: Any, output_path: Path) -> Path:
    """同目录临时文件 + zip 校验 + ``os.replace``；失败时旧文件保持原样。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_fd, temp_name = tempfile.mkstemp(
        prefix=f".{output_path.stem}.",
        suffix=f".{uuid.uuid4().hex[:8]}.tmp{output_path.suffix or '.xlsx'}",
        dir=str(output_path.parent),
    )
    os.close(temp_fd)
    temp_path = Path(temp_name)
    try:
        workbook.save(str(temp_path))
        try:
            with zipfile.ZipFile(temp_path) as archive:
                broken = "[Content_Types].xml" not in archive.namelist() or archive.testzip() is not None
        except zipfile.BadZipFile:
            broken = True
        if broken:
            raise HandoverAttachmentWorkbookError("临时工作簿 zip 结构校验失败")
        os.replace(str(temp_path), str(output_path))
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise
    return output_path


def build_station_h_workbook(
    template_path: str | Path,
    output_path: str | Path,
    cell_values: Mapping[str, Any],
    image_path: Optional[str | Path] = None,
    *,
    section_overflow: Optional[list[dict[str, Any]]] = None,
) -> Path:
    """基于 H楼模板生成填充后的工作簿，返回最终 ``output_path``。

    ``cell_values`` 形如 ``{"B2": 2026年11月9日, "F2": "白班"}``；``image_path`` 为
    独立值班关注点图片，插入 ``J2``。失败时旧目标文件保持不变。
    """
    template = _resolve_template_path(template_path)
    output = _resolve_output_path(output_path)
    if template == output:
        raise HandoverAttachmentWorkbookError("输出路径不能与模板路径相同")
    image = _read_image(image_path) if str(image_path or "").strip() else None

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        workbook = openpyxl.load_workbook(str(template), data_only=False)
        try:
            if STATION_H_DEFAULT_SHEET_TITLE not in workbook.sheetnames:
                if len(workbook.sheetnames) > 1:
                    raise HandoverAttachmentWorkbookError(
                        f"H楼模板缺少工作表 {STATION_H_DEFAULT_SHEET_TITLE}: {workbook.sheetnames}"
                    )
                workbook.worksheets[0].title = STATION_H_DEFAULT_SHEET_TITLE
            worksheet = workbook[STATION_H_DEFAULT_SHEET_TITLE]
            _write_values(worksheet, cell_values)
            wrap_and_fit_rows(worksheet)
            if image is not None:
                _insert_duty_focus_image(worksheet, *image)
            if section_overflow:
                continuation = workbook.create_sheet("分类续表")
                continuation.column_dimensions["A"].width = 8
                for section in section_overflow:
                    continuation.append([section["name"]])
                    continuation.cell(continuation.max_row, 1).font = Font(bold=True)
                    columns = section["columns"]
                    continuation.append(["序号", *(column["label"] or column["key"] for column in columns)])
                    for index, cells in section["rows"]:
                        continuation.append([index, *(cells.get(column["key"], "") for column in columns)])
                        for cell in continuation[continuation.max_row]:
                            if isinstance(cell.value, str):
                                cell.data_type = "s"
                    for index in range(2, len(columns) + 2):
                        continuation.column_dimensions[get_column_letter(index)].width = 28
                    continuation.append([""])
                wrap_and_fit_rows(continuation)
            return _atomic_save_workbook(workbook, output)
        finally:
            workbook.close()


__all__ = [
    "HandoverAttachmentWorkbookError",
    "STATION_H_CELL_KEYS",
    "STATION_H_DEFAULT_SHEET_TITLE",
    "STATION_H_DUTY_FOCUS_IMAGE_CELL",
    "STATION_H_DUTY_FOCUS_IMAGE_MAX_WIDTH_PX",
    "STATION_H_TEMPLATE_FILE_NAME",
    "build_station_h_workbook",
    "default_station_h_template_path",
]
