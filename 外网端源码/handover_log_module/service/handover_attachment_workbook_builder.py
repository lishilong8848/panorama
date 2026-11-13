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
from openpyxl.utils import get_column_letter
from openpyxl.utils.cell import coordinate_from_string, column_index_from_string
from openpyxl.utils.exceptions import CellCoordinatesException
from openpyxl.utils.units import pixels_to_EMU
from openpyxl.worksheet.properties import PageSetupProperties
from PIL import Image as PILImage

from handover_log_module.core.worksheet_layout import wrap_and_fit_rows
from handover_log_module.core.station_h_operating_cells import STATION_H_CARRY_READING_CELLS

STATION_H_DEFAULT_SHEET_TITLE = "H楼"
STATION_H_TEMPLATE_FILE_NAME = "H楼交接班日志空模板.xlsx"
STATION_H_DUTY_FOCUS_IMAGE_CELL = "J2"
STATION_H_DUTY_FOCUS_IMAGE_MAX_WIDTH_PX = 900
_IMAGE_CELL_PADDING_PX = 2

# H楼交接班日志打印预设：纵向 A4、缩放成单页。打印区域按最终行数动态锁定日志正文，
# 分类插行后仍覆盖到页脚。模板本身没有任何打印设置，导致每次打印都要手工重设；
# 这里在生成时烘焙进去，下载/上传/打印的 H_handover.xlsx 都自带正确打印配置。
STATION_H_PRINT_LAST_COLUMN = "H"
STATION_H_PRINT_DEFAULT_LAST_ROW = 50
STATION_H_PRINT_ORIENTATION = "portrait"
STATION_H_PRINT_PAPER_SIZE = 9  # openpyxl Worksheet.PAPERSIZE_A4


def _apply_station_h_print_setup(worksheet: Any, *, last_row: int = STATION_H_PRINT_DEFAULT_LAST_ROW) -> None:
    """给 H楼日志正文工作表烘焙固定打印配置（不改动列宽/合并区域；行高由 wrap 逻辑负责）。"""
    final_row = max(1, int(last_row or STATION_H_PRINT_DEFAULT_LAST_ROW))
    worksheet.print_area = f"A1:{STATION_H_PRINT_LAST_COLUMN}{final_row}"
    worksheet.sheet_properties.pageSetUpPr = PageSetupProperties(fitToPage=True)
    worksheet.page_setup.orientation = STATION_H_PRINT_ORIENTATION
    worksheet.page_setup.paperSize = STATION_H_PRINT_PAPER_SIZE
    worksheet.page_setup.fitToWidth = 1
    worksheet.page_setup.fitToHeight = 1

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


def _write_cell(worksheet: Any, coordinate: str, value: Any) -> None:
    """写单个单元格；字符串强制 data_type='s'，避免 openpyxl 误判类型。"""
    cell = worksheet[coordinate]
    cell.value = value
    if isinstance(value, str):
        cell.data_type = "s"


def _row_of(coordinate: str) -> int:
    return _parse_cell(coordinate)[0]


def _render_station_h_sections(
    worksheet: Any,
    section_rows: Mapping[str, list[dict[str, Any]]],
    *,
    attention_executor: str = "",
) -> tuple[int, int, int]:
    """按记录数在主表**插行/删行**渲染各分类，使「新增一条 = 主表新增一行」。

    返回 ``(total_delta, first_title_row, footer_start_row)``（均为模板坐标）：
    * ``total_delta``：相对模板净增/减的行数，供页脚与打印区顺延；
    * ``first_title_row``：第一个分类标题行（其上是头部固定区，不顺延）；
    * ``footer_start_row``：页脚起始行（模板坐标，其下整体顺延 total_delta）。

    规则：
    * ``section_rows`` 里出现的分类＝已保存，渲染其全部记录（ desired=max(1,记录数) ），
      记录数多于模板行则插行、少于则删行；未出现的分类保留模板默认，不插不删。
    * 培训管理同样按记录逐行渲染（不再挤进行31一个单元格）。
    * 「三、注意事项交接」执行人（H 列）为空时自动填 ``attention_executor``（当班人员第一个）。
    * 每条记录行首列 A 写序号（1..N）。
    """
    from handover_log_module.repository.section_writer import (
        _delete_rows_like_excel,
        _insert_rows_like_excel,
    )
    from handover_log_module.service.station_h_section_schema import (
        STATION_H_ATTENTION_EXECUTOR_COLUMN,
        STATION_H_ATTENTION_SECTION,
        station_h_build_section_specs,
    )

    noop_log = lambda _message: None  # noqa: E731
    specs = station_h_build_section_specs()
    if not specs:
        return 0, 1, int(worksheet.max_row or 1) + 1

    rows_in = section_rows if isinstance(section_rows, Mapping) else {}
    first_title_row = int(specs[0]["title_row"])
    footer_start_row = max(int(spec["rows"][-1]) for spec in specs) + 1

    # 1) 每个分类的目标行数（模板坐标）与增减量
    plan: list[dict[str, Any]] = []
    for spec in specs:
        template_rows = [int(r) for r in spec["rows"]]
        start_row, end_row = template_rows[0], template_rows[-1]
        baseline = end_row - start_row + 1
        if spec["name"] in rows_in:
            records = [rec for rec in rows_in[spec["name"]] if isinstance(rec, dict)]
            desired = max(1, len(records))
        else:
            records = None
            desired = baseline  # 保留模板默认，不插不删
        plan.append({
            "spec": spec, "records": records, "start_row": start_row, "end_row": end_row,
            "baseline": baseline, "desired": desired, "delta": desired - baseline,
        })

    # 2) 自底向上插行/删行（用模板坐标；下方已处理，上方坐标不受影响）
    for entry in sorted(plan, key=lambda item: item["start_row"], reverse=True):
        delta = entry["delta"]
        if delta > 0:
            _insert_rows_like_excel(
                worksheet, insert_at=entry["end_row"] + 1, amount=delta,
                template_row=entry["end_row"], emit_log=noop_log,
            )
        elif delta < 0:
            _delete_rows_like_excel(
                worksheet, delete_at=entry["start_row"] + entry["desired"], amount=-delta,
                emit_log=noop_log,
            )

    # 3) 计算每个分类插删后的最终起始行，并写记录
    total_delta = sum(entry["delta"] for entry in plan)
    for index, entry in enumerate(plan):
        offset_above = sum(other["delta"] for other in plan[:index])
        final_start = entry["start_row"] + offset_above
        spec = entry["spec"]
        columns = spec["columns"]
        is_attention = spec["name"] == STATION_H_ATTENTION_SECTION
        if entry["records"] is None:
            # 未保存：保留模板内容；仅注意事项兜底填执行人。
            if is_attention and str(attention_executor or "").strip():
                for row in range(final_start, final_start + entry["desired"]):
                    coord = f"{STATION_H_ATTENTION_EXECUTOR_COLUMN}{row}"
                    existing = str(worksheet[coord].value or "").strip()
                    if not existing:
                        _write_cell(worksheet, coord, str(attention_executor).strip())
            continue
        executor = str(attention_executor or "").strip()
        for i in range(entry["desired"]):
            row = final_start + i
            record = entry["records"][i] if i < len(entry["records"]) else {}
            _write_cell(worksheet, f"A{row}", i + 1)
            for column in columns:
                key = column["key"]
                value = record.get(key, "")
                value = "" if value is None else str(value)
                if is_attention and key == STATION_H_ATTENTION_EXECUTOR_COLUMN and not value.strip() and executor:
                    value = executor
                _write_cell(worksheet, f"{key}{row}", value)
    return total_delta, first_title_row, footer_start_row


def build_station_h_workbook(
    template_path: str | Path,
    output_path: str | Path,
    cell_values: Mapping[str, Any],
    image_path: Optional[str | Path] = None,
    *,
    section_rows: Optional[Mapping[str, list[dict[str, Any]]]] = None,
    attention_executor: str = "",
) -> Path:
    """基于 H楼模板生成填充后的工作簿，返回最终 ``output_path``。

    ``cell_values`` 为扁平绝对单元格字典（头部固定区 + 页脚区）。当提供 ``section_rows``
    时，各业务分类（含培训管理、注意事项）按记录数在主表**逐行渲染**（新增一条即新增一行），
    其后所有页脚锚点与打印区域自动顺延；``cell_values`` 中落在分类区的坐标被忽略（以
    ``section_rows`` 为准）。``attention_executor`` 为注意事项执行人自动填充值（当班人员第一个）。
    ``image_path`` 为独立值班关注点图片，插入 ``J2``。失败时旧目标文件保持不变。
    """
    template = _resolve_template_path(template_path)
    output = _resolve_output_path(output_path)
    if template == output:
        raise HandoverAttachmentWorkbookError("输出路径不能与模板路径相同")
    if not isinstance(cell_values, Mapping):
        raise HandoverAttachmentWorkbookError(
            f"cell_values 必须是 Mapping，收到 {type(cell_values).__name__}"
        )
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
            template_max_row = int(worksheet.max_row or 1)
            if section_rows is None:
                # 兼容旧调用：无分类数据时按扁平字典整体写入（不插行）。
                _write_values(worksheet, cell_values)
                total_delta, first_title_row, footer_start_row = 0, template_max_row + 1, template_max_row + 1
            else:
                flat = dict(cell_values)
                total_delta, first_title_row, footer_start_row = _render_station_h_sections(
                    worksheet, section_rows, attention_executor=attention_executor,
                )
                # 头部固定区（分类标题行之上）按模板坐标写；页脚区顺延 total_delta。
                for coordinate, value in flat.items():
                    row = _row_of(coordinate)
                    if row < first_title_row:
                        _write_cell(worksheet, coordinate, value)
                    elif row >= footer_start_row:
                        column_letter = "".join(ch for ch in coordinate if ch.isalpha())
                        _write_cell(worksheet, f"{column_letter}{row + total_delta}", value)
                    # 分类区坐标忽略：以 section_rows 渲染结果为准
            wrap_and_fit_rows(worksheet)
            _apply_station_h_print_setup(
                worksheet,
                last_row=max(template_max_row, STATION_H_PRINT_DEFAULT_LAST_ROW) + total_delta,
            )
            if image is not None:
                _insert_duty_focus_image(worksheet, *image)
            return _atomic_save_workbook(workbook, output)
        finally:
            workbook.close()


__all__ = [
    "HandoverAttachmentWorkbookError",
    "STATION_H_CELL_KEYS",
    "STATION_H_DEFAULT_SHEET_TITLE",
    "STATION_H_DUTY_FOCUS_IMAGE_CELL",
    "STATION_H_DUTY_FOCUS_IMAGE_MAX_WIDTH_PX",
    "STATION_H_PRINT_LAST_COLUMN",
    "STATION_H_PRINT_DEFAULT_LAST_ROW",
    "STATION_H_PRINT_ORIENTATION",
    "STATION_H_PRINT_PAPER_SIZE",
    "STATION_H_TEMPLATE_FILE_NAME",
    "build_station_h_workbook",
    "default_station_h_template_path",
]
