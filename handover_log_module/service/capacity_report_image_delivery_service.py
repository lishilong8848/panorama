from __future__ import annotations

import hashlib
import math
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from openpyxl import load_workbook
from openpyxl.cell.cell import MergedCell
from openpyxl.utils import get_column_letter, range_boundaries
from PIL import Image, ImageDraw, ImageFont

from app.shared.utils.atomic_file import atomic_write_bytes, validate_image_file
from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root
from handover_log_module.service.review_link_delivery_service import ReviewLinkDeliveryService
from handover_log_module.service.review_session_service import ReviewSessionService


_CAPACITY_IMAGE_DELIVERY_LOCK = threading.RLock()
_CAPACITY_IMAGE_DELIVERY_RUNNING_STATUSES = {"sending"}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def _safe_name(value: str) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[^\w\u4e00-\u9fff.-]+", "_", text, flags=re.UNICODE)
    return text.strip("._") or "unknown"


class CapacityReportImageRenderer:
    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg if isinstance(handover_cfg, dict) else {}

    def _runtime_root(self) -> Path:
        return resolve_runtime_state_root(
            runtime_config={"paths": self.handover_cfg.get("_global_paths", {})},
            app_dir=Path(__file__).resolve().parents[2],
        )

    def _configured_sheet_name(self) -> str:
        capacity_cfg = self.handover_cfg.get("capacity_report", {})
        if not isinstance(capacity_cfg, dict):
            return ""
        template_cfg = capacity_cfg.get("template", {})
        if not isinstance(template_cfg, dict):
            return ""
        return _text(template_cfg.get("sheet_name"))

    def _cache_path(
        self,
        *,
        source_path: Path,
        building: str,
        duty_date: str,
        duty_shift: str,
        session_id: str,
    ) -> Path:
        stat = source_path.stat()
        source_sig = hashlib.sha1(
            f"{source_path.resolve()}|{stat.st_size}|{stat.st_mtime_ns}".encode("utf-8", errors="ignore")
        ).hexdigest()[:12]
        session_sig = hashlib.sha1(str(session_id or "").encode("utf-8", errors="ignore")).hexdigest()[:10]
        batch_dir = self._runtime_root() / "handover" / "capacity_report_images" / _safe_name(f"{duty_date}--{duty_shift}")
        return batch_dir / f"{_safe_name(building)}_{session_sig}_{source_sig}.png"

    def render_to_image(
        self,
        *,
        source_file: str,
        building: str,
        duty_date: str,
        duty_shift: str,
        session_id: str,
    ) -> Path:
        source_path = Path(str(source_file or "").strip())
        if not source_path.exists() or not source_path.is_file():
            raise FileNotFoundError(f"交接班容量报表文件不存在: {source_path}")
        output_path = self._cache_path(
            source_path=source_path,
            building=building,
            duty_date=duty_date,
            duty_shift=duty_shift,
            session_id=session_id,
        )
        if output_path.exists() and output_path.is_file():
            return output_path

        workbook = load_workbook(source_path, data_only=True)
        try:
            configured_sheet = self._configured_sheet_name()
            sheet = workbook[configured_sheet] if configured_sheet and configured_sheet in workbook.sheetnames else workbook[workbook.sheetnames[0]]
            image = self._render_sheet(sheet)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            buffer = self._image_bytes(image)
            atomic_write_bytes(output_path, buffer, validator=validate_image_file, temp_suffix=".tmp")
            return output_path
        finally:
            workbook.close()

    @staticmethod
    def _image_bytes(image: Image.Image) -> bytes:
        from io import BytesIO

        buffer = BytesIO()
        image.save(buffer, format="PNG")
        return buffer.getvalue()

    @staticmethod
    def _print_area_bounds(sheet) -> Tuple[int, int, int, int] | None:
        raw = getattr(sheet, "print_area", None)
        if not raw:
            return None
        areas = raw if isinstance(raw, (list, tuple)) else str(raw).split(",")
        for area in areas:
            ref = str(area or "").strip()
            if not ref:
                continue
            if "!" in ref:
                ref = ref.split("!", 1)[1]
            ref = ref.replace("'", "").replace("$", "")
            try:
                min_col, min_row, max_col, max_row = range_boundaries(ref)
            except Exception:
                continue
            if min_col and min_row and max_col and max_row:
                return min_col, min_row, max_col, max_row
        return None

    @staticmethod
    def _content_bounds(sheet) -> Tuple[int, int, int, int]:
        min_row = sheet.max_row or 1
        min_col = sheet.max_column or 1
        max_row = 1
        max_col = 1
        found = False
        for row in sheet.iter_rows():
            for cell in row:
                if isinstance(cell, MergedCell):
                    continue
                if _text(cell.value):
                    found = True
                    min_row = min(min_row, cell.row)
                    min_col = min(min_col, cell.column)
                    max_row = max(max_row, cell.row)
                    max_col = max(max_col, cell.column)
        for merged in sheet.merged_cells.ranges:
            min_col = min(min_col, merged.min_col)
            min_row = min(min_row, merged.min_row)
            max_col = max(max_col, merged.max_col)
            max_row = max(max_row, merged.max_row)
            found = True
        if not found:
            return 1, 1, 1, 1
        return min_col, min_row, max_col, max_row

    @classmethod
    def _render_bounds(cls, sheet) -> Tuple[int, int, int, int]:
        return cls._print_area_bounds(sheet) or cls._content_bounds(sheet)

    @staticmethod
    def _column_width_px(sheet, column_index: int) -> int:
        letter = get_column_letter(column_index)
        dimension = sheet.column_dimensions.get(letter)
        if dimension is not None and bool(getattr(dimension, "hidden", False)):
            return 0
        width = getattr(dimension, "width", None) if dimension is not None else None
        try:
            value = float(width if width is not None else 8.43)
        except Exception:
            value = 8.43
        return max(28, min(220, int(round(value * 7 + 8))))

    @staticmethod
    def _row_height_px(sheet, row_index: int) -> int:
        dimension = sheet.row_dimensions.get(row_index)
        if dimension is not None and bool(getattr(dimension, "hidden", False)):
            return 0
        height = getattr(dimension, "height", None) if dimension is not None else None
        try:
            points = float(height if height is not None else 15)
        except Exception:
            points = 15
        return max(20, min(120, int(round(points * 96 / 72 + 6))))

    @staticmethod
    def _rgb(color: Any, default: Tuple[int, int, int] | None = None) -> Tuple[int, int, int] | None:
        if color is None:
            return default
        rgb = str(getattr(color, "rgb", "") or "").strip()
        if rgb and len(rgb) in {6, 8}:
            rgb = rgb[-6:]
            try:
                return int(rgb[0:2], 16), int(rgb[2:4], 16), int(rgb[4:6], 16)
            except Exception:
                return default
        return default

    @classmethod
    def _fill_color(cls, cell) -> Tuple[int, int, int]:
        fill = getattr(cell, "fill", None)
        if fill is None or str(getattr(fill, "fill_type", "") or "").lower() in {"", "none"}:
            return 255, 255, 255
        color = cls._rgb(getattr(fill, "fgColor", None))
        if color is None or color == (0, 0, 0):
            return 255, 255, 255
        return color

    @classmethod
    def _font_color(cls, cell) -> Tuple[int, int, int]:
        color = cls._rgb(getattr(getattr(cell, "font", None), "color", None))
        return color or (0, 0, 0)

    @staticmethod
    def _font_path(bold: bool) -> str:
        candidates = [
            Path("C:/Windows/Fonts/msyhbd.ttc" if bold else "C:/Windows/Fonts/msyh.ttc"),
            Path("C:/Windows/Fonts/simhei.ttf"),
            Path("C:/Windows/Fonts/simsun.ttc"),
        ]
        for path in candidates:
            if path.exists():
                return str(path)
        return ""

    @classmethod
    def _font(cls, cell) -> ImageFont.ImageFont:
        raw_size = getattr(getattr(cell, "font", None), "sz", None)
        try:
            size = max(10, min(36, int(round(float(raw_size or 11) * 96 / 72))))
        except Exception:
            size = 15
        bold = bool(getattr(getattr(cell, "font", None), "bold", False))
        font_path = cls._font_path(bold)
        if font_path:
            try:
                return ImageFont.truetype(font_path, size=size)
            except Exception:
                pass
        return ImageFont.load_default()

    @staticmethod
    def _measure(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> Tuple[int, int]:
        if not text:
            return 0, 0
        box = draw.textbbox((0, 0), text, font=font)
        return max(0, box[2] - box[0]), max(0, box[3] - box[1])

    @classmethod
    def _wrap_text(cls, draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, width: int) -> List[str]:
        max_width = max(1, width)
        output: List[str] = []
        for raw_line in str(text or "").splitlines() or [""]:
            line = ""
            for char in raw_line:
                candidate = f"{line}{char}"
                if line and cls._measure(draw, candidate, font)[0] > max_width:
                    output.append(line)
                    line = char
                else:
                    line = candidate
            output.append(line)
        return output

    @staticmethod
    def _alignment(cell) -> Tuple[str, str, bool]:
        alignment = getattr(cell, "alignment", None)
        horizontal = str(getattr(alignment, "horizontal", "") or "center").lower()
        vertical = str(getattr(alignment, "vertical", "") or "center").lower()
        wrap = bool(getattr(alignment, "wrap_text", False))
        return horizontal, vertical, wrap

    @staticmethod
    def _merged_bounds(sheet) -> Dict[Tuple[int, int], Tuple[int, int, int, int]]:
        mapping: Dict[Tuple[int, int], Tuple[int, int, int, int]] = {}
        for merged in sheet.merged_cells.ranges:
            bounds = (merged.min_col, merged.min_row, merged.max_col, merged.max_row)
            for row in range(merged.min_row, merged.max_row + 1):
                for col in range(merged.min_col, merged.max_col + 1):
                    mapping[(row, col)] = bounds
        return mapping

    @staticmethod
    def _border_color(side) -> Tuple[int, int, int]:
        return CapacityReportImageRenderer._rgb(getattr(side, "color", None), (0, 0, 0)) or (0, 0, 0)

    @staticmethod
    def _has_border(side) -> bool:
        return bool(side is not None and str(getattr(side, "style", "") or "").strip())

    @classmethod
    def _draw_border(cls, draw: ImageDraw.ImageDraw, rect: Tuple[int, int, int, int], cell) -> None:
        border = getattr(cell, "border", None)
        if border is None:
            draw.rectangle(rect, outline=(210, 210, 210), width=1)
            return
        left, top, right, bottom = rect
        if cls._has_border(border.left):
            draw.line((left, top, left, bottom), fill=cls._border_color(border.left), width=1)
        if cls._has_border(border.right):
            draw.line((right, top, right, bottom), fill=cls._border_color(border.right), width=1)
        if cls._has_border(border.top):
            draw.line((left, top, right, top), fill=cls._border_color(border.top), width=1)
        if cls._has_border(border.bottom):
            draw.line((left, bottom, right, bottom), fill=cls._border_color(border.bottom), width=1)

    @classmethod
    def _render_sheet(cls, sheet) -> Image.Image:
        min_col, min_row, max_col, max_row = cls._render_bounds(sheet)
        col_widths = {col: cls._column_width_px(sheet, col) for col in range(min_col, max_col + 1)}
        row_heights = {row: cls._row_height_px(sheet, row) for row in range(min_row, max_row + 1)}
        x_offsets: Dict[int, int] = {}
        y_offsets: Dict[int, int] = {}
        x = 0
        for col in range(min_col, max_col + 1):
            x_offsets[col] = x
            x += col_widths[col]
        y = 0
        for row in range(min_row, max_row + 1):
            y_offsets[row] = y
            y += row_heights[row]
        width = max(1, x)
        height = max(1, y)
        image = Image.new("RGB", (width, height), (255, 255, 255))
        draw = ImageDraw.Draw(image)
        merged_map = cls._merged_bounds(sheet)
        rendered_merged: set[Tuple[int, int, int, int]] = set()

        for row in range(min_row, max_row + 1):
            for col in range(min_col, max_col + 1):
                bounds = merged_map.get((row, col))
                if bounds:
                    if bounds in rendered_merged:
                        continue
                    rendered_merged.add(bounds)
                    start_col, start_row, end_col, end_row = bounds
                    if start_col < min_col or start_row < min_row:
                        continue
                    cell = sheet.cell(start_row, start_col)
                else:
                    start_col = end_col = col
                    start_row = end_row = row
                    cell = sheet.cell(row, col)
                left = x_offsets[start_col]
                top = y_offsets[start_row]
                right = x_offsets[end_col] + col_widths[end_col]
                bottom = y_offsets[end_row] + row_heights[end_row]
                rect = (left, top, max(left + 1, right), max(top + 1, bottom))
                draw.rectangle(rect, fill=cls._fill_color(cell))
                cls._draw_border(draw, rect, cell)
                value = _text(cell.value)
                if not value:
                    continue
                font = cls._font(cell)
                font_color = cls._font_color(cell)
                horizontal, vertical, wrap = cls._alignment(cell)
                padding_x = 5
                padding_y = 3
                text_width = max(1, rect[2] - rect[0] - padding_x * 2)
                lines = cls._wrap_text(draw, value, font, text_width) if wrap or "\n" in value else [value]
                line_heights = [max(1, cls._measure(draw, line, font)[1]) for line in lines]
                line_gap = 3
                total_text_height = sum(line_heights) + max(0, len(lines) - 1) * line_gap
                available_height = max(1, rect[3] - rect[1] - padding_y * 2)
                if total_text_height > available_height and lines:
                    max_lines = max(1, math.floor((available_height + line_gap) / (max(line_heights) + line_gap)))
                    lines = lines[:max_lines]
                    if len(lines) == max_lines:
                        lines[-1] = lines[-1].rstrip()
                    line_heights = [max(1, cls._measure(draw, line, font)[1]) for line in lines]
                    total_text_height = sum(line_heights) + max(0, len(lines) - 1) * line_gap
                if vertical == "top":
                    text_y = rect[1] + padding_y
                elif vertical == "bottom":
                    text_y = rect[3] - padding_y - total_text_height
                else:
                    text_y = rect[1] + max(0, (rect[3] - rect[1] - total_text_height) // 2)
                for line, line_height in zip(lines, line_heights):
                    measured_width = cls._measure(draw, line, font)[0]
                    if horizontal in {"right", "distributed"}:
                        text_x = rect[2] - padding_x - measured_width
                    elif horizontal in {"left", "general"}:
                        text_x = rect[0] + padding_x
                    else:
                        text_x = rect[0] + max(0, (rect[2] - rect[0] - measured_width) // 2)
                    draw.text((text_x, text_y), line, font=font, fill=font_color)
                    text_y += line_height + line_gap
        return image


class CapacityReportImageDeliveryService:
    def __init__(self, handover_cfg: Dict[str, Any], *, config_path: str | Path | None = None) -> None:
        self.handover_cfg = handover_cfg if isinstance(handover_cfg, dict) else {}
        self.config_path = Path(config_path) if config_path else None
        self._review_service = ReviewSessionService(self.handover_cfg)
        self._link_service = ReviewLinkDeliveryService(self.handover_cfg, config_path=self.config_path)
        self._renderer = CapacityReportImageRenderer(self.handover_cfg)

    def validate_preflight(self, session: Dict[str, Any], *, building: str) -> None:
        self._validate_session(session, building=building)
        self._validate_not_running(session)
        recipients = self._link_service._recipients_for_building(building)
        if not recipients:
            raise ValueError("当前楼未配置启用的审核链接接收人")

    @staticmethod
    def _validate_not_running(session: Dict[str, Any]) -> None:
        delivery = session.get("capacity_image_delivery", {}) if isinstance(session, dict) else {}
        if not isinstance(delivery, dict):
            return
        status = str(delivery.get("status", "") or "").strip().lower()
        if status in _CAPACITY_IMAGE_DELIVERY_RUNNING_STATUSES:
            raise ValueError("容量表图片正在发送中，请等待发送完成")

    @staticmethod
    def _validate_session(session: Dict[str, Any], *, building: str) -> None:
        if not isinstance(session, dict) or not str(session.get("session_id", "") or "").strip():
            raise ValueError("未找到交接班审核会话")
        if str(session.get("building", "") or "").strip() != str(building or "").strip():
            raise ValueError("审核会话楼栋不匹配")
        output_file_text = str(session.get("capacity_output_file", "") or "").strip()
        if not output_file_text:
            raise ValueError("当前交接班容量报表尚未生成")
        output_file = Path(output_file_text)
        if not output_file.exists() or not output_file.is_file():
            raise FileNotFoundError("交接班容量报表文件不存在，请重新生成")
        capacity_sync = session.get("capacity_sync", {}) if isinstance(session.get("capacity_sync", {}), dict) else {}
        if str(capacity_sync.get("status", "") or "").strip().lower() != "ready":
            raise ValueError(str(capacity_sync.get("error", "") or "").strip() or "容量报表待补写完成后才能发送")

    def begin_delivery(self, session: Dict[str, Any], *, building: str, source: str = "manual") -> Dict[str, Any]:
        session_id = str(session.get("session_id", "") or "").strip() if isinstance(session, dict) else ""
        if not session_id:
            raise ValueError("未找到交接班审核会话")
        with _CAPACITY_IMAGE_DELIVERY_LOCK:
            latest_session = self._review_service.get_session_by_id(session_id)
            if not isinstance(latest_session, dict):
                latest_session = session
            self.validate_preflight(latest_session, building=building)
            delivery = {
                "status": "sending",
                "last_attempt_at": _now_text(),
                "last_sent_at": "",
                "error": "",
                "image_path": "",
                "image_key": "",
                "successful_recipients": [],
                "failed_recipients": [],
                "source": str(source or "manual").strip().lower() or "manual",
            }
            updated = self._review_service.update_capacity_image_delivery(
                session_id=session_id,
                capacity_image_delivery=delivery,
            )
            return dict(updated.get("capacity_image_delivery", delivery))

    def mark_failed(self, *, session_id: str, error: str, source: str = "manual") -> Dict[str, Any]:
        delivery = {
            "status": "failed",
            "last_attempt_at": _now_text(),
            "last_sent_at": "",
            "error": str(error or "容量表图片发送失败").strip() or "容量表图片发送失败",
            "image_path": "",
            "image_key": "",
            "successful_recipients": [],
            "failed_recipients": [],
            "source": str(source or "manual").strip().lower() or "manual",
        }
        updated = self._review_service.update_capacity_image_delivery(
            session_id=str(session_id or "").strip(),
            capacity_image_delivery=delivery,
        )
        return dict(updated.get("capacity_image_delivery", delivery))

    def send_for_session(
        self,
        session: Dict[str, Any],
        *,
        building: str,
        source: str = "manual",
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        normalized_session = dict(session) if isinstance(session, dict) else {}
        building_text = str(building or normalized_session.get("building", "") or "").strip()
        session_id = str(normalized_session.get("session_id", "") or "").strip()
        self._validate_session(normalized_session, building=building_text)
        recipients = self._link_service._recipients_for_building(building_text)
        if not recipients:
            raise ValueError("当前楼未配置启用的审核链接接收人")

        attempt_at = _now_text()
        self._review_service.update_capacity_image_delivery(
            session_id=session_id,
            capacity_image_delivery={
                "status": "sending",
                "last_attempt_at": attempt_at,
                "last_sent_at": "",
                "error": "",
                "image_path": "",
                "image_key": "",
                "successful_recipients": [],
                "failed_recipients": [],
                "source": str(source or "manual").strip().lower() or "manual",
            },
        )
        image_path: Path | None = None
        image_key = ""
        successful_recipients: List[str] = []
        failed_recipients: List[Dict[str, str]] = []
        try:
            image_path = self._renderer.render_to_image(
                source_file=str(normalized_session.get("capacity_output_file", "") or "").strip(),
                building=building_text,
                duty_date=str(normalized_session.get("duty_date", "") or "").strip(),
                duty_shift=str(normalized_session.get("duty_shift", "") or "").strip(),
                session_id=session_id,
            )
            emit_log(
                "[交接班][容量表图片发送] 图片已生成 "
                f"building={building_text}, session_id={session_id}, image={image_path}"
            )

            client = self._link_service._build_feishu_client()
            image_upload = client.upload_image(str(image_path))
            image_key = str(image_upload.get("image_key", "") or "").strip()
            for recipient in recipients:
                open_id = str(recipient.get("open_id", "") or "").strip()
                note = str(recipient.get("note", "") or "").strip()
                receive_id_type = self._link_service._resolve_effective_receive_id_type(open_id, "open_id")
                try:
                    client.send_image_message(
                        receive_id=open_id,
                        receive_id_type=receive_id_type,
                        image_key=image_key,
                    )
                    successful_recipients.append(open_id)
                    emit_log(
                        "[交接班][容量表图片发送] 发送成功 "
                        f"building={building_text}, session_id={session_id}, open_id={open_id}, note={note or '-'}"
                    )
                except Exception as exc:  # noqa: BLE001
                    failed_recipients.append({"open_id": open_id, "note": note, "error": str(exc)})
                    emit_log(
                        "[交接班][容量表图片发送] 发送失败 "
                        f"building={building_text}, session_id={session_id}, open_id={open_id}, note={note or '-'}, error={exc}"
                    )
        except Exception as exc:  # noqa: BLE001
            delivery = {
                "status": "failed",
                "last_attempt_at": attempt_at,
                "last_sent_at": "",
                "error": str(exc),
                "image_path": str(image_path or ""),
                "image_key": image_key,
                "successful_recipients": successful_recipients,
                "failed_recipients": failed_recipients,
                "source": str(source or "manual").strip().lower() or "manual",
            }
            try:
                self._review_service.update_capacity_image_delivery(
                    session_id=session_id,
                    capacity_image_delivery=delivery,
                )
            except Exception as save_exc:  # noqa: BLE001
                emit_log(
                    "[交接班][容量表图片发送] 失败状态保存失败 "
                    f"building={building_text}, session_id={session_id}, error={save_exc}"
                )
            emit_log(
                "[交接班][容量表图片发送] 失败 "
                f"building={building_text}, session_id={session_id}, error={exc}"
            )
            raise

        if successful_recipients and failed_recipients:
            status = "partial_failed"
            error = "部分收件人发送失败"
        elif successful_recipients:
            status = "success"
            error = ""
        else:
            status = "failed"
            error = "全部收件人发送失败"
        delivery = {
            "status": status,
            "last_attempt_at": attempt_at,
            "last_sent_at": attempt_at if successful_recipients else "",
            "error": error,
            "image_path": str(image_path or ""),
            "image_key": image_key,
            "successful_recipients": successful_recipients,
            "failed_recipients": failed_recipients,
            "source": str(source or "manual").strip().lower() or "manual",
        }
        try:
            updated = self._review_service.update_capacity_image_delivery(
                session_id=session_id,
                capacity_image_delivery=delivery,
            )
            delivery = dict(updated.get("capacity_image_delivery", delivery))
        except Exception as exc:  # noqa: BLE001
            emit_log(
                "[交接班][容量表图片发送] 状态保存失败但不影响发送结果 "
                f"building={building_text}, session_id={session_id}, error={exc}"
            )
        emit_log(
            "[交接班][容量表图片发送] 完成 "
            f"building={building_text}, session_id={session_id}, status={status}, "
            f"successful={len(successful_recipients)}, failed={len(failed_recipients)}"
        )
        return {
            "ok": status in {"success", "partial_failed"},
            "status": status,
            "building": building_text,
            "session_id": session_id,
            "capacity_image_delivery": delivery,
        }
