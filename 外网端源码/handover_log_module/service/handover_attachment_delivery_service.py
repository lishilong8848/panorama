"""Build supplemental handover files and attach them to a Bitable record."""
from __future__ import annotations

from datetime import date as calendar_date, datetime
import hashlib
import os
from pathlib import Path
import shutil
import tempfile
import threading
from typing import Any
from zipfile import ZipFile

from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from handover_log_module.service.handover_attachment_store import HandoverAttachmentStore
from handover_log_module.service.handover_attachment_workbook_builder import (
    build_station_h_workbook,
    default_station_h_template_path,
)


class HandoverAttachmentDeliveryService:
    MANAGED_BUILDINGS = ("A楼", "B楼", "C楼", "D楼", "E楼")

    def __init__(self, config: dict[str, Any], *, store=None, client=None, output_root=None):
        self.config = config
        self._store = store
        self._attachment_client = client
        self._store_lock = threading.Lock()
        self.output_root = Path(output_root) if output_root is not None else (
            Path(__file__).resolve().parents[2] / "runtime" / "handover" / "attachment_outputs"
        )

    def _enabled(self):
        return bool(self.config.get("daily_report_bitable_export", {}).get("enabled", True))

    def station_h_template_path(self):
        # Keep the existing local template path, not the obsolete Wiki template.
        legacy = self.config.get("cloud_sheet_sync")
        configured = str(legacy.get("station_h_template_path") or "").strip() if isinstance(legacy, dict) else ""
        default = default_station_h_template_path()
        return default.parent / configured if configured else default

    def _build_abcdeh_work_content_workbook(self, work_content):
        workbook = Workbook()
        worksheet = workbook.active
        worksheet.title = "ABCDEH工作内容"
        worksheet.sheet_format.defaultRowHeight = 42
        for col, width in enumerate((10, 12, *([18] * 6), *([10] * 12)), start=1):
            worksheet.column_dimensions[get_column_letter(col)].width = width
        for row in range(1, 21):
            worksheet.row_dimensions[row].height = 42
        worksheet.cell(20, 20, "")
        for row, building in zip(range(3, 15, 2), (*self.MANAGED_BUILDINGS, "H楼")):
            worksheet.merge_cells(start_row=row, start_column=1, end_row=row + 1, end_column=1)
            worksheet.cell(row, 1, building)
            payload = work_content.get(building, {}) if isinstance(work_content, dict) else {}
            payload = payload if isinstance(payload, dict) else {}
            for target, label, key in ((row, "交班信息", "previous"), (row + 1, "当班信息", "current")):
                worksheet.cell(target, 2, label)
                worksheet.merge_cells(start_row=target, start_column=3, end_row=target, end_column=8)
                if building != "H楼":
                    worksheet.cell(target, 3, str(payload.get(key) or "").strip() or "/")
        return workbook

    def _attachment_store(self):
        with self._store_lock:
            if self._store is None:
                self._store = HandoverAttachmentStore(self.config, client=self._attachment_client)
            return self._store

    @staticmethod
    def _context(meta: dict[str, Any]) -> tuple[str, str]:
        dates = [str(meta[k]).strip() for k in ("duty_date", "date") if meta.get(k)]
        shifts = [str(meta[k]).strip().lower() for k in ("duty_shift", "shift", "shift_text") if meta.get(k)]
        key = str(meta.get("batch_key", "") or "").strip()
        if key:
            parts = key.split("|")
            if len(parts) != 2:
                raise ValueError("batch_key must be YYYY-MM-DD|day or YYYY-MM-DD|night")
            dates.append(parts[0].strip())
            shifts.append(parts[1].strip().lower())
        aliases = {"day": "day", "night": "night", "白班": "day", "日班": "day", "夜班": "night"}
        shifts = [aliases.get(value, value) for value in shifts]
        if not dates or not shifts or len(set(dates)) != 1 or len(set(shifts)) != 1:
            raise ValueError("missing or conflicting attachment duty date/shift")
        if calendar_date.fromisoformat(dates[0]).isoformat() != dates[0] or shifts[0] not in {"day", "night"}:
            raise ValueError("invalid attachment duty date/shift")
        return dates[0], shifts[0]

    def _base(self, meta=None, *, status="failed", error="", sheet_title=""):
        meta = meta if isinstance(meta, dict) else {}
        token = str(meta.get("spreadsheet_token", "") or "").strip()
        is_attachment = token.startswith("bitable:") and bool(token[7:].strip())
        return {
            "attempted": status not in {"disabled", "skipped"},
            "success": status == "success", "status": status, "error": error,
            "spreadsheet_token": token if is_attachment else "",
            "spreadsheet_url": str(meta.get("spreadsheet_url") or "") if is_attachment else "",
            "spreadsheet_title": str(meta.get("spreadsheet_title") or ""),
            "sheet_title": sheet_title, "delivery_mode": "attachment",
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def _record_meta(self, record, date, shift, meta=None):
        if record.get("status") not in {None, "success"}:
            raise RuntimeError(str(record.get("error") or "attachment store failed"))
        record_id = str(record.get("record_id", "") or "").strip()
        url = str(record.get("record_url", "") or "").strip()
        if not record_id or not url:
            raise RuntimeError("attachment store returned no record_id/record_url")
        meta = meta or {}
        title = str(meta.get("spreadsheet_title", "") or "").strip() or "班次交接班附件"
        return {
            "spreadsheet_token": f"bitable:{record_id}", "spreadsheet_url": url,
            "spreadsheet_title": title, "record_id": record_id, "record_url": url,
            "date": date, "shift": shift, "duty_date": date, "duty_shift": shift,
            "batch_key": f"{date}|{shift}", "delivery_mode": "attachment",
        }

    @staticmethod
    def _reject_cloud_token(meta, *, allow_missing=False):
        token = str(meta.get("spreadsheet_token", "") or "").strip()
        if not token and allow_missing:
            return
        if not token.startswith("bitable:") or not token[7:].strip():
            raise ValueError("五楼班次附件尚未生成多维记录，暂不能补传")

    def _directory(self, date, shift):
        output = self.output_root / date / shift
        output.mkdir(parents=True, exist_ok=True)
        return output

    @staticmethod
    def _require_file(raw, *, workbook=False):
        path = Path(str(raw or "").strip())
        if not path.is_file() or path.stat().st_size == 0:
            raise ValueError(f"attachment missing or empty: {raw}")
        if workbook:
            if path.suffix.lower() in {".xlsx", ".xlsm"}:
                with ZipFile(path) as archive:
                    if "xl/workbook.xml" not in archive.namelist() or archive.testzip():
                        raise ValueError(f"incomplete Excel attachment: {path}")
            elif path.suffix.lower() == ".xls":
                with path.open("rb") as stream:
                    if stream.read(8) != bytes.fromhex("D0CF11E0A1B11AE1"):
                        raise ValueError(f"invalid XLS attachment: {path}")
            else:
                raise ValueError(f"unsupported Excel attachment: {path}")
        return path

    @staticmethod
    def _save_atomic(target, writer):
        fd, temporary = tempfile.mkstemp(prefix=".attachment-", suffix=target.suffix, dir=target.parent)
        os.close(fd)
        try:
            writer(temporary)
            os.replace(temporary, target)
        finally:
            Path(temporary).unlink(missing_ok=True)
        return target

    def _stage(self, source, target):
        if source.resolve() != target.resolve():
            self._save_atomic(target, lambda temporary: shutil.copyfile(source, temporary))
        return target

    def _upload(self, meta, files, *, building, require_buildings=True, is_current=None):
        self._reject_cloud_token(meta, allow_missing=not require_buildings)
        date, shift = self._context(meta)
        options = {"is_current": is_current} if is_current is not None else {}
        # 按「日期+班次+楼栋」各自记录上传：upload_files 内部 ensure_record(date,shift,building)
        # 找/建本楼记录（楼栋单选）。不再用 batch token 当记录ID、不再要求其它楼栋文件同记录。
        record = self._attachment_store().upload_files(
            date, shift, files, building=building,
            **options,
        )
        result = self._base(meta, status="success")
        result.update(self._record_meta(record, date, shift, meta))
        result.update(attachment_files={name: str(path) for name, path in files.items()},
                      attachment_count=record.get("attachment_count", len(files)))
        return result

    def sync_station_h_sheet(self, *, batch_meta, cell_values, duty_focus_image_path=None,
                             skip_values=False, emit_log=print, is_current=None,
                             section_rows=None, attention_executor=""):
        title = "H楼"
        if not self._enabled():
            return self._base(batch_meta, status="skipped", error="disabled", sheet_title=title)
        image_error = ""
        image = None
        try:
            self._reject_cloud_token(batch_meta, allow_missing=True)
            date, shift = self._context(batch_meta)
            if not cell_values:
                raise ValueError("H attachment requires complete cell_values, including on image retry")
            output = self._directory(date, shift)
            if duty_focus_image_path:
                try:
                    source = self._require_file(duty_focus_image_path)
                    image = self._stage(source, output / f"H_duty_focus{source.suffix.lower()}")
                except Exception as exc:
                    image_error = str(exc)
            else:
                image_error = "duty_focus_image_missing"
            target = output / "H_handover.xlsx"
            # 值班关注点图片**不再嵌入日志表**，只作为独立附件上传到「交接班日志」字段；
            # 因此日志工作簿构建不再涉及图片，也无需“嵌入失败重试”。
            build_station_h_workbook(
                self.station_h_template_path(), target, cell_values,
                section_rows=section_rows, attention_executor=attention_executor,
            )
            files = {target.name: self._require_file(target, workbook=True)}
            if image is not None:
                files[image.name] = image
            result = self._upload(batch_meta, files, building="H楼", require_buildings=False, is_current=is_current)
            result.update(sheet_title=title, source_file=str(target), h_values_status="success",
                          duty_focus_image_synced=image is not None,
                          duty_focus_image_status="success" if image is not None else "failed",
                          duty_focus_image_error=image_error,
                          duty_focus_image_cell="",  # 图片不再嵌入日志表，无单元格锚点
                          duty_focus_image_sha256=hashlib.sha256(image.read_bytes()).hexdigest() if image is not None else "")
            if image_error:
                result.update(status="partial_failed", success=False, error=image_error)
                emit_log(f"[交接班][H楼附件] 正文已上传，图片未完成: {image_error}")
            return result
        except Exception as exc:
            emit_log(f"[交接班][H楼附件] 失败: {exc}")
            result = self._base(batch_meta, error=str(exc), sheet_title=title)
            result.update(h_values_status="failed", duty_focus_image_status="failed",
                          duty_focus_image_synced=False, duty_focus_image_error=image_error)
            return result

    def sync_abcdeh_work_content_sheet(self, *, batch_meta, work_content, emit_log=print):
        title = "ABCDEH工作内容"
        if not self._enabled():
            return self._base(batch_meta, status="skipped", error="disabled", sheet_title=title)
        try:
            self._reject_cloud_token(batch_meta)
            date, shift = self._context(batch_meta)
            target = self._directory(date, shift) / "ABCDEH_work_content.xlsx"
            workbook = self._build_abcdeh_work_content_workbook(work_content)
            try:
                rows, cols = workbook.active.max_row, workbook.active.max_column
                self._save_atomic(target, workbook.save)
            finally:
                workbook.close()
            # ABCDEH工作内容是跨 A–E+H 的合并汇总文件，无单一楼栋：building="" → 各自独立记录（楼栋留空）。
            result = self._upload(batch_meta, {target.name: self._require_file(target, workbook=True)}, building="")
            result.update(sheet_title=title, source_file=str(target), synced_row_count=rows, synced_column_count=cols)
            return result
        except Exception as exc:
            emit_log(f"[交接班][工作内容附件] 失败: {exc}")
            return self._base(batch_meta, error=str(exc), sheet_title=title)

    def sync_station_110_workbook(self, *, batch_meta, source_file, emit_log=print):
        title = "110"
        if not self._enabled():
            return self._base(batch_meta, status="skipped", error="disabled", sheet_title=title)
        try:
            self._reject_cloud_token(batch_meta)
            source = self._require_file(source_file, workbook=True)
            result = self._upload(batch_meta, {f"110_original{source.suffix.lower()}": source}, building="110站")
            result.update(sheet_title=title, source_file=str(source), output_file=str(source))
            return result
        except Exception as exc:
            emit_log(f"[交接班][110附件] 失败: {exc}")
            return self._base(batch_meta, error=str(exc), sheet_title=title)

