from __future__ import annotations

import re
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List

from handover_log_module.core.shift_window import format_duty_date_text
from handover_log_module.repository.excel_reader import load_workbook_quietly
from handover_log_module.repository.review_session_state_store import ReviewSessionStateStore
from handover_log_module.service.handover_cloud_sheet_sync_service import HandoverCloudSheetSyncService
from handover_log_module.service.review_session_service import ReviewSessionService
from pipeline_utils import get_app_dir


_UPLOAD_BLOCK_ID = "station_110_upload"
_SUBSTATION_110KV_BLOCK_ID = "substation_110kv"
_UPLOAD_CLIENT_ID = "110_station_upload"
_UPLOAD_BUILDING = "110站"
_MAX_UPLOAD_BYTES = 50 * 1024 * 1024
_BATCH_LOCK_GUARD = threading.Lock()
_BATCH_LOCKS: dict[str, threading.RLock] = {}

_ROW_SPECS = [
    {"row_id": "incoming_akai", "label": "阿开", "group": "incoming", "tokens": ("阿开",)},
    {"row_id": "incoming_ajia", "label": "阿家", "group": "incoming", "tokens": ("阿家",)},
    {"row_id": "transformer_1", "label": "1#主变", "group": "transformer", "tokens": ("#1主变", "1#主变")},
    {"row_id": "transformer_2", "label": "2#主变", "group": "transformer", "tokens": ("#2主变", "2#主变")},
    {"row_id": "transformer_3", "label": "3#主变", "group": "transformer", "tokens": ("#3主变", "3#主变")},
    {"row_id": "transformer_4", "label": "4#主变", "group": "transformer", "tokens": ("#4主变", "4#主变")},
]
_VALUE_KEYS = ("line_voltage", "current", "power_kw", "power_factor", "load_rate")


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _batch_lock(batch_key: str) -> threading.RLock:
    key = str(batch_key or "").strip()
    with _BATCH_LOCK_GUARD:
        lock = _BATCH_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _BATCH_LOCKS[key] = lock
        return lock


def _runtime_root(config: Dict[str, Any]) -> Path:
    global_paths = config.get("_global_paths", {}) if isinstance(config, dict) else {}
    root_text = str(global_paths.get("runtime_state_root", "") if isinstance(global_paths, dict) else "").strip()
    return Path(root_text) if root_text else get_app_dir() / ".runtime"


def _normalize_shift(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"day", "白班"}:
        return "day"
    if text in {"night", "夜班"}:
        return "night"
    return ""


def _default_duty_context() -> tuple[str, str]:
    now = datetime.now()
    if now.hour < 8:
        return (now - timedelta(days=1)).strftime("%Y-%m-%d"), "night"
    if now.hour < 20:
        return now.strftime("%Y-%m-%d"), "day"
    return now.strftime("%Y-%m-%d"), "night"


def _normalize_duty_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return datetime.strptime(text, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return ""


def _safe_filename(value: Any) -> str:
    text = str(value or "").strip() or "110站上传.xlsx"
    text = text.replace("\\", "_").replace("/", "_").replace(":", "_")
    text = re.sub(r"[\r\n\t]+", "_", text)
    return text[:120] or "110站上传.xlsx"


def _format_cell_display(cell: Any) -> str:
    value = getattr(cell, "value", None)
    if value is None:
        return ""
    number_format = str(getattr(cell, "number_format", "") or "")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "%" in number_format:
            return f"{float(value) * 100:.2f}%"
        if isinstance(value, float):
            return f"{value:.2f}".rstrip("0").rstrip(".")
        return str(value)
    return str(value).strip()


def _count_source_sheet_data_rows(worksheet: Any) -> int:
    max_row = int(getattr(worksheet, "max_row", 0) or 0)
    if max_row <= 1:
        return max_row
    return max_row - 1


def _normalize_match_text(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "").strip()).replace("（", "(").replace("）", ")")


def _empty_cloud_sync(status: str = "") -> Dict[str, Any]:
    return {
        "status": str(status or "").strip().lower(),
        "spreadsheet_token": "",
        "spreadsheet_url": "",
        "spreadsheet_title": "",
        "sheet_title": "110",
        "source_file": "",
        "synced_row_count": 0,
        "synced_column_count": 0,
        "synced_merges": [],
        "dynamic_merge_signature": "",
        "updated_at": "",
        "error": "",
    }


def _normalize_upload_state(raw: Dict[str, Any] | None, *, batch_key: str = "") -> Dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    cloud = payload.get("cloud_sync", {}) if isinstance(payload.get("cloud_sync", {}), dict) else {}
    return {
        "batch_key": str(payload.get("batch_key", "") or batch_key).strip(),
        "duty_date": str(payload.get("duty_date", "")).strip(),
        "duty_shift": _normalize_shift(payload.get("duty_shift", "")),
        "status": str(payload.get("status", "")).strip().lower(),
        "error": str(payload.get("error", "")).strip(),
        "original_filename": str(payload.get("original_filename", "")).strip(),
        "stored_path": str(payload.get("stored_path", "")).strip(),
        "uploaded_at": str(payload.get("uploaded_at", "")).strip(),
        "updated_at": str(payload.get("updated_at", "")).strip(),
        "source_sheet": dict(payload.get("source_sheet", {})) if isinstance(payload.get("source_sheet", {}), dict) else {},
        "substation_sheet": dict(payload.get("substation_sheet", {})) if isinstance(payload.get("substation_sheet", {}), dict) else {},
        "parsed_110kv_rows": payload.get("parsed_110kv_rows", []) if isinstance(payload.get("parsed_110kv_rows", []), list) else [],
        "cloud_sync": {
            **_empty_cloud_sync(),
            **{
                "status": str(cloud.get("status", "")).strip().lower(),
                "spreadsheet_token": str(cloud.get("spreadsheet_token", "")).strip(),
                "spreadsheet_url": str(cloud.get("spreadsheet_url", "")).strip(),
                "spreadsheet_title": str(cloud.get("spreadsheet_title", "")).strip(),
                "sheet_title": str(cloud.get("sheet_title", "110") or "110").strip(),
                "source_file": str(cloud.get("source_file", "")).strip(),
                "synced_row_count": int(cloud.get("synced_row_count", 0) or 0),
                "synced_column_count": int(cloud.get("synced_column_count", 0) or 0),
                "synced_merges": cloud.get("synced_merges", []) if isinstance(cloud.get("synced_merges", []), list) else [],
                "dynamic_merge_signature": str(cloud.get("dynamic_merge_signature", "")).strip(),
                "updated_at": str(cloud.get("updated_at", "")).strip(),
                "error": str(cloud.get("error", "")).strip(),
            },
        },
    }


class Handover110StationUploadService:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config if isinstance(config, dict) else {}
        event_sections = self.config.get("event_sections", {})
        cache_cfg = event_sections.get("cache", {}) if isinstance(event_sections, dict) else {}
        global_paths = self.config.get("_global_paths", {})
        self._state_store = ReviewSessionStateStore(
            cache_state_file=str(cache_cfg.get("state_file", "") or ""),
            global_paths=global_paths if isinstance(global_paths, dict) else None,
        )
        self._review_service = ReviewSessionService(self.config)
        self._cloud_sheet_sync_service = HandoverCloudSheetSyncService(self.config)

    def batch_lock(self, batch_key: str) -> threading.RLock:
        return _batch_lock(batch_key)

    def resolve_context(self, *, duty_date: str = "", duty_shift: str = "") -> Dict[str, Any]:
        date_text = _normalize_duty_date(duty_date)
        shift_text = _normalize_shift(duty_shift)
        if not date_text:
            date_text, shift_text = _default_duty_context()
        elif not shift_text:
            _, shift_text = _default_duty_context()
        batch_key = self._review_service.build_batch_key(date_text, shift_text)
        return {
            "batch_key": batch_key,
            "duty_date": date_text,
            "duty_shift": shift_text,
            "duty_shift_text": "白班" if shift_text == "day" else "夜班",
        }

    def _upload_dir(self, *, duty_date: str, duty_shift: str) -> Path:
        root = _runtime_root(self.config) / "handover" / "110_station_uploads"
        path = root / f"{duty_date}_{duty_shift}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _load_state(self, batch_key: str) -> Dict[str, Any]:
        raw = self._state_store.get_shared_block(batch_key=batch_key, block_id=_UPLOAD_BLOCK_ID)
        payload = raw.get("payload", {}) if isinstance(raw, dict) and isinstance(raw.get("payload", {}), dict) else {}
        return _normalize_upload_state(payload, batch_key=batch_key)

    def _save_state(self, batch_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = _normalize_upload_state(payload, batch_key=batch_key)
        saved = self._state_store.save_shared_block_unlocked(
            batch_key=batch_key,
            block_id=_UPLOAD_BLOCK_ID,
            building=_UPLOAD_BUILDING,
            client_id=_UPLOAD_CLIENT_ID,
            payload=normalized,
        )
        block = saved.get("block", {}) if isinstance(saved, dict) else {}
        return _normalize_upload_state(
            block.get("payload", {}) if isinstance(block.get("payload", {}), dict) else normalized,
            batch_key=batch_key,
        )

    def _save_substation_rows(self, *, batch_key: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        saved = self._state_store.save_shared_block_unlocked(
            batch_key=batch_key,
            block_id=_SUBSTATION_110KV_BLOCK_ID,
            building=_UPLOAD_BUILDING,
            client_id=_UPLOAD_CLIENT_ID,
            payload={"rows": rows},
        )
        if not bool(saved.get("no_change", False)):
            self._state_store.clear_shared_block_lock(
                batch_key=batch_key,
                block_id=_SUBSTATION_110KV_BLOCK_ID,
            )
        return saved

    def _build_upload_file_path(self, *, upload_dir: Path, filename: str) -> Path:
        suffix_name = _safe_filename(filename)
        base_stamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        candidate = upload_dir / f"{base_stamp}--{suffix_name}"
        if not candidate.exists():
            return candidate
        index = 1
        while True:
            fallback = upload_dir / f"{base_stamp}-{index:02d}--{suffix_name}"
            if not fallback.exists():
                return fallback
            index += 1

    @staticmethod
    def _build_failed_state_payload(
        *,
        batch_key: str,
        duty_date: str,
        duty_shift: str,
        filename: str,
        stored_path: Path,
        error_text: str,
        parsed: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "batch_key": batch_key,
            "duty_date": duty_date,
            "duty_shift": duty_shift,
            "status": "failed",
            "error": str(error_text or "").strip(),
            "original_filename": str(filename or "").strip(),
            "stored_path": str(stored_path),
            "uploaded_at": _now_text(),
            "updated_at": _now_text(),
            "cloud_sync": _empty_cloud_sync(),
        }
        if isinstance(parsed, dict):
            payload["source_sheet"] = parsed.get("source_sheet", {}) if isinstance(parsed.get("source_sheet", {}), dict) else {}
            payload["substation_sheet"] = parsed.get("substation_sheet", {}) if isinstance(parsed.get("substation_sheet", {}), dict) else {}
            payload["parsed_110kv_rows"] = (
                parsed.get("parsed_110kv_rows", [])
                if isinstance(parsed.get("parsed_110kv_rows", []), list)
                else []
            )
        return payload

    def _parse_workbook(self, path: Path) -> Dict[str, Any]:
        workbook = load_workbook_quietly(path, data_only=True)
        try:
            if len(workbook.worksheets) < 2:
                raise ValueError("110站文件至少需要包含2个sheet页")
            first_ws = workbook.worksheets[0]
            second_ws = workbook.worksheets[1]
            parsed_rows = self._parse_substation_sheet(second_ws)
            return {
                "source_sheet": {
                    "title": str(first_ws.title or "").strip(),
                    "max_row": int(first_ws.max_row or 0),
                    "max_column": int(first_ws.max_column or 0),
                    "recognized_row_count": _count_source_sheet_data_rows(first_ws),
                    "merge_count": len(first_ws.merged_cells.ranges),
                },
                "substation_sheet": {
                    "title": str(second_ws.title or "").strip(),
                    "max_row": int(second_ws.max_row or 0),
                    "max_column": int(second_ws.max_column or 0),
                    "parsed_row_count": len(parsed_rows),
                },
                "parsed_110kv_rows": parsed_rows,
            }
        finally:
            workbook.close()

    def _parse_substation_sheet(self, worksheet: Any) -> List[Dict[str, Any]]:
        matched: Dict[str, Dict[str, Any]] = {}
        for row_idx in range(1, int(worksheet.max_row or 0) + 1):
            raw_label = worksheet.cell(row=row_idx, column=4).value or worksheet.cell(row=row_idx, column=1).value
            label_text = _normalize_match_text(raw_label)
            if not label_text:
                continue
            value_cells = [worksheet.cell(row=row_idx, column=col_idx) for col_idx in range(5, 10)]
            values = [_format_cell_display(cell) for cell in value_cells]
            if not any(values) or str(values[0]).strip() == "线电压":
                continue
            for spec in _ROW_SPECS:
                if spec["row_id"] in matched:
                    continue
                if not any(_normalize_match_text(token) in label_text for token in spec["tokens"]):
                    continue
                row = {
                    "row_id": spec["row_id"],
                    "label": spec["label"],
                    "group": spec["group"],
                }
                for key, value in zip(_VALUE_KEYS, values):
                    row[key] = str(value or "").strip()
                matched[spec["row_id"]] = row
                break

        missing = [spec["label"] for spec in _ROW_SPECS if spec["row_id"] not in matched]
        if missing:
            raise ValueError(f"110站第2个sheet未识别到关键行: {', '.join(missing)}")
        return [matched[spec["row_id"]] for spec in _ROW_SPECS]

    def _ensure_cloud_batch(
        self,
        *,
        batch_key: str,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        existing = self._review_service.get_cloud_batch(batch_key)
        if isinstance(existing, dict) and str(existing.get("spreadsheet_token", "")).strip():
            status = str(existing.get("status", "")).strip().lower()
            if status in {"prepared", "success"}:
                validation = self._cloud_sheet_sync_service.validate_batch_spreadsheet(
                    batch_meta=existing,
                    emit_log=emit_log,
                )
                if bool(validation.get("valid", False)):
                    return existing
                emit_log(
                    "[交接班][110站云表预建] 已缓存云文档失效，将重新创建: "
                    f"batch={batch_key}, token={existing.get('spreadsheet_token', '')}"
                )

        shift_text = "白班" if duty_shift == "day" else "夜班"
        prepared = self._cloud_sheet_sync_service.prepare_batch_spreadsheet(
            duty_date=duty_date,
            duty_date_text=format_duty_date_text(duty_date),
            shift_text=shift_text,
            emit_log=emit_log,
        )
        return self._review_service.register_cloud_batch(
            batch_key=batch_key,
            duty_date=duty_date,
            duty_shift=duty_shift,
            cloud_batch=prepared,
        )

    def _sync_state_to_cloud(
        self,
        *,
        state: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        batch_key = str(state.get("batch_key", "")).strip()
        duty_date = _normalize_duty_date(state.get("duty_date", ""))
        duty_shift = _normalize_shift(state.get("duty_shift", ""))
        stored_path = Path(str(state.get("stored_path", "")).strip())
        if not batch_key or not duty_date or not duty_shift:
            raise ValueError("110站上传状态缺少日期/班次")
        if not stored_path.exists():
            raise FileNotFoundError(f"110站上传文件不存在: {stored_path}")

        with _batch_lock(batch_key):
            batch_meta = self._ensure_cloud_batch(
                batch_key=batch_key,
                duty_date=duty_date,
                duty_shift=duty_shift,
                emit_log=emit_log,
            )
            if str(batch_meta.get("status", "")).strip().lower() not in {"prepared", "success"}:
                result = {
                    **_empty_cloud_sync("failed"),
                    "spreadsheet_token": str(batch_meta.get("spreadsheet_token", "")).strip(),
                    "spreadsheet_url": str(batch_meta.get("spreadsheet_url", "")).strip(),
                    "spreadsheet_title": str(batch_meta.get("spreadsheet_title", "")).strip(),
                    "error": str(batch_meta.get("error", "")).strip() or "云文档预创建失败",
                    "updated_at": _now_text(),
                }
            else:
                result = self._cloud_sheet_sync_service.sync_station_110_workbook(
                    batch_meta=batch_meta,
                    source_file=stored_path,
                    emit_log=emit_log,
                )
                result["updated_at"] = _now_text()
            next_state = {
                **state,
                "cloud_sync": {**_empty_cloud_sync(), **result},
                "updated_at": _now_text(),
            }
            return self._save_state(batch_key, next_state)

    def status(self, *, duty_date: str = "", duty_shift: str = "") -> Dict[str, Any]:
        context = self.resolve_context(duty_date=duty_date, duty_shift=duty_shift)
        batch_key = context["batch_key"]
        upload_state = self._load_state(batch_key)
        cloud_batch = self._review_service.get_cloud_batch(batch_key) or {}
        batch_status = self._review_service.get_batch_status(batch_key)
        return {
            "ok": True,
            "batch": context,
            "batch_status": batch_status,
            "upload": upload_state,
            "cloud_batch": cloud_batch,
        }

    def parse(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        filename: str,
        content: bytes,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        context = self.resolve_context(duty_date=duty_date, duty_shift=duty_shift)
        batch_key = context["batch_key"]
        suffix = Path(str(filename or "")).suffix.lower()
        if suffix not in {".xlsx", ".xlsm"}:
            raise ValueError("110站文件仅支持 .xlsx / .xlsm")
        if not content:
            raise ValueError("上传文件为空")
        if len(content) > _MAX_UPLOAD_BYTES:
            raise ValueError("上传文件过大，110站文件最大支持50MB")

        with _batch_lock(batch_key):
            upload_dir = self._upload_dir(duty_date=context["duty_date"], duty_shift=context["duty_shift"])
            stored_path = self._build_upload_file_path(upload_dir=upload_dir, filename=filename)
            stored_path.write_bytes(content)
            emit_log(
                f"[交接班][110站解析] 已保存 batch={batch_key}, file={stored_path}"
            )
            try:
                parsed = self._parse_workbook(stored_path)
            except Exception as exc:  # noqa: BLE001
                failed_state = self._save_state(
                    batch_key,
                    self._build_failed_state_payload(
                        batch_key=batch_key,
                        duty_date=context["duty_date"],
                        duty_shift=context["duty_shift"],
                        filename=filename,
                        stored_path=stored_path,
                        error_text=str(exc),
                    ),
                )
                emit_log(f"[交接班][110站解析] 解析失败 batch={batch_key}, error={exc}")
                return {"ok": False, "batch": context, "upload": failed_state, "error": str(exc)}

            state = self._save_state(
                batch_key,
                {
                    "batch_key": batch_key,
                    "duty_date": context["duty_date"],
                    "duty_shift": context["duty_shift"],
                    "status": "parsed",
                    "error": "",
                    "original_filename": str(filename or "").strip(),
                    "stored_path": str(stored_path),
                    "uploaded_at": _now_text(),
                    "updated_at": _now_text(),
                    "source_sheet": parsed["source_sheet"],
                    "substation_sheet": parsed["substation_sheet"],
                    "parsed_110kv_rows": parsed["parsed_110kv_rows"],
                    "cloud_sync": _empty_cloud_sync(),
                },
            )
            self._save_substation_rows(batch_key=batch_key, rows=parsed["parsed_110kv_rows"])
            emit_log(
                f"[交接班][110站解析] 解析完成 batch={batch_key}, "
                f"source_rows={parsed['source_sheet'].get('recognized_row_count', 0)}, "
                f"110kv_rows={len(parsed['parsed_110kv_rows'])}"
            )
            return {
                "ok": True,
                "batch": context,
                "upload": state,
                "cloud_batch": self._review_service.get_cloud_batch(batch_key) or {},
            }

    def upload(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        filename: str,
        content: bytes,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        context = self.resolve_context(duty_date=duty_date, duty_shift=duty_shift)
        batch_key = context["batch_key"]
        suffix = Path(str(filename or "")).suffix.lower()
        if suffix not in {".xlsx", ".xlsm"}:
            raise ValueError("110站文件仅支持 .xlsx / .xlsm")
        if not content:
            raise ValueError("上传文件为空")
        if len(content) > _MAX_UPLOAD_BYTES:
            raise ValueError("上传文件过大，110站文件最大支持50MB")

        with _batch_lock(batch_key):
            upload_dir = self._upload_dir(duty_date=context["duty_date"], duty_shift=context["duty_shift"])
            stored_path = self._build_upload_file_path(upload_dir=upload_dir, filename=filename)
            stored_path.write_bytes(content)
            emit_log(
                f"[交接班][110站上传] 已保存 batch={batch_key}, file={stored_path}"
            )
            try:
                parsed = self._parse_workbook(stored_path)
            except Exception as exc:  # noqa: BLE001
                failed_state = self._save_state(
                    batch_key,
                    self._build_failed_state_payload(
                        batch_key=batch_key,
                        duty_date=context["duty_date"],
                        duty_shift=context["duty_shift"],
                        filename=filename,
                        stored_path=stored_path,
                        error_text=str(exc),
                    ),
                )
                emit_log(f"[交接班][110站上传] 解析失败 batch={batch_key}, error={exc}")
                return {"ok": False, "batch": context, "upload": failed_state, "error": str(exc)}

            state = self._save_state(
                batch_key,
                {
                    "batch_key": batch_key,
                    "duty_date": context["duty_date"],
                    "duty_shift": context["duty_shift"],
                    "status": "success",
                    "error": "",
                    "original_filename": str(filename or "").strip(),
                    "stored_path": str(stored_path),
                    "uploaded_at": _now_text(),
                    "updated_at": _now_text(),
                    "source_sheet": parsed["source_sheet"],
                    "substation_sheet": parsed["substation_sheet"],
                    "parsed_110kv_rows": parsed["parsed_110kv_rows"],
                    "cloud_sync": _empty_cloud_sync("pending"),
                },
            )
            try:
                self._save_substation_rows(batch_key=batch_key, rows=parsed["parsed_110kv_rows"])
            except Exception as exc:  # noqa: BLE001
                error_text = str(exc)
                failed_state = self._save_state(
                    batch_key,
                    self._build_failed_state_payload(
                        batch_key=batch_key,
                        duty_date=context["duty_date"],
                        duty_shift=context["duty_shift"],
                        filename=filename,
                        stored_path=stored_path,
                        error_text=error_text,
                        parsed=parsed,
                    ),
                )
                emit_log(f"[交接班][110站上传] 共享110KV写入失败 batch={batch_key}, error={error_text}")
                return {"ok": False, "batch": context, "upload": failed_state, "error": error_text}
            emit_log(
                f"[交接班][110站上传] 解析完成 batch={batch_key}, "
                f"source_sheet={parsed['source_sheet'].get('title', '-')}, "
                f"110kv_rows={len(parsed['parsed_110kv_rows'])}"
            )
            synced_state = self._sync_state_to_cloud(state=state, emit_log=emit_log)
            return {
                "ok": True,
                "batch": context,
                "upload": synced_state,
                "cloud_batch": self._review_service.get_cloud_batch(batch_key) or {},
            }

    def retry_cloud_sync(
        self,
        *,
        duty_date: str = "",
        duty_shift: str = "",
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        context = self.resolve_context(duty_date=duty_date, duty_shift=duty_shift)
        state = self._load_state(context["batch_key"])
        if str(state.get("status", "")).strip().lower() != "success":
            return {
                "ok": False,
                "batch": context,
                "upload": state,
                "error": str(state.get("error", "")).strip() or "当前批次没有可同步的110站文件",
            }
        synced_state = self._sync_state_to_cloud(state=state, emit_log=emit_log)
        return {
            "ok": str(synced_state.get("cloud_sync", {}).get("status", "")).strip().lower() == "success",
            "batch": context,
            "upload": synced_state,
            "cloud_batch": self._review_service.get_cloud_batch(context["batch_key"]) or {},
        }

    def sync_existing_upload_to_cloud(
        self,
        *,
        batch_key: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        target_batch = str(batch_key or "").strip()
        if not target_batch:
            return {"status": "skipped", "reason": "missing_batch_key"}
        state = self._load_state(target_batch)
        if str(state.get("status", "")).strip().lower() != "success":
            return {"status": "skipped", "reason": "no_success_upload", "upload": state}
        synced = self._sync_state_to_cloud(state=state, emit_log=emit_log)
        return {
            "status": str(synced.get("cloud_sync", {}).get("status", "")).strip().lower() or "failed",
            "upload": synced,
        }
