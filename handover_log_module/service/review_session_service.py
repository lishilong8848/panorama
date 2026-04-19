from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import Any, Dict, List

from app.shared.utils.artifact_naming import handover_log_output_patterns
from app.shared.utils.file_utils import fallback_missing_windows_drive_path
from handover_log_module.repository.excel_reader import load_workbook_quietly
from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore
from handover_log_module.repository.review_session_state_store import ReviewSessionStateStore
from handover_log_module.service.handover_source_file_cache_service import HandoverSourceFileCacheService
from pipeline_utils import get_app_dir


class ReviewSessionConflictError(RuntimeError):
    pass


class ReviewSessionNotFoundError(RuntimeError):
    pass


class ReviewSessionStoreUnavailableError(RuntimeError):
    pass


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_datetime_text(value: Any) -> datetime:
    text = str(value or "").strip()
    if not text:
        return datetime.min
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return datetime.min


def _is_recoverable_review_store_error(exc: Exception) -> bool:
    if isinstance(exc, ReviewSessionStoreUnavailableError):
        return True
    if isinstance(exc, PermissionError):
        return True
    text = f"{type(exc).__name__}: {exc}".lower()
    tokens = (
        "database is locked",
        "database table is locked",
        "database is busy",
        "busy",
        "unable to open database file",
        "disk i/o error",
        "readonly database",
        "cannot operate on a closed database",
        "permission denied",
        "winerror 5",
    )
    return any(token in text for token in tokens)


def _reraise_review_store_error(exc: Exception) -> None:
    if isinstance(exc, ReviewSessionStoreUnavailableError):
        raise exc
    if _is_recoverable_review_store_error(exc):
        raise ReviewSessionStoreUnavailableError("审核状态存储暂时不可用，请稍后重试") from exc
    raise exc


def _normalize_review_link_delivery(raw: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    return {
        "status": str(payload.get("status", "") or "").strip().lower(),
        "last_attempt_at": str(payload.get("last_attempt_at", "") or "").strip(),
        "last_sent_at": str(payload.get("last_sent_at", "") or "").strip(),
        "error": str(payload.get("error", "") or "").strip(),
        "url": str(payload.get("url", "") or "").strip(),
        "successful_recipients": [
            str(item or "").strip()
            for item in (
                payload.get("successful_recipients", [])
                if isinstance(payload.get("successful_recipients", []), list)
                else []
            )
            if str(item or "").strip()
        ],
        "failed_recipients": [
            {
                "open_id": str(item.get("open_id", "") or "").strip(),
                "note": str(item.get("note", "") or "").strip(),
                "error": str(item.get("error", "") or "").strip(),
            }
            for item in (
                payload.get("failed_recipients", [])
                if isinstance(payload.get("failed_recipients", []), list)
                else []
            )
            if isinstance(item, dict)
        ],
        "source": str(payload.get("source", "") or "").strip().lower(),
        "auto_attempted": bool(payload.get("auto_attempted", False)),
        "auto_attempted_at": str(payload.get("auto_attempted_at", "") or "").strip(),
    }


_CAPACITY_SYNC_TRACKED_CELLS = ["H6", "F8", "B6", "D6", "F6", "D8", "B13", "D13"]


class ReviewSessionService:
    DEFAULT_BUILDINGS = [
        {"code": "a", "name": "A楼"},
        {"code": "b", "name": "B楼"},
        {"code": "c", "name": "C楼"},
        {"code": "d", "name": "D楼"},
        {"code": "e", "name": "E楼"},
    ]

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config if isinstance(config, dict) else {}
        event_sections = self.config.get("event_sections", {})
        cache_cfg = event_sections.get("cache", {}) if isinstance(event_sections, dict) else {}
        global_paths = self.config.get("_global_paths", {})
        self._review_state_store = ReviewSessionStateStore(
            cache_state_file=str(cache_cfg.get("state_file", "") or ""),
            global_paths=global_paths if isinstance(global_paths, dict) else None,
        )
        self._source_file_cache_service = HandoverSourceFileCacheService(self.config)

    def _review_cfg(self) -> Dict[str, Any]:
        review_ui = self.config.get("review_ui", {})
        return review_ui if isinstance(review_ui, dict) else {}

    def _template_output_dir(self) -> Path | None:
        template_cfg = self.config.get("template", {})
        if not isinstance(template_cfg, dict):
            return None
        output_dir_text = str(template_cfg.get("output_dir", "")).strip()
        if not output_dir_text:
            return None
        output_dir = Path(output_dir_text)
        global_paths = self.config.get("_global_paths", {})
        runtime_root_text = (
            str(global_paths.get("runtime_state_root", "")).strip()
            if isinstance(global_paths, dict)
            else ""
        )
        if not output_dir.is_absolute() and runtime_root_text:
            runtime_root = Path(runtime_root_text)
            if runtime_root.is_absolute():
                output_dir = runtime_root.parent / output_dir
        if not output_dir.is_absolute():
            output_dir = Path(__file__).resolve().parents[2] / output_dir
        return fallback_missing_windows_drive_path(output_dir, app_dir=get_app_dir())

    @staticmethod
    def _is_path_under(child: Path, parent: Path) -> bool:
        try:
            child.resolve().relative_to(parent.resolve())
            return True
        except Exception:  # noqa: BLE001
            return False

    def _is_legacy_test_output_file(self, output_file: str) -> bool:
        raw = str(output_file or "").strip()
        if not raw:
            return False
        normalized = raw.replace("/", "\\").lower()
        file_name = Path(raw).name.lower()
        if "pytest-of-" in normalized or "\\pytest-" in normalized:
            return True
        if "\\appdata\\local\\temp\\" in normalized and file_name.endswith("_handover.xlsx"):
            return True
        template_output_dir = self._template_output_dir()
        if template_output_dir is not None:
            try:
                output_path = Path(raw)
                if (
                    file_name.endswith("_handover.xlsx")
                    and not self._is_path_under(output_path, template_output_dir)
                ):
                    return True
            except Exception:  # noqa: BLE001
                pass
        return False

    @staticmethod
    def _safe_mtime(path: Path) -> float:
        try:
            return path.stat().st_mtime
        except Exception:  # noqa: BLE001
            return 0.0

    def _formal_output_patterns(self, building: str) -> tuple[re.Pattern[str], re.Pattern[str]]:
        return handover_log_output_patterns(building)

    def _match_formal_output_name(self, building: str, file_name: str) -> re.Match[str] | None:
        legacy_pattern, canonical_pattern = self._formal_output_patterns(building)
        for pattern in (canonical_pattern, legacy_pattern):
            matched = pattern.match(str(file_name or "").strip())
            if matched:
                return matched
        return None

    def _list_formal_output_files(self, building: str) -> List[Path]:
        output_dir = self._template_output_dir()
        if output_dir is None or not output_dir.exists():
            return []
        candidates: List[tuple[str, int, float, Path]] = []
        for path in output_dir.rglob("*.xlsx"):
            if not path.is_file():
                continue
            match = self._match_formal_output_name(building, path.name)
            if not match:
                continue
            duty_date = str(match.group("date") or "").strip()
            sequence = int(match.group("seq") or 0)
            candidates.append((duty_date, sequence, self._safe_mtime(path), path))
        candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
        return [item[3] for item in candidates]

    def _template_sheet_name(self) -> str:
        template_cfg = self.config.get("template", {})
        if not isinstance(template_cfg, dict):
            return "交接班日志"
        return str(template_cfg.get("sheet_name", "")).strip() or "交接班日志"

    def _shift_cell(self) -> str:
        fixed_cfg = self.config.get("template_fixed_fill", {})
        if not isinstance(fixed_cfg, dict):
            return "F2"
        return str(fixed_cfg.get("shift_cell", "")).strip() or "F2"

    def _shift_alias_lookup(self) -> Dict[str, str]:
        roster_cfg = self.config.get("shift_roster", {})
        alias_cfg = roster_cfg.get("shift_alias", {}) if isinstance(roster_cfg, dict) else {}
        lookup: Dict[str, str] = {}
        if isinstance(alias_cfg, dict):
            for duty_shift in ("day", "night"):
                aliases = alias_cfg.get(duty_shift, [])
                if isinstance(aliases, list):
                    for alias in aliases:
                        alias_text = str(alias or "").strip()
                        if alias_text:
                            lookup[alias_text.casefold()] = duty_shift
        lookup.setdefault("白班".casefold(), "day")
        lookup.setdefault("夜班".casefold(), "night")
        lookup.setdefault("day".casefold(), "day")
        lookup.setdefault("night".casefold(), "night")
        return lookup

    def _infer_duty_context_from_output(self, building: str, output_path: Path) -> tuple[str, str] | None:
        match = self._match_formal_output_name(building, output_path.name)
        if not match:
            return None
        raw_date = str(match.group("date") or "").strip()
        if len(raw_date) != 8:
            return None
        duty_date = f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
        shift_text = str(match.groupdict().get("shift", "") or "").strip()
        if shift_text == "白班":
            return duty_date, "day"
        if shift_text == "夜班":
            return duty_date, "night"
        workbook = None
        try:
            workbook = load_workbook_quietly(output_path, read_only=True, data_only=False)
            sheet_name = self._template_sheet_name()
            if sheet_name not in workbook.sheetnames:
                return None
            worksheet = workbook[sheet_name]
            shift_text = str(worksheet[self._shift_cell()].value or "").strip()
            duty_shift = self._shift_alias_lookup().get(shift_text.casefold(), "")
            if not duty_shift:
                return None
            return duty_date, duty_shift
        except Exception:  # noqa: BLE001
            return None
        finally:
            if workbook is not None:
                workbook.close()

    def _recover_latest_session_from_output_file(self, building: str) -> Dict[str, Any] | None:
        building_name = str(building or "").strip()
        if not building_name:
            return None
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        latest_map = state.get("review_latest_by_building", {})
        if not isinstance(sessions, dict):
            sessions = {}
        if not isinstance(latest_map, dict):
            latest_map = {}

        latest_session_id = str(latest_map.get(building_name, "")).strip()
        latest_session = (
            self._normalize_session(sessions.get(latest_session_id, {}))
            if latest_session_id and isinstance(sessions.get(latest_session_id, {}), dict)
            else None
        )
        latest_output = str(latest_session.get("output_file", "")).strip() if isinstance(latest_session, dict) else ""

        for candidate_path in self._list_formal_output_files(building_name):
            inferred = self._infer_duty_context_from_output(building_name, candidate_path)
            if inferred is None:
                continue
            duty_date, duty_shift = inferred
            session_id = self.build_session_id(building_name, duty_date, duty_shift)
            candidate_text = str(candidate_path)
            existing_raw = sessions.get(session_id, {})
            if isinstance(existing_raw, dict):
                existing_session = self._normalize_session(existing_raw)
                existing_output = str(existing_session.get("output_file", "")).strip()
                if existing_output == candidate_text:
                    if latest_session_id != session_id:
                        try:
                            self._apply_review_state_changes(
                                latest_by_building={building_name: session_id},
                                latest_batch_key=str(existing_session.get("batch_key", "")).strip() or None,
                            )
                        except Exception:  # noqa: BLE001
                            pass
                    return existing_session
            if latest_output == candidate_text and isinstance(latest_session, dict):
                return latest_session
            try:
                return self.register_generated_output(
                    building=building_name,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    data_file="",
                    output_file=candidate_text,
                    source_mode="recovered_from_output",
                )
            except Exception:  # noqa: BLE001
                return self._build_recovered_output_session(
                    building=building_name,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    output_file=candidate_text,
                )
        return latest_session

    def _recover_session_from_output_file(self, building: str, duty_date: str, duty_shift: str) -> Dict[str, Any] | None:
        building_name = str(building or "").strip()
        duty_date_text = str(duty_date or "").strip()
        duty_shift_text = str(duty_shift or "").strip().lower()
        if not building_name or not duty_date_text or not duty_shift_text:
            return None

        target_session_id = self.build_session_id(building_name, duty_date_text, duty_shift_text)
        existing = self.get_session_by_id(target_session_id)
        if isinstance(existing, dict):
            return existing

        for candidate_path in self._list_formal_output_files(building_name):
            inferred = self._infer_duty_context_from_output(building_name, candidate_path)
            if inferred != (duty_date_text, duty_shift_text):
                continue
            output_file = str(candidate_path)
            try:
                return self.register_generated_output(
                    building=building_name,
                    duty_date=duty_date_text,
                    duty_shift=duty_shift_text,
                    data_file="",
                    output_file=output_file,
                    source_mode="recovered_from_output",
                )
            except Exception:  # noqa: BLE001
                return self._build_recovered_output_session(
                    building=building_name,
                    duty_date=duty_date_text,
                    duty_shift=duty_shift_text,
                    output_file=output_file,
                )
        return None

    def _recover_all_sessions_from_output_files(self, building: str) -> None:
        building_name = str(building or "").strip()
        if not building_name:
            return
        for candidate_path in self._list_formal_output_files(building_name):
            inferred = self._infer_duty_context_from_output(building_name, candidate_path)
            if inferred is None:
                continue
            duty_date, duty_shift = inferred
            session_id = self.build_session_id(building_name, duty_date, duty_shift)
            if isinstance(self.get_session_by_id(session_id), dict):
                continue
            try:
                self.register_generated_output(
                    building=building_name,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    data_file="",
                    output_file=str(candidate_path),
                    source_mode="recovered_from_output",
                )
            except Exception:  # noqa: BLE001
                continue

    def _build_recovered_output_session(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        output_file: str,
    ) -> Dict[str, Any]:
        return self._normalize_session(
            {
                "session_id": self.build_session_id(building, duty_date, duty_shift),
                "building": building,
                "building_code": self._building_to_code().get(building, ""),
                "duty_date": duty_date,
                "duty_shift": duty_shift,
                "batch_key": self.build_batch_key(duty_date, duty_shift),
                "data_file": "",
                "output_file": str(output_file or "").strip(),
                "source_mode": "recovered_from_output",
                "revision": 1,
                "confirmed": False,
                "confirmed_at": "",
                "confirmed_by": "",
                "updated_at": _now_text(),
                "cloud_sheet_sync": {},
                "source_file_cache": {},
                "source_data_attachment_export": {},
            }
        )

    def _building_defs(self) -> List[Dict[str, str]]:
        review_ui = self._review_cfg()
        items = review_ui.get("buildings", [])
        output: List[Dict[str, str]] = []
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                code = str(item.get("code", "")).strip().lower()
                name = str(item.get("name", "")).strip()
                if code and name:
                    output.append({"code": code, "name": name})
        return output or list(self.DEFAULT_BUILDINGS)

    def _code_to_building(self) -> Dict[str, str]:
        return {item["code"]: item["name"] for item in self._building_defs()}

    def _building_to_code(self) -> Dict[str, str]:
        return {item["name"]: item["code"] for item in self._building_defs()}

    @staticmethod
    def build_session_id(building: str, duty_date: str, duty_shift: str) -> str:
        return f"{building}|{duty_date}|{duty_shift}"

    @staticmethod
    def build_batch_key(duty_date: str, duty_shift: str) -> str:
        return f"{duty_date}|{duty_shift}"

    @staticmethod
    def parse_batch_key(batch_key: str) -> tuple[str, str]:
        text = str(batch_key or "").strip()
        if "|" not in text:
            return "", ""
        duty_date, duty_shift = text.split("|", 1)
        return str(duty_date or "").strip(), str(duty_shift or "").strip().lower()

    @staticmethod
    def _normalize_cloud_sheet_sync(raw: Dict[str, Any] | None) -> Dict[str, Any]:
        payload = raw if isinstance(raw, dict) else {}
        merges = payload.get("synced_merges", [])
        normalized_merges: List[Dict[str, int]] = []
        if isinstance(merges, list):
            for item in merges:
                if not isinstance(item, dict):
                    continue
                try:
                    start_row = int(item.get("start_row_index", 0))
                    end_row = int(item.get("end_row_index", 0))
                    start_col = int(item.get("start_column_index", 0))
                    end_col = int(item.get("end_column_index", 0))
                except (TypeError, ValueError):
                    continue
                if end_row <= start_row or end_col <= start_col:
                    continue
                normalized_merges.append(
                    {
                        "start_row_index": start_row,
                        "end_row_index": end_row,
                        "start_column_index": start_col,
                        "end_column_index": end_col,
                    }
                )
        return {
            "attempted": bool(payload.get("attempted", False)),
            "success": bool(payload.get("success", False)),
            "status": str(payload.get("status", "")).strip(),
            "spreadsheet_token": str(payload.get("spreadsheet_token", "")).strip(),
            "spreadsheet_url": str(payload.get("spreadsheet_url", "")).strip(),
            "spreadsheet_title": str(payload.get("spreadsheet_title", "")).strip(),
            "sheet_title": str(payload.get("sheet_title", "")).strip(),
            "synced_revision": int(payload.get("synced_revision", 0) or 0),
            "last_attempt_revision": int(payload.get("last_attempt_revision", 0) or 0),
            "prepared_at": str(payload.get("prepared_at", "")).strip(),
            "updated_at": str(payload.get("updated_at", "")).strip(),
            "error": str(payload.get("error", "")).strip(),
            "synced_row_count": int(payload.get("synced_row_count", 0) or 0),
            "synced_column_count": int(payload.get("synced_column_count", 0) or 0),
            "synced_merges": normalized_merges,
            "dynamic_merge_signature": str(payload.get("dynamic_merge_signature", "")).strip(),
        }

    @staticmethod
    def _normalize_cloud_batch(raw: Dict[str, Any] | None) -> Dict[str, Any]:
        payload = raw if isinstance(raw, dict) else {}
        return {
            "batch_key": str(payload.get("batch_key", "")).strip(),
            "duty_date": str(payload.get("duty_date", "")).strip(),
            "duty_shift": str(payload.get("duty_shift", "")).strip().lower(),
            "status": str(payload.get("status", "")).strip(),
            "spreadsheet_token": str(payload.get("spreadsheet_token", "")).strip(),
            "spreadsheet_url": str(payload.get("spreadsheet_url", "")).strip(),
            "spreadsheet_title": str(payload.get("spreadsheet_title", "")).strip(),
            "prepared_at": str(payload.get("prepared_at", "")).strip(),
            "updated_at": str(payload.get("updated_at", "")).strip(),
            "error": str(payload.get("error", "")).strip(),
            "first_full_cloud_sync_completed": bool(payload.get("first_full_cloud_sync_completed", False)),
            "first_full_cloud_sync_at": str(payload.get("first_full_cloud_sync_at", "")).strip(),
        }

    def _cloud_sync_enabled(self) -> bool:
        cloud_cfg = self.config.get("cloud_sheet_sync", {})
        if not isinstance(cloud_cfg, dict):
            return True
        return bool(cloud_cfg.get("enabled", True))

    def _cloud_sheet_title_for_building(self, building: str) -> str:
        cloud_cfg = self.config.get("cloud_sheet_sync", {})
        names = cloud_cfg.get("sheet_names", {}) if isinstance(cloud_cfg, dict) and isinstance(cloud_cfg.get("sheet_names", {}), dict) else {}
        title = str(names.get(str(building or "").strip(), "")).strip()
        return title or str(building or "").strip()

    def _build_pending_cloud_sync(
        self,
        *,
        building: str,
        revision: int,
        previous_cloud_sync: Dict[str, Any] | None = None,
        batch_cloud: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        previous = self._normalize_cloud_sheet_sync(previous_cloud_sync)
        if not self._cloud_sync_enabled():
            return self._normalize_cloud_sheet_sync(
                {
                    **previous,
                    "attempted": False,
                    "success": False,
                    "status": "disabled",
                    "sheet_title": self._cloud_sheet_title_for_building(building),
                    "updated_at": _now_text(),
                    "error": "",
                }
            )

        batch_meta = self._normalize_cloud_batch(batch_cloud)
        batch_status = str(batch_meta.get("status", "")).strip().lower()
        status = "pending_upload"
        attempted = False
        error = ""
        if batch_status == "prepare_failed":
            status = "prepare_failed"
            attempted = True
            error = str(batch_meta.get("error", "")).strip()

        return self._normalize_cloud_sheet_sync(
            {
                **previous,
                "attempted": attempted,
                "success": False,
                "status": status,
                "spreadsheet_token": str(batch_meta.get("spreadsheet_token", "")).strip()
                or previous.get("spreadsheet_token", ""),
                "spreadsheet_url": str(batch_meta.get("spreadsheet_url", "")).strip()
                or previous.get("spreadsheet_url", ""),
                "spreadsheet_title": str(batch_meta.get("spreadsheet_title", "")).strip()
                or previous.get("spreadsheet_title", ""),
                "sheet_title": self._cloud_sheet_title_for_building(building),
                "synced_revision": int(previous.get("synced_revision", 0) or 0),
                "last_attempt_revision": int(previous.get("last_attempt_revision", 0) or 0),
                "prepared_at": str(batch_meta.get("prepared_at", "")).strip() or previous.get("prepared_at", ""),
                "updated_at": _now_text(),
                "error": error,
                "synced_row_count": int(previous.get("synced_row_count", 0) or 0),
                "synced_column_count": int(previous.get("synced_column_count", 0) or 0),
                "synced_merges": previous.get("synced_merges", []),
                "dynamic_merge_signature": str(previous.get("dynamic_merge_signature", "")).strip(),
            }
        )

    @staticmethod
    def _normalize_source_data_attachment_export(raw: Dict[str, Any] | None) -> Dict[str, Any]:
        payload = raw if isinstance(raw, dict) else {}
        return {
            "status": str(payload.get("status", "pending_review")).strip() or "pending_review",
            "reason": str(payload.get("reason", "await_all_confirmed")).strip(),
            "uploaded_count": int(payload.get("uploaded_count", 0) or 0),
            "error": str(payload.get("error", "")).strip(),
            "uploaded_at": str(payload.get("uploaded_at", "")).strip(),
            "uploaded_revision": int(payload.get("uploaded_revision", 0) or 0),
            "frozen_after_first_full_cloud_sync": bool(payload.get("frozen_after_first_full_cloud_sync", False)),
        }

    @staticmethod
    def _normalize_source_file_cache(raw: Dict[str, Any] | None) -> Dict[str, Any]:
        payload = raw if isinstance(raw, dict) else {}
        managed = bool(payload.get("managed", False))
        cleanup_status = str(payload.get("cleanup_status", "")).strip().lower()
        if managed and cleanup_status not in {"active", "removed", "missing"}:
            cleanup_status = "active"
        if not managed:
            cleanup_status = ""
        return {
            "managed": managed,
            "stored_path": str(payload.get("stored_path", "")).strip(),
            "original_name": str(payload.get("original_name", "")).strip(),
            "stored_at": str(payload.get("stored_at", "")).strip(),
            "cleanup_status": cleanup_status,
            "cleanup_at": str(payload.get("cleanup_at", "")).strip(),
        }

    @staticmethod
    def _derive_capacity_sync_from_legacy_fields(raw: Dict[str, Any] | None) -> Dict[str, Any]:
        payload = raw if isinstance(raw, dict) else {}
        capacity_output_file = str(payload.get("capacity_output_file", "") or "").strip()
        capacity_status = str(payload.get("capacity_status", "") or "").strip().lower()
        capacity_error = str(payload.get("capacity_error", "") or "").strip()
        if not capacity_output_file:
            status = "missing_file"
            error = capacity_error or "交接班容量报表尚未生成"
        elif capacity_status in {"ok", "success"}:
            status = "ready"
            error = ""
        elif capacity_status in {"pending", "pending_input", "missing_file", "failed"}:
            status = capacity_status
            error = capacity_error
        elif capacity_status == "skipped":
            status = "missing_file"
            error = capacity_error or "交接班容量报表尚未生成"
        else:
            status = "failed"
            error = capacity_error
        return {
            "status": status,
            "updated_at": str(payload.get("updated_at", "") or "").strip(),
            "error": error,
            "tracked_cells": list(_CAPACITY_SYNC_TRACKED_CELLS),
            "input_signature": str(payload.get("capacity_input_signature", "") or "").strip(),
        }

    @staticmethod
    def _normalize_capacity_sync(raw: Dict[str, Any] | None, fallback: Dict[str, Any] | None = None) -> Dict[str, Any]:
        source = raw if isinstance(raw, dict) else {}
        backup = fallback if isinstance(fallback, dict) else {}
        merged: Dict[str, Any] = {}
        merged.update(backup)
        merged.update(source)
        status = str(merged.get("status", "") or "").strip().lower()
        if status not in {"ready", "pending", "pending_input", "missing_file", "failed"}:
            status = str(backup.get("status", "") or "failed").strip().lower()
            if status not in {"ready", "pending", "pending_input", "missing_file", "failed"}:
                status = "failed"
        return {
            "status": status,
            "updated_at": str(merged.get("updated_at", "") or "").strip(),
            "error": str(merged.get("error", "") or "").strip(),
            "tracked_cells": list(_CAPACITY_SYNC_TRACKED_CELLS),
            "input_signature": str(merged.get("input_signature", "") or "").strip(),
        }

    def _normalize_session(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        building = str(raw.get("building", "")).strip()
        duty_date = str(raw.get("duty_date", "")).strip()
        duty_shift = str(raw.get("duty_shift", "")).strip().lower()
        building_code = str(raw.get("building_code", "")).strip().lower()
        if not building_code and building:
            building_code = self._building_to_code().get(building, "")
        batch_key = str(raw.get("batch_key", "")).strip() or self.build_batch_key(duty_date, duty_shift)
        session_id = str(raw.get("session_id", "")).strip() or self.build_session_id(building, duty_date, duty_shift)
        legacy_capacity_sync = self._derive_capacity_sync_from_legacy_fields(raw)
        capacity_sync = self._normalize_capacity_sync(raw.get("capacity_sync", {}), fallback=legacy_capacity_sync)
        return {
            "session_id": session_id,
            "building": building,
            "building_code": building_code,
            "duty_date": duty_date,
            "duty_shift": duty_shift,
            "batch_key": batch_key,
            "output_file": str(raw.get("output_file", "")).strip(),
            "capacity_output_file": str(raw.get("capacity_output_file", "")).strip(),
            "capacity_status": str(raw.get("capacity_status", "")).strip().lower(),
            "capacity_error": str(raw.get("capacity_error", "")).strip(),
            "capacity_warnings": [
                str(item or "").strip()
                for item in (raw.get("capacity_warnings", []) if isinstance(raw.get("capacity_warnings", []), list) else [])
                if str(item or "").strip()
            ],
            "capacity_sync": capacity_sync,
            "data_file": str(raw.get("data_file", "")).strip(),
            "source_mode": str(raw.get("source_mode", "")).strip(),
            "revision": int(raw.get("revision", 1) or 1),
            "confirmed": bool(raw.get("confirmed", False)),
            "confirmed_at": str(raw.get("confirmed_at", "")).strip(),
            "confirmed_by": str(raw.get("confirmed_by", "")).strip(),
            "updated_at": str(raw.get("updated_at", "")).strip(),
            "cloud_sheet_sync": self._normalize_cloud_sheet_sync(raw.get("cloud_sheet_sync", {})),
            "source_file_cache": self._normalize_source_file_cache(raw.get("source_file_cache", {})),
            "source_data_attachment_export": self._normalize_source_data_attachment_export(
                raw.get("source_data_attachment_export", {})
            ),
            "review_link_delivery": _normalize_review_link_delivery(raw.get("review_link_delivery", {})),
        }

    def _managed_source_file_references(self, state: Dict[str, Any]) -> set[str]:
        references: set[str] = set()
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict):
            return references
        for raw in list(sessions.values()):
            if not isinstance(raw, dict):
                continue
            session = self._normalize_session(raw)
            source_cache = session.get("source_file_cache", {})
            if not isinstance(source_cache, dict):
                continue
            if not bool(source_cache.get("managed", False)):
                continue
            stored_path = str(source_cache.get("stored_path", "")).strip()
            if stored_path:
                references.add(stored_path)
        return references

    def _refresh_source_file_cache_state(self, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict):
            return []
        changed_sessions: List[Dict[str, Any]] = []
        for session_id, raw in list(sessions.items()):
            if not isinstance(raw, dict):
                continue
            session = self._normalize_session(raw)
            source_cache = session.get("source_file_cache", {})
            if not isinstance(source_cache, dict) or not bool(source_cache.get("managed", False)):
                continue
            stored_path = str(source_cache.get("stored_path", "")).strip()
            if not stored_path:
                continue
            path = Path(stored_path)
            if path.exists():
                continue
            if str(source_cache.get("cleanup_status", "")).strip().lower() == "missing":
                continue
            source_cache["cleanup_status"] = "missing"
            source_cache["cleanup_at"] = _now_text()
            session["source_file_cache"] = source_cache
            sessions[session_id] = session
            changed_sessions.append(session)
        if changed_sessions:
            state["review_sessions"] = sessions
        return changed_sessions

    def _rebuild_latest_by_building(self, state: Dict[str, Any]) -> None:
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict):
            state["review_latest_by_building"] = {}
            return

        latest_by_building: Dict[str, Dict[str, Any]] = {}
        for raw in list(sessions.values()):
            if not isinstance(raw, dict):
                continue
            session = self._normalize_session(raw)
            building = str(session.get("building", "")).strip()
            output_file = str(session.get("output_file", "")).strip()
            if not building or self._is_legacy_test_output_file(output_file):
                continue
            current_best = latest_by_building.get(building)
            if current_best is None:
                latest_by_building[building] = session
                continue
            current_key = (
                str(current_best.get("duty_date", "")),
                self._parse_updated_at(str(current_best.get("updated_at", ""))),
                int(current_best.get("revision", 0) or 0),
                str(current_best.get("duty_shift", "")),
            )
            candidate_key = (
                str(session.get("duty_date", "")),
                self._parse_updated_at(str(session.get("updated_at", ""))),
                int(session.get("revision", 0) or 0),
                str(session.get("duty_shift", "")),
            )
            if candidate_key >= current_key:
                latest_by_building[building] = session

        state["review_latest_by_building"] = {
            building: str(session.get("session_id", "")).strip()
            for building, session in latest_by_building.items()
            if str(session.get("session_id", "")).strip()
        }

    @staticmethod
    def _latest_by_building_delta(
        current_latest: Dict[str, Any],
        rebuilt_latest: Dict[str, Any],
    ) -> Dict[str, str | None]:
        current = current_latest if isinstance(current_latest, dict) else {}
        rebuilt = rebuilt_latest if isinstance(rebuilt_latest, dict) else {}
        delta: Dict[str, str | None] = {}
        for building in sorted(set(current) | set(rebuilt)):
            building_text = str(building or "").strip()
            if not building_text:
                continue
            current_session_id = str(current.get(building_text, "") or "").strip()
            rebuilt_session_id = str(rebuilt.get(building_text, "") or "").strip()
            if current_session_id == rebuilt_session_id:
                continue
            delta[building_text] = rebuilt_session_id or None
        return delta

    def _load_state(self) -> Dict[str, Any]:
        try:
            state = self._review_state_store.load_state()
        except Exception as exc:  # noqa: BLE001
            _reraise_review_store_error(exc)
        if not isinstance(state.get("review_cloud_batches", {}), dict):
            state["review_cloud_batches"] = {}
        if not isinstance(state.get("review_latest_batch_key", ""), str):
            state["review_latest_batch_key"] = ""
        sessions = state.get("review_sessions", {})
        filtered_changed = False
        removed_session_ids: List[str] = []
        if isinstance(sessions, dict):
            filtered_sessions: Dict[str, Any] = {}
            for session_id, raw_session in list(sessions.items()):
                session_id_text = str(session_id or "").strip()
                if not isinstance(raw_session, dict):
                    if session_id_text:
                        removed_session_ids.append(session_id_text)
                    filtered_changed = True
                    continue
                output_file = str(raw_session.get("output_file", "")).strip()
                if self._is_legacy_test_output_file(output_file):
                    if session_id_text:
                        removed_session_ids.append(session_id_text)
                    filtered_changed = True
                    continue
                filtered_sessions[session_id_text] = raw_session
            if filtered_changed:
                state["review_sessions"] = filtered_sessions
        refreshed_sessions = self._refresh_source_file_cache_state(state)
        self._source_file_cache_service.cleanup_orphan_sources(
            referenced_paths=self._managed_source_file_references(state),
            emit_log=lambda *_args: None,
        )
        rebuilt_latest: Dict[str, Any] = {}
        rebuild_source = state.get("review_sessions", {})
        if isinstance(rebuild_source, dict):
            temp_state = {"review_sessions": rebuild_source}
            self._rebuild_latest_by_building(temp_state)
            rebuilt_latest = temp_state.get("review_latest_by_building", {})
        current_latest = state.get("review_latest_by_building", {})
        latest_delta = self._latest_by_building_delta(current_latest, rebuilt_latest)
        if latest_delta:
            state["review_latest_by_building"] = rebuilt_latest
        derived_latest_batch_key = self._derive_latest_batch_key_from_state(state)
        current_latest_batch_key = str(state.get("review_latest_batch_key", "") or "").strip()
        latest_batch_key_changed = current_latest_batch_key != derived_latest_batch_key
        if latest_batch_key_changed:
            state["review_latest_batch_key"] = derived_latest_batch_key
        if filtered_changed or refreshed_sessions or latest_delta or latest_batch_key_changed:
            return self._apply_review_state_changes(
                upsert_sessions=refreshed_sessions or None,
                delete_session_ids=removed_session_ids or None,
                latest_by_building=latest_delta or None,
                latest_batch_key=derived_latest_batch_key if latest_batch_key_changed else None,
            )
        return state

    def _rebuild_batch_status(self, state: Dict[str, Any]) -> None:
        building_defs = self._building_defs()
        building_names = [item["name"] for item in building_defs]
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict):
            sessions = {}

        grouped: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for raw_session in list(sessions.values()):
            if not isinstance(raw_session, dict):
                continue
            session = self._normalize_session(raw_session)
            batch_key = session["batch_key"]
            building = session["building"]
            if not batch_key or not building:
                continue
            grouped.setdefault(batch_key, {})[building] = session

        batch_status: Dict[str, Any] = {}
        for batch_key, session_map in grouped.items():
            rows: List[Dict[str, Any]] = []
            confirmed_count = 0
            sample_session = next(iter(session_map.values()), {})
            duty_date = str(sample_session.get("duty_date", "")).strip() if isinstance(sample_session, dict) else ""
            duty_shift = str(sample_session.get("duty_shift", "")).strip().lower() if isinstance(sample_session, dict) else ""
            if not duty_date or not duty_shift:
                duty_date, duty_shift = self.parse_batch_key(batch_key)
            for building in building_names:
                session = session_map.get(building)
                has_session = isinstance(session, dict)
                confirmed = bool(session.get("confirmed", False)) if has_session else False
                if confirmed:
                    confirmed_count += 1
                rows.append(
                    {
                        "building": building,
                        "has_session": has_session,
                        "confirmed": confirmed,
                        "session_id": str(session.get("session_id", "")).strip() if has_session else "",
                        "revision": int(session.get("revision", 0) or 0) if has_session else 0,
                        "updated_at": str(session.get("updated_at", "")).strip() if has_session else "",
                        "review_link_delivery": (
                            _normalize_review_link_delivery(session.get("review_link_delivery", {}))
                            if has_session
                            else _normalize_review_link_delivery({})
                        ),
                        "cloud_sheet_sync": (
                            self._normalize_cloud_sheet_sync(session.get("cloud_sheet_sync", {}))
                            if has_session
                            else self._normalize_cloud_sheet_sync({})
                        ),
                    }
                )
            required_count = len(building_names)
            has_any_session = any(row["has_session"] for row in rows)
            all_confirmed = confirmed_count == required_count and all(row["has_session"] for row in rows)
            batch_status[batch_key] = {
                "batch_key": batch_key,
                "duty_date": duty_date,
                "duty_shift": duty_shift,
                "has_any_session": has_any_session,
                "confirmed_count": confirmed_count,
                "required_count": required_count,
                "all_confirmed": all_confirmed,
                "ready_for_followup_upload": all_confirmed,
                "buildings": rows,
                "updated_at": _now_text(),
            }
        state["review_batch_status"] = batch_status

    @staticmethod
    def _derive_latest_batch_key_from_state(state: Dict[str, Any]) -> str:
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict):
            return ""
        best_session: Dict[str, Any] | None = None
        for raw_session in list(sessions.values()):
            if not isinstance(raw_session, dict):
                continue
            batch_key = str(raw_session.get("batch_key", "") or "").strip()
            if not batch_key:
                continue
            if best_session is None:
                best_session = raw_session
                continue
            current_key = (
                str(best_session.get("duty_date", "") or "").strip(),
                _safe_datetime_text(best_session.get("updated_at", "")),
                int(best_session.get("revision", 0) or 0),
                str(best_session.get("duty_shift", "") or "").strip().lower(),
            )
            candidate_key = (
                str(raw_session.get("duty_date", "") or "").strip(),
                _safe_datetime_text(raw_session.get("updated_at", "")),
                int(raw_session.get("revision", 0) or 0),
                str(raw_session.get("duty_shift", "") or "").strip().lower(),
            )
            if candidate_key >= current_key:
                best_session = raw_session
        if not isinstance(best_session, dict):
            return ""
        return str(best_session.get("batch_key", "") or "").strip()

    @staticmethod
    def _set_latest_batch_key(state: Dict[str, Any], batch_key: str) -> None:
        target_batch = str(batch_key or "").strip()
        if target_batch:
            state["review_latest_batch_key"] = target_batch

    def _save_state(self, state: Dict[str, Any]) -> Dict[str, Any]:
        self._rebuild_latest_by_building(state)
        self._rebuild_batch_status(state)
        latest_batch_key = str(state.get("review_latest_batch_key", "") or "").strip()
        batch_status = state.get("review_batch_status", {})
        if not latest_batch_key or not isinstance(batch_status, dict) or latest_batch_key not in batch_status:
            state["review_latest_batch_key"] = self._derive_latest_batch_key_from_state(state)
        try:
            return self._review_state_store.save_state(state)
        except Exception as exc:  # noqa: BLE001
            _reraise_review_store_error(exc)

    def _apply_review_state_changes(
        self,
        *,
        upsert_sessions: List[Dict[str, Any]] | None = None,
        delete_session_ids: List[str] | None = None,
        latest_by_building: Dict[str, str | None] | None = None,
        upsert_cloud_batches: List[Dict[str, Any]] | None = None,
        delete_cloud_batch_keys: List[str] | None = None,
        latest_batch_key: str | None = None,
    ) -> Dict[str, Any]:
        meta_updates = {"review_latest_batch_key": latest_batch_key} if latest_batch_key is not None else None
        try:
            return self._review_state_store.apply_changes(
                upsert_sessions=upsert_sessions,
                delete_session_ids=delete_session_ids,
                latest_by_building=latest_by_building,
                upsert_cloud_batches=upsert_cloud_batches,
                delete_cloud_batch_keys=delete_cloud_batch_keys,
                meta_updates=meta_updates,
            )
        except Exception as exc:  # noqa: BLE001
            _reraise_review_store_error(exc)

    def get_building_by_code(self, building_code: str) -> str:
        code = str(building_code or "").strip().lower()
        return self._code_to_building().get(code, "")

    def list_buildings(self) -> List[str]:
        return [
            str(item.get("name", "")).strip()
            for item in self._building_defs()
            if str(item.get("name", "")).strip()
        ]

    def get_batch_status(self, batch_key: str) -> Dict[str, Any]:
        key = str(batch_key or "").strip()
        state = self._load_state()
        self._rebuild_batch_status(state)
        batch_status = state.get("review_batch_status", {})
        if isinstance(batch_status, dict) and isinstance(batch_status.get(key), dict):
            return dict(batch_status[key])
        building_defs = self._building_defs()
        duty_date, duty_shift = self.parse_batch_key(key)
        return {
            "batch_key": key,
            "duty_date": duty_date,
            "duty_shift": duty_shift,
            "has_any_session": False,
            "confirmed_count": 0,
            "required_count": len(building_defs),
            "all_confirmed": False,
            "ready_for_followup_upload": False,
            "buildings": [
                {
                    "building": item["name"],
                    "has_session": False,
                    "confirmed": False,
                    "session_id": "",
                    "revision": 0,
                    "updated_at": "",
                    "cloud_sheet_sync": self._normalize_cloud_sheet_sync({}),
                }
                for item in building_defs
            ],
        }

    def get_batch_status_for_duty(self, duty_date: str, duty_shift: str) -> Dict[str, Any]:
        duty_date_text = str(duty_date or "").strip()
        duty_shift_text = str(duty_shift or "").strip().lower()
        if not duty_date_text or not duty_shift_text:
            return self.get_latest_batch_status()
        return self.get_batch_status(self.build_batch_key(duty_date_text, duty_shift_text))

    def list_batch_sessions(self, batch_key: str) -> List[Dict[str, Any]]:
        target_batch = str(batch_key or "").strip()
        if not target_batch:
            return []
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict):
            return []
        output: List[Dict[str, Any]] = []
        for raw in list(sessions.values()):
            if not isinstance(raw, dict):
                continue
            session = self._normalize_session(raw)
            if str(session.get("batch_key", "")).strip() == target_batch:
                output.append(session)
        output.sort(key=lambda item: str(item.get("building", "")))
        return output

    def list_sessions(self) -> List[Dict[str, Any]]:
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict):
            return []
        output: List[Dict[str, Any]] = []
        for raw in list(sessions.values()):
            if not isinstance(raw, dict):
                continue
            output.append(self._normalize_session(raw))
        output.sort(
            key=lambda item: (
                str(item.get("duty_date", "")),
                str(item.get("duty_shift", "")),
                str(item.get("building", "")),
                self._parse_updated_at(str(item.get("updated_at", ""))),
            ),
            reverse=True,
        )
        return output

    def get_latest_session(self, building: str) -> Dict[str, Any] | None:
        return self._get_latest_session(building, allow_recover=True)

    def get_latest_session_fast(self, building: str) -> Dict[str, Any] | None:
        return self._get_latest_session(building, allow_recover=False)

    def _get_latest_session(self, building: str, *, allow_recover: bool) -> Dict[str, Any] | None:
        building_name = str(building or "").strip()
        if not building_name:
            return None
        state = self._load_state()
        return self._get_latest_session_from_state(
            state,
            building_name,
            allow_recover=allow_recover,
        )

    def _get_latest_session_from_state(
        self,
        state: Dict[str, Any],
        building: str,
        *,
        allow_recover: bool,
    ) -> Dict[str, Any] | None:
        building_name = str(building or "").strip()
        if not building_name:
            return None
        latest_map = state.get("review_latest_by_building", {})
        sessions = state.get("review_sessions", {})
        if isinstance(latest_map, dict) and isinstance(sessions, dict):
            session_id = str(latest_map.get(building_name, "")).strip()
            raw_session = sessions.get(session_id, {})
            if session_id and isinstance(raw_session, dict):
                return self._normalize_session(raw_session)
        if not allow_recover:
            return None
        recovered = self._recover_latest_session_from_output_file(building_name)
        if isinstance(recovered, dict):
            return recovered
        return None

    def _latest_session_id_from_state(self, state: Dict[str, Any], building: str) -> str:
        building_name = str(building or "").strip()
        if not building_name:
            return ""
        latest_map = state.get("review_latest_by_building", {})
        sessions = state.get("review_sessions", {})
        if not isinstance(latest_map, dict) or not isinstance(sessions, dict):
            return ""
        session_id = str(latest_map.get(building_name, "")).strip()
        raw_session = sessions.get(session_id, {})
        if not session_id or not isinstance(raw_session, dict):
            return ""
        if str(raw_session.get("building", "")).strip() != building_name:
            return ""
        return session_id

    def _list_building_sessions_from_state(
        self,
        state: Dict[str, Any],
        building: str,
    ) -> List[Dict[str, Any]]:
        building_name = str(building or "").strip()
        if not building_name:
            return []
        latest_session_id = self._latest_session_id_from_state(state, building_name)
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict):
            return []

        output: List[Dict[str, Any]] = []
        for raw in list(sessions.values()):
            if not isinstance(raw, dict):
                continue
            session = self._normalize_session(raw)
            if str(session.get("building", "")).strip() != building_name:
                continue
            output.append(session)

        output.sort(
            key=lambda item: (
                str(item.get("duty_date", "")),
                2 if str(item.get("duty_shift", "")).strip().lower() == "night" else 1,
                self._parse_updated_at(str(item.get("updated_at", ""))),
                int(item.get("revision", 0) or 0),
            ),
            reverse=True,
        )
        if latest_session_id:
            output.sort(
                key=lambda item: 0 if str(item.get("session_id", "")).strip() == latest_session_id else 1
            )
        return output

    def get_latest_session_for_context(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
    ) -> Dict[str, Any] | None:
        return self._get_latest_session_for_context(
            building=building,
            duty_date=duty_date,
            duty_shift=duty_shift,
            allow_recover=True,
        )

    def get_latest_session_for_context_fast(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
    ) -> Dict[str, Any] | None:
        return self._get_latest_session_for_context(
            building=building,
            duty_date=duty_date,
            duty_shift=duty_shift,
            allow_recover=False,
        )

    def _get_latest_session_for_context(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        allow_recover: bool,
    ) -> Dict[str, Any] | None:
        building_name = str(building or "").strip()
        duty_date_text = str(duty_date or "").strip()
        duty_shift_text = str(duty_shift or "").strip().lower()
        if not building_name or not duty_date_text or duty_shift_text not in {"day", "night"}:
            return None
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        if isinstance(sessions, dict):
            session_id = self.build_session_id(building_name, duty_date_text, duty_shift_text)
            raw_session = sessions.get(session_id, {})
            if isinstance(raw_session, dict):
                session = self._normalize_session(raw_session)
                if str(session.get("building", "")).strip() == building_name:
                    return session
        if not allow_recover:
            return None
        return self._recover_session_from_output_file(building_name, duty_date_text, duty_shift_text)

    def get_session_by_id(self, session_id: str) -> Dict[str, Any] | None:
        target_session_id = str(session_id or "").strip()
        if not target_session_id:
            return None
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict):
            return None
        if target_session_id not in sessions:
            return None
        raw_session = sessions.get(target_session_id, {})
        if not isinstance(raw_session, dict):
            return None
        return self._normalize_session(raw_session)

    def get_or_recover_session_by_id(self, session_id: str) -> Dict[str, Any] | None:
        target_session_id = str(session_id or "").strip()
        if not target_session_id:
            return None
        session = self.get_session_by_id(target_session_id)
        if isinstance(session, dict):
            return session
        try:
            building, duty_date, duty_shift = target_session_id.split("|", 2)
        except ValueError:
            return None
        duty_shift_text = str(duty_shift or "").strip().lower()
        if duty_shift_text not in {"day", "night"}:
            return None
        return self._recover_session_from_output_file(
            str(building or "").strip(),
            str(duty_date or "").strip(),
            duty_shift_text,
        )

    def get_session_concurrency(
        self,
        *,
        building: str,
        session_id: str,
        client_id: str = "",
    ) -> Dict[str, Any]:
        session = self.get_session_by_id(session_id)
        building_name = str(building or "").strip()
        if not isinstance(session, dict) or str(session.get("building", "")).strip() != building_name:
            raise ReviewSessionNotFoundError("review session not found")
        try:
            return self._review_state_store.get_concurrency(
                building=building_name,
                session_id=str(session.get("session_id", "")).strip(),
                current_revision=int(session.get("revision", 0) or 0),
                client_id=str(client_id or "").strip(),
            )
        except Exception as exc:  # noqa: BLE001
            _reraise_review_store_error(exc)

    def claim_session_lock(
        self,
        *,
        building: str,
        session_id: str,
        client_id: str,
        holder_label: str = "",
        lease_ttl_sec: int = 60,
    ) -> Dict[str, Any]:
        session = self.get_session_by_id(session_id)
        building_name = str(building or "").strip()
        if not isinstance(session, dict) or str(session.get("building", "")).strip() != building_name:
            raise ReviewSessionNotFoundError("review session not found")
        try:
            return self._review_state_store.claim_lock(
                building=building_name,
                session_id=str(session.get("session_id", "")).strip(),
                current_revision=int(session.get("revision", 0) or 0),
                client_id=str(client_id or "").strip(),
                holder_label=str(holder_label or "").strip(),
                lease_ttl_sec=lease_ttl_sec,
            )
        except Exception as exc:  # noqa: BLE001
            _reraise_review_store_error(exc)

    def heartbeat_session_lock(
        self,
        *,
        building: str,
        session_id: str,
        client_id: str,
        lease_ttl_sec: int = 60,
    ) -> Dict[str, Any]:
        session = self.get_session_by_id(session_id)
        building_name = str(building or "").strip()
        if not isinstance(session, dict) or str(session.get("building", "")).strip() != building_name:
            raise ReviewSessionNotFoundError("review session not found")
        try:
            return self._review_state_store.heartbeat_lock(
                building=building_name,
                session_id=str(session.get("session_id", "")).strip(),
                current_revision=int(session.get("revision", 0) or 0),
                client_id=str(client_id or "").strip(),
                lease_ttl_sec=lease_ttl_sec,
            )
        except Exception as exc:  # noqa: BLE001
            _reraise_review_store_error(exc)

    def release_session_lock(
        self,
        *,
        building: str,
        session_id: str,
        client_id: str,
    ) -> Dict[str, Any]:
        session = self.get_session_by_id(session_id)
        building_name = str(building or "").strip()
        if not isinstance(session, dict) or str(session.get("building", "")).strip() != building_name:
            return {
                "current_revision": 0,
                "active_editor": None,
                "lease_expires_at": "",
                "is_editing_elsewhere": False,
                "client_holds_lock": False,
                "released": False,
            }
        try:
            return self._review_state_store.release_lock(
                building=building_name,
                session_id=str(session.get("session_id", "")).strip(),
                current_revision=int(session.get("revision", 0) or 0),
                client_id=str(client_id or "").strip(),
            )
        except Exception as exc:  # noqa: BLE001
            _reraise_review_store_error(exc)

    def get_session_for_building_duty(self, building: str, duty_date: str, duty_shift: str) -> Dict[str, Any] | None:
        return self._get_session_for_building_duty(
            building,
            duty_date,
            duty_shift,
            allow_recover=True,
        )

    def get_session_for_building_duty_fast(self, building: str, duty_date: str, duty_shift: str) -> Dict[str, Any] | None:
        return self._get_session_for_building_duty(
            building,
            duty_date,
            duty_shift,
            allow_recover=False,
        )

    def _get_session_for_building_duty(
        self,
        building: str,
        duty_date: str,
        duty_shift: str,
        *,
        allow_recover: bool,
    ) -> Dict[str, Any] | None:
        building_name = str(building or "").strip()
        duty_date_text = str(duty_date or "").strip()
        duty_shift_text = str(duty_shift or "").strip().lower()
        if not building_name:
            return None
        if not duty_date_text or not duty_shift_text:
            return self._get_latest_session(building_name, allow_recover=allow_recover)
        target_session_id = self.build_session_id(building_name, duty_date_text, duty_shift_text)
        session = self.get_session_by_id(target_session_id)
        if isinstance(session, dict):
            return session
        if not allow_recover:
            return None
        return self._recover_session_from_output_file(building_name, duty_date_text, duty_shift_text)

    def get_latest_session_id(self, building: str) -> str:
        building_name = str(building or "").strip()
        if not building_name:
            return ""
        state = self._load_state()
        session_id = self._latest_session_id_from_state(state, building_name)
        if session_id:
            return session_id
        latest = self._get_latest_session_from_state(state, building_name, allow_recover=True)
        if not isinstance(latest, dict):
            return ""
        return str(latest.get("session_id", "")).strip()

    def get_latest_session_id_fast(self, building: str) -> str:
        building_name = str(building or "").strip()
        if not building_name:
            return ""
        state = self._load_state()
        return self._latest_session_id_from_state(state, building_name)

    def _session_has_successful_cloud_history(self, session: Dict[str, Any]) -> bool:
        cloud_sync = self._normalize_cloud_sheet_sync(session.get("cloud_sheet_sync", {}))
        status = str(cloud_sync.get("status", "")).strip().lower()
        spreadsheet_url = str(cloud_sync.get("spreadsheet_url", "")).strip()
        return status == "success" and bool(spreadsheet_url)

    def list_building_sessions(self, building: str) -> List[Dict[str, Any]]:
        building_name = str(building or "").strip()
        if not building_name:
            return []
        state = self._load_state()
        return self._list_building_sessions_from_state(state, building_name)

    def list_building_cloud_history_sessions(
        self,
        building: str,
        *,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        history_limit = max(0, int(limit or 0))
        if history_limit <= 0:
            return []

        output: List[Dict[str, Any]] = []
        for session in self.list_building_sessions(building):
            if not self._session_has_successful_cloud_history(session):
                continue
            output.append(session)
            if len(output) >= history_limit:
                break
        return output

    @staticmethod
    def _parse_updated_at(value: str) -> datetime:
        text = str(value or "").strip()
        if not text:
            return datetime.min
        try:
            return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return datetime.min

    def get_latest_batch_status(self) -> Dict[str, Any]:
        state = self._load_state()
        self._rebuild_batch_status(state)

        batch_status = state.get("review_batch_status", {})
        if not isinstance(batch_status, dict):
            return self.get_batch_status("")

        latest_batch_key = str(state.get("review_latest_batch_key", "") or "").strip()
        if latest_batch_key and isinstance(batch_status.get(latest_batch_key), dict):
            return dict(batch_status[latest_batch_key])
        latest_batch_key = self._derive_latest_batch_key_from_state(state)
        if latest_batch_key and isinstance(batch_status.get(latest_batch_key), dict):
            return dict(batch_status[latest_batch_key])
        return self.get_batch_status(latest_batch_key)

    def register_cloud_batch(
        self,
        *,
        batch_key: str,
        duty_date: str,
        duty_shift: str,
        cloud_batch: Dict[str, Any],
    ) -> Dict[str, Any]:
        target_batch = str(batch_key or "").strip()
        if not target_batch:
            raise ValueError("batch_key 不能为空")
        state = self._load_state()
        cloud_batches = state.get("review_cloud_batches", {})
        if not isinstance(cloud_batches, dict):
            cloud_batches = {}
        normalized = self._normalize_cloud_batch(
            {
                **(cloud_batch if isinstance(cloud_batch, dict) else {}),
                "batch_key": target_batch,
                "duty_date": str(duty_date or "").strip(),
                "duty_shift": str(duty_shift or "").strip().lower(),
                "updated_at": _now_text(),
            }
        )
        self._apply_review_state_changes(
            upsert_cloud_batches=[normalized],
            latest_batch_key=target_batch,
        )
        return dict(normalized)

    def get_cloud_batch(self, batch_key: str) -> Dict[str, Any] | None:
        target_batch = str(batch_key or "").strip()
        if not target_batch:
            return None
        state = self._load_state()
        cloud_batches = state.get("review_cloud_batches", {})
        if not isinstance(cloud_batches, dict):
            return None
        raw = cloud_batches.get(target_batch, {})
        if not isinstance(raw, dict):
            return None
        normalized = self._normalize_cloud_batch(raw)
        if not normalized.get("batch_key"):
            normalized["batch_key"] = target_batch
        return normalized

    def is_first_full_cloud_sync_completed(self, batch_key: str) -> bool:
        batch_meta = self.get_cloud_batch(batch_key)
        if not isinstance(batch_meta, dict):
            return False
        return bool(batch_meta.get("first_full_cloud_sync_completed", False))

    def mark_first_full_cloud_sync_completed(self, *, batch_key: str) -> Dict[str, Any] | None:
        target_batch = str(batch_key or "").strip()
        if not target_batch:
            return None
        state = self._load_state()
        cloud_batches = state.get("review_cloud_batches", {})
        if not isinstance(cloud_batches, dict):
            return None
        raw = cloud_batches.get(target_batch, {})
        if not isinstance(raw, dict):
            return None
        batch_meta = self._normalize_cloud_batch(raw)
        if bool(batch_meta.get("first_full_cloud_sync_completed", False)):
            return batch_meta
        now_text = _now_text()
        batch_meta["first_full_cloud_sync_completed"] = True
        batch_meta["first_full_cloud_sync_at"] = now_text
        batch_meta["updated_at"] = now_text
        self._apply_review_state_changes(upsert_cloud_batches=[batch_meta], latest_batch_key=target_batch)
        return dict(batch_meta)

    def attach_cloud_batch_to_session(self, *, session_id: str, batch_key: str, building: str) -> Dict[str, Any]:
        target_session_id = str(session_id or "").strip()
        target_batch = str(batch_key or "").strip()
        if not target_session_id:
            raise ReviewSessionNotFoundError("review session not found")
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict) or target_session_id not in sessions:
            raise ReviewSessionNotFoundError("review session not found")
        session = self._normalize_session(sessions[target_session_id])
        batch_cloud = self.get_cloud_batch(target_batch)
        session["cloud_sheet_sync"] = self._build_pending_cloud_sync(
            building=building,
            revision=int(session.get("revision", 0) or 0),
            previous_cloud_sync=session.get("cloud_sheet_sync", {}),
            batch_cloud=batch_cloud,
        )
        session["updated_at"] = _now_text()
        self._apply_review_state_changes(upsert_sessions=[session], latest_batch_key=session["batch_key"])
        return dict(session)

    def register_generated_output(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        data_file: str,
        output_file: str,
        capacity_output_file: str = "",
        capacity_status: str = "",
        capacity_error: str = "",
        capacity_warnings: List[str] | None = None,
        capacity_sync: Dict[str, Any] | None = None,
        source_mode: str,
        source_file_cache: Dict[str, Any] | None = None,
        source_data_attachment_export: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        building_name = str(building or "").strip()
        duty_date_text = str(duty_date or "").strip()
        duty_shift_text = str(duty_shift or "").strip().lower()
        if not building_name or not duty_date_text or not duty_shift_text:
            raise ValueError("missing building/duty_date/duty_shift when registering review session")

        session_id = self.build_session_id(building_name, duty_date_text, duty_shift_text)
        batch_key = self.build_batch_key(duty_date_text, duty_shift_text)
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        latest_map = state.get("review_latest_by_building", {})
        if not isinstance(sessions, dict):
            sessions = {}
        if not isinstance(latest_map, dict):
            latest_map = {}
        cloud_batches = state.get("review_cloud_batches", {})
        if not isinstance(cloud_batches, dict):
            cloud_batches = {}

        previous = sessions.get(session_id, {})
        previous_revision = int(previous.get("revision", 0) or 0) if isinstance(previous, dict) else 0
        previous_output_file = str(previous.get("output_file", "") or "").strip() if isinstance(previous, dict) else ""
        previous_cloud_sync = previous.get("cloud_sheet_sync", {}) if isinstance(previous, dict) else {}
        batch_cloud = cloud_batches.get(batch_key, {}) if isinstance(cloud_batches.get(batch_key, {}), dict) else {}
        session = {
            "session_id": session_id,
            "building": building_name,
            "building_code": self._building_to_code().get(building_name, ""),
            "duty_date": duty_date_text,
            "duty_shift": duty_shift_text,
            "batch_key": batch_key,
            "data_file": str(data_file or "").strip(),
            "output_file": str(output_file or "").strip(),
            "capacity_output_file": str(capacity_output_file or "").strip(),
            "capacity_status": str(capacity_status or "").strip().lower(),
            "capacity_error": str(capacity_error or "").strip(),
            "capacity_warnings": [
                str(item or "").strip()
                for item in (capacity_warnings if isinstance(capacity_warnings, list) else [])
                if str(item or "").strip()
            ],
            "capacity_sync": self._normalize_capacity_sync(
                capacity_sync,
                fallback=self._derive_capacity_sync_from_legacy_fields(
                    {
                        "capacity_output_file": capacity_output_file,
                        "capacity_status": capacity_status,
                        "capacity_error": capacity_error,
                    }
                ),
            ),
            "source_mode": str(source_mode or "").strip(),
            "revision": previous_revision + 1 if previous_revision > 0 else 1,
            "confirmed": False,
            "confirmed_at": "",
            "confirmed_by": "",
            "updated_at": _now_text(),
            "cloud_sheet_sync": self._build_pending_cloud_sync(
                building=building_name,
                revision=previous_revision + 1 if previous_revision > 0 else 1,
                previous_cloud_sync=previous_cloud_sync,
                batch_cloud=batch_cloud,
            ),
            "source_file_cache": self._normalize_source_file_cache(source_file_cache),
            "source_data_attachment_export": self._normalize_source_data_attachment_export(
                source_data_attachment_export
            ),
            "review_link_delivery": _normalize_review_link_delivery(
                previous.get("review_link_delivery", {}) if isinstance(previous, dict) else {}
            ),
        }
        self._apply_review_state_changes(
            upsert_sessions=[session],
            latest_by_building={building_name: session_id},
            latest_batch_key=batch_key,
        )
        if previous_revision > 0 and previous_output_file != str(output_file or "").strip():
            ReviewBuildingDocumentStore(config=self.config, building=building_name).delete_document(session_id)
        return dict(session)

    def update_review_link_delivery(
        self,
        *,
        session_id: str,
        review_link_delivery: Dict[str, Any],
    ) -> Dict[str, Any]:
        target_session_id = str(session_id or "").strip()
        if not target_session_id:
            raise ReviewSessionNotFoundError("review session not found")
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict) or target_session_id not in sessions:
            raise ReviewSessionNotFoundError("review session not found")
        raw_session = sessions.get(target_session_id, {})
        if not isinstance(raw_session, dict):
            raise ReviewSessionNotFoundError("review session not found")
        session = self._normalize_session(raw_session)
        session["review_link_delivery"] = _normalize_review_link_delivery(review_link_delivery)
        session["updated_at"] = _now_text()
        self._apply_review_state_changes(upsert_sessions=[session], latest_batch_key=session["batch_key"])
        return dict(session)

    def mark_confirmed(
        self,
        *,
        building: str,
        session_id: str,
        confirmed: bool,
        base_revision: int,
        confirmed_by: str = "",
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        building_name = str(building or "").strip()
        target_session_id = str(session_id or "").strip()
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict) or target_session_id not in sessions:
            raise ReviewSessionNotFoundError("review session not found")

        session = self._normalize_session(sessions[target_session_id])
        if session["building"] != building_name:
            raise ReviewSessionNotFoundError("review session building mismatch")
        current_revision = int(session.get("revision", 1) or 1)
        if int(base_revision) != current_revision:
            raise ReviewSessionConflictError("review session revision conflict")

        session["confirmed"] = bool(confirmed)
        session["confirmed_at"] = _now_text() if confirmed else ""
        session["confirmed_by"] = str(confirmed_by or "").strip() if confirmed else ""
        session["revision"] = current_revision + 1
        session["updated_at"] = _now_text()
        self._apply_review_state_changes(upsert_sessions=[session], latest_batch_key=session["batch_key"])
        return dict(session), self.get_batch_status(session["batch_key"])

    def confirm_all_in_batch(self, *, batch_key: str, confirmed_by: str = "") -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        target_batch = str(batch_key or "").strip()
        if not target_batch:
            return [], self.get_batch_status("")
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict):
            return [], self.get_batch_status(target_batch)

        updated_sessions: List[Dict[str, Any]] = []
        for session_id, raw_session in list(sessions.items()):
            if not isinstance(raw_session, dict):
                continue
            session = self._normalize_session(raw_session)
            if str(session.get("batch_key", "")).strip() != target_batch:
                continue
            current_revision = int(session.get("revision", 1) or 1)
            session["confirmed"] = True
            session["confirmed_at"] = _now_text()
            session["confirmed_by"] = str(confirmed_by or "").strip()
            session["revision"] = current_revision + 1
            session["updated_at"] = _now_text()
            sessions[session_id] = session
            updated_sessions.append(dict(session))

        self._apply_review_state_changes(upsert_sessions=updated_sessions, latest_batch_key=target_batch)
        return updated_sessions, self.get_batch_status(target_batch)

    def update_cloud_sheet_sync(self, *, session_id: str, cloud_sheet_sync: Dict[str, Any]) -> Dict[str, Any]:
        target_session_id = str(session_id or "").strip()
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict) or target_session_id not in sessions:
            raise ReviewSessionNotFoundError("review session not found")

        session = self._normalize_session(sessions[target_session_id])
        session["cloud_sheet_sync"] = self._normalize_cloud_sheet_sync(cloud_sheet_sync)
        session["updated_at"] = _now_text()
        self._apply_review_state_changes(upsert_sessions=[session], latest_batch_key=session["batch_key"])
        return dict(session)

    def update_capacity_sync(
        self,
        *,
        session_id: str,
        capacity_sync: Dict[str, Any],
        capacity_status: str = "",
        capacity_error: str = "",
    ) -> Dict[str, Any]:
        target_session_id = str(session_id or "").strip()
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict) or target_session_id not in sessions:
            raise ReviewSessionNotFoundError("review session not found")

        session = self._normalize_session(sessions[target_session_id])
        fallback_sync = self._derive_capacity_sync_from_legacy_fields(
            {
                "capacity_output_file": session.get("capacity_output_file", ""),
                "capacity_status": capacity_status or session.get("capacity_status", ""),
                "capacity_error": capacity_error or session.get("capacity_error", ""),
                "updated_at": _now_text(),
            }
        )
        session["capacity_sync"] = self._normalize_capacity_sync(capacity_sync, fallback=fallback_sync)
        if str(capacity_status or "").strip():
            session["capacity_status"] = str(capacity_status or "").strip().lower()
        if capacity_error is not None:
            session["capacity_error"] = str(capacity_error or "").strip()
        session["updated_at"] = _now_text()
        self._apply_review_state_changes(upsert_sessions=[session], latest_batch_key=session["batch_key"])
        return dict(session)

    def update_source_data_attachment_export(
        self,
        *,
        session_id: str,
        source_data_attachment_export: Dict[str, Any],
    ) -> Dict[str, Any]:
        target_session_id = str(session_id or "").strip()
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict) or target_session_id not in sessions:
            raise ReviewSessionNotFoundError("review session not found")

        session = self._normalize_session(sessions[target_session_id])
        session["source_data_attachment_export"] = self._normalize_source_data_attachment_export(
            source_data_attachment_export
        )
        session["updated_at"] = _now_text()
        self._apply_review_state_changes(upsert_sessions=[session], latest_batch_key=session["batch_key"])
        return dict(session)

    def touch_session_after_save(
        self,
        *,
        building: str,
        session_id: str,
        base_revision: int,
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        building_name = str(building or "").strip()
        target_session_id = str(session_id or "").strip()
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        cloud_batches = state.get("review_cloud_batches", {})
        if not isinstance(sessions, dict) or target_session_id not in sessions:
            raise ReviewSessionNotFoundError("review session not found")

        session = self._normalize_session(sessions[target_session_id])
        if session["building"] != building_name:
            raise ReviewSessionNotFoundError("review session building mismatch")
        current_revision = int(session.get("revision", 1) or 1)
        if int(base_revision) != current_revision:
            raise ReviewSessionConflictError("review session revision conflict")

        session["revision"] = current_revision + 1
        session["confirmed"] = False
        session["confirmed_at"] = ""
        session["confirmed_by"] = ""
        session["updated_at"] = _now_text()

        batch_cloud = (
            self._normalize_cloud_batch(cloud_batches.get(session["batch_key"], {}))
            if isinstance(cloud_batches, dict)
            else self._normalize_cloud_batch({})
        )
        if not bool(batch_cloud.get("first_full_cloud_sync_completed", False)):
            attachment_state = self._normalize_source_data_attachment_export(
                session.get("source_data_attachment_export", {})
            )
            attachment_state["frozen_after_first_full_cloud_sync"] = False
            if attachment_state.get("reason") not in {"disabled", "missing_duty_context", "night_shift_disabled"}:
                attachment_state["status"] = "pending_review"
                attachment_state["reason"] = "await_all_confirmed"
                attachment_state["uploaded_count"] = 0
                attachment_state["error"] = ""
                attachment_state["uploaded_at"] = ""
                attachment_state["uploaded_revision"] = 0
            session["source_data_attachment_export"] = attachment_state
        else:
            attachment_state = self._normalize_source_data_attachment_export(
                session.get("source_data_attachment_export", {})
            )
            if str(attachment_state.get("status", "")).strip().lower() in {"ok", "success", "skipped"}:
                attachment_state["frozen_after_first_full_cloud_sync"] = True
            session["source_data_attachment_export"] = attachment_state

        cloud_state = self._normalize_cloud_sheet_sync(session.get("cloud_sheet_sync", {}))
        if str(cloud_state.get("status", "")).strip().lower() != "disabled":
            cloud_state["attempted"] = False
            cloud_state["success"] = False
            cloud_state["status"] = "pending_upload"
            cloud_state["last_attempt_revision"] = int(cloud_state.get("synced_revision", 0) or 0)
            cloud_state["updated_at"] = _now_text()
            cloud_state["error"] = ""
        session["cloud_sheet_sync"] = cloud_state

        self._apply_review_state_changes(upsert_sessions=[session], latest_batch_key=session["batch_key"])
        return dict(session), self.get_batch_status(session["batch_key"])

    def touch_session_after_history_save(
        self,
        *,
        building: str,
        session_id: str,
        base_revision: int,
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        building_name = str(building or "").strip()
        target_session_id = str(session_id or "").strip()
        state = self._load_state()
        sessions = state.get("review_sessions", {})
        if not isinstance(sessions, dict) or target_session_id not in sessions:
            raise ReviewSessionNotFoundError("review session not found")

        session = self._normalize_session(sessions[target_session_id])
        if session["building"] != building_name:
            raise ReviewSessionNotFoundError("review session building mismatch")
        current_revision = int(session.get("revision", 1) or 1)
        if int(base_revision) != current_revision:
            raise ReviewSessionConflictError("review session revision conflict")

        session["revision"] = current_revision + 1
        session["updated_at"] = _now_text()

        cloud_state = self._normalize_cloud_sheet_sync(session.get("cloud_sheet_sync", {}))
        if str(cloud_state.get("status", "")).strip().lower() != "disabled":
            cloud_state["attempted"] = False
            cloud_state["success"] = False
            cloud_state["status"] = "pending_upload"
            cloud_state["last_attempt_revision"] = int(cloud_state.get("synced_revision", 0) or 0)
            cloud_state["updated_at"] = _now_text()
            cloud_state["error"] = ""
        session["cloud_sheet_sync"] = cloud_state

        self._apply_review_state_changes(upsert_sessions=[session], latest_batch_key=session["batch_key"])
        return dict(session), self.get_batch_status(session["batch_key"])
