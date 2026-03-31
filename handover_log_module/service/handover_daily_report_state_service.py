from __future__ import annotations

import copy
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from app.shared.utils.atomic_file import atomic_write_text
from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root


class HandoverDailyReportStateService:
    STATE_FILE = Path("handover") / "daily_report_state.json"

    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg if isinstance(handover_cfg, dict) else {}

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _runtime_root(self) -> Path:
        return resolve_runtime_state_root(
            runtime_config={"paths": self.handover_cfg.get("_global_paths", {})},
            app_dir=Path(__file__).resolve().parents[2],
        )

    def _state_path(self) -> Path:
        path = self._runtime_root() / self.STATE_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def build_batch_key(duty_date: str, duty_shift: str) -> str:
        date_text = str(duty_date or "").strip()
        shift_text = str(duty_shift or "").strip().lower()
        if not date_text or shift_text not in {"day", "night"}:
            return ""
        return f"{date_text}|{shift_text}"

    @staticmethod
    def _default_export_state() -> Dict[str, Any]:
        return {
            "status": "idle",
            "record_id": "",
            "record_url": "",
            "spreadsheet_url": "",
            "summary_screenshot_path": "",
            "external_screenshot_path": "",
            "summary_screenshot_source_used": "",
            "external_screenshot_source_used": "",
            "updated_at": "",
            "error": "",
            "error_code": "",
            "error_detail": "",
        }

    @staticmethod
    def _default_capture_assets() -> Dict[str, Any]:
        empty_variant = {"exists": False, "stored_path": "", "captured_at": "", "preview_url": ""}
        return {
            "summary_sheet_image": {
                "exists": False,
                "source": "none",
                "stored_path": "",
                "captured_at": "",
                "preview_url": "",
                "auto": dict(empty_variant),
                "manual": dict(empty_variant),
            },
            "external_page_image": {
                "exists": False,
                "source": "none",
                "stored_path": "",
                "captured_at": "",
                "preview_url": "",
                "auto": dict(empty_variant),
                "manual": dict(empty_variant),
            },
        }

    @staticmethod
    def _default_auth_state() -> Dict[str, Any]:
        return {
            "status": "missing_login",
            "profile_dir": "",
            "last_checked_at": "",
            "error": "",
            "browser_kind": "",
            "browser_label": "",
            "browser_executable": "",
        }

    def _load_state(self) -> Dict[str, Any]:
        path = self._state_path()
        if not path.exists():
            return {"batches": {}, "screenshot_auth": self._default_auth_state()}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {"batches": {}, "screenshot_auth": self._default_auth_state()}
        if not isinstance(payload, dict):
            return {"batches": {}, "screenshot_auth": self._default_auth_state()}
        batches = payload.get("batches", {})
        if not isinstance(batches, dict):
            batches = {}
        screenshot_auth = payload.get("screenshot_auth", {})
        if not isinstance(screenshot_auth, dict):
            screenshot_auth = {}
        return {
            "batches": batches,
            "screenshot_auth": self._normalize_auth_state(screenshot_auth),
        }

    def _save_state(self, state: Dict[str, Any]) -> None:
        path = self._state_path()
        atomic_write_text(
            path,
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _normalize_export_state(self, raw: Dict[str, Any] | None) -> Dict[str, Any]:
        payload = raw if isinstance(raw, dict) else {}
        normalized = self._default_export_state()
        normalized.update(
            {
                "status": str(payload.get("status", normalized["status"]) or "").strip(),
                "record_id": str(payload.get("record_id", "") or "").strip(),
                "record_url": str(payload.get("record_url", "") or "").strip(),
                "spreadsheet_url": str(payload.get("spreadsheet_url", "") or "").strip(),
                "summary_screenshot_path": str(payload.get("summary_screenshot_path", "") or "").strip(),
                "external_screenshot_path": str(payload.get("external_screenshot_path", "") or "").strip(),
                "summary_screenshot_source_used": str(payload.get("summary_screenshot_source_used", "") or "").strip().lower(),
                "external_screenshot_source_used": str(payload.get("external_screenshot_source_used", "") or "").strip().lower(),
                "updated_at": str(payload.get("updated_at", "") or "").strip(),
                "error": str(payload.get("error", "") or "").strip(),
                "error_code": str(payload.get("error_code", "") or "").strip(),
                "error_detail": str(payload.get("error_detail", "") or "").strip(),
            }
        )
        return normalized

    def mark_pending_asset_rewrite(self, *, duty_date: str, duty_shift: str) -> Dict[str, Any]:
        current = self.get_export_state(duty_date=duty_date, duty_shift=duty_shift)
        state = {
            **current,
            "status": "pending_asset_rewrite",
            "updated_at": self._now_text(),
            "error": "",
            "error_code": "",
            "error_detail": "",
        }
        return self.update_export_state(
            duty_date=duty_date,
            duty_shift=duty_shift,
            daily_report_record_export=state,
        )

    def _normalize_auth_state(self, raw: Dict[str, Any] | None) -> Dict[str, Any]:
        payload = raw if isinstance(raw, dict) else {}
        normalized = self._default_auth_state()
        normalized.update(
            {
                "status": str(payload.get("status", normalized["status"]) or "").strip(),
                "profile_dir": str(payload.get("profile_dir", "") or "").strip(),
                "last_checked_at": str(payload.get("last_checked_at", "") or "").strip(),
                "error": str(payload.get("error", "") or "").strip(),
                "browser_kind": str(payload.get("browser_kind", "") or "").strip().lower(),
                "browser_label": str(payload.get("browser_label", "") or "").strip(),
                "browser_executable": str(payload.get("browser_executable", "") or "").strip(),
            }
        )
        return normalized

    def get_export_state(self, *, duty_date: str, duty_shift: str) -> Dict[str, Any]:
        batch_key = self.build_batch_key(duty_date, duty_shift)
        if not batch_key:
            return self._default_export_state()
        state = self._load_state()
        raw = state.get("batches", {}).get(batch_key, {})
        if not isinstance(raw, dict):
            raw = {}
        return self._normalize_export_state(raw.get("daily_report_record_export", {}))

    def update_export_state(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        daily_report_record_export: Dict[str, Any],
    ) -> Dict[str, Any]:
        batch_key = self.build_batch_key(duty_date, duty_shift)
        if not batch_key:
            raise ValueError("invalid daily report batch key")
        state = self._load_state()
        batches = state.get("batches", {})
        if not isinstance(batches, dict):
            batches = {}
        batch_state = batches.get(batch_key, {})
        if not isinstance(batch_state, dict):
            batch_state = {}
        normalized = self._normalize_export_state(daily_report_record_export)
        if not normalized["updated_at"]:
            normalized["updated_at"] = self._now_text()
        batch_state["daily_report_record_export"] = normalized
        batches[batch_key] = batch_state
        state["batches"] = batches
        self._save_state(state)
        return copy.deepcopy(normalized)

    def get_screenshot_auth_state(self) -> Dict[str, Any]:
        return self._load_state().get("screenshot_auth", self._default_auth_state())

    def update_screenshot_auth_state(self, screenshot_auth: Dict[str, Any]) -> Dict[str, Any]:
        state = self._load_state()
        normalized = self._normalize_auth_state(screenshot_auth)
        if not normalized["last_checked_at"]:
            normalized["last_checked_at"] = self._now_text()
        state["screenshot_auth"] = normalized
        self._save_state(state)
        return copy.deepcopy(normalized)

    def get_context(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        screenshot_auth: Dict[str, Any] | None = None,
        capture_assets: Dict[str, Any] | None = None,
        spreadsheet_url: str = "",
    ) -> Dict[str, Any]:
        batch_key = self.build_batch_key(duty_date, duty_shift)
        export_state = self.get_export_state(duty_date=duty_date, duty_shift=duty_shift)
        if spreadsheet_url and not export_state.get("spreadsheet_url"):
            export_state["spreadsheet_url"] = str(spreadsheet_url or "").strip()
        return {
            "ok": True,
            "batch_key": batch_key,
            "duty_date": str(duty_date or "").strip(),
            "duty_shift": str(duty_shift or "").strip().lower(),
            "daily_report_record_export": export_state,
            "screenshot_auth": self._normalize_auth_state(screenshot_auth),
            "capture_assets": capture_assets if isinstance(capture_assets, dict) else self._default_capture_assets(),
        }
