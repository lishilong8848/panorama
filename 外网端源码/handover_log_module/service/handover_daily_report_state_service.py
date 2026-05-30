from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from app.modules.scheduler.repository.scheduler_state_repository import SchedulerStateRepository
from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root


_STATE_REPOSITORY = SchedulerStateRepository()


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
            "updated_at": "",
            "error": "",
            "error_code": "",
            "error_detail": "",
        }

    def _load_state(self) -> Dict[str, Any]:
        payload = _STATE_REPOSITORY.load(self._state_path(), {"batches": {}})
        if not isinstance(payload, dict):
            return {"batches": {}}
        batches = payload.get("batches", {})
        if not isinstance(batches, dict):
            batches = {}
        return {"batches": batches}

    def _save_state(self, state: Dict[str, Any]) -> None:
        _STATE_REPOSITORY.save(self._state_path(), state)

    def _normalize_export_state(self, raw: Dict[str, Any] | None) -> Dict[str, Any]:
        payload = raw if isinstance(raw, dict) else {}
        normalized = self._default_export_state()
        normalized.update(
            {
                "status": str(payload.get("status", normalized["status"]) or "").strip(),
                "record_id": str(payload.get("record_id", "") or "").strip(),
                "record_url": str(payload.get("record_url", "") or "").strip(),
                "spreadsheet_url": str(payload.get("spreadsheet_url", "") or "").strip(),
                "updated_at": str(payload.get("updated_at", "") or "").strip(),
                "error": str(payload.get("error", "") or "").strip(),
                "error_code": str(payload.get("error_code", "") or "").strip(),
                "error_detail": str(payload.get("error_detail", "") or "").strip(),
            }
        )
        return normalized

    @staticmethod
    def _action_payload(
        *,
        label: str,
        allowed: bool = True,
        pending: bool = False,
        disabled_reason: str = "",
        reason_code: str = "",
    ) -> Dict[str, Any]:
        return {
            "allowed": bool(allowed),
            "pending": bool(pending),
            "label": str(label or "").strip(),
            "disabled_reason": str(disabled_reason or "").strip(),
            "reason_code": str(reason_code or "").strip().lower(),
        }

    def _present_export_display(self, raw_export: Dict[str, Any] | None) -> Dict[str, Any]:
        export_state = self._normalize_export_state(raw_export)
        status = str(export_state.get("status", "") or "").strip().lower()
        error_code = str(export_state.get("error_code", "") or "").strip()
        raw_error = str(export_state.get("error", "") or "").strip()
        raw_detail = str(export_state.get("error_detail", "") or "").strip()
        if raw_error:
            error = raw_error
        elif error_code == "daily_report_url_field_invalid":
            error = "日报链接字段写入失败，请检查飞书多维表“交接班日报”字段类型。"
        elif error_code == "missing_spreadsheet_url":
            error = "当前批次缺少云文档链接，无法重写日报记录。"
        else:
            error = raw_detail
        if status == "success":
            return {"text": "日报多维记录已写入", "tone": "success", "error": error}
        if status == "pending":
            return {"text": "日报多维待写入", "tone": "warning", "error": error}
        if status == "skipped_due_to_cloud_sync_not_ok":
            return {"text": "等待本批次云文档全部成功", "tone": "neutral", "error": error}
        if status == "failed":
            return {"text": "日报多维写入失败", "tone": "danger", "error": error}
        if status == "skipped":
            return {"text": "日报多维已跳过", "tone": "neutral", "error": error}
        return {"text": "日报多维未执行", "tone": "neutral", "error": error}

    def _present_daily_report_actions(self, *, export_state: Dict[str, Any]) -> Dict[str, Any]:
        spreadsheet_url = str(export_state.get("spreadsheet_url", "") or "").strip()
        rewrite_allowed = bool(spreadsheet_url)
        return {
            "rewrite_record": self._action_payload(
                label="重新写入日报多维表",
                allowed=rewrite_allowed,
                disabled_reason="" if rewrite_allowed else "当前批次缺少云文档链接，无法重写日报记录。",
                reason_code="" if rewrite_allowed else "missing_spreadsheet_url",
            ),
        }

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

    def get_context(
        self,
        *,
        duty_date: str,
        duty_shift: str,
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
            "display": {
                "export": self._present_export_display(export_state),
                "actions": self._present_daily_report_actions(export_state=export_state),
            },
        }
