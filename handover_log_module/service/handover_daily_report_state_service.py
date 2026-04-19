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
            "browser_profile_name": "",
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
                "browser_profile_name": str(payload.get("browser_profile_name", "") or "").strip(),
                "browser_executable": str(payload.get("browser_executable", "") or "").strip(),
            }
        )
        return normalized

    @staticmethod
    def _daily_report_browser_label(raw_auth: Dict[str, Any] | None, fallback: str = "系统浏览器") -> str:
        payload = raw_auth if isinstance(raw_auth, dict) else {}
        label = str(payload.get("browser_label", "") or "").strip()
        return label or fallback

    def _present_auth_display(self, raw_auth: Dict[str, Any] | None) -> Dict[str, Any]:
        auth = self._normalize_auth_state(raw_auth)
        status = str(auth.get("status", "") or "").strip().lower()
        error = str(auth.get("error", "") or "").strip()
        browser_label = self._daily_report_browser_label(auth)
        browser_profile_name = str(auth.get("browser_profile_name", "") or "").strip()
        profile_text = f"{browser_label} / {browser_profile_name}" if browser_profile_name else browser_label
        profile_label = "当前接管浏览器" if status in {"ready", "ready_without_target_page"} else "当前目标浏览器"
        if status == "ready":
            return {"text": "已登录", "tone": "success", "error": "", "profile_text": profile_text, "profile_label": profile_label}
        if status == "ready_without_target_page":
            return {
                "text": "已登录",
                "tone": "success",
                "error": f"当前已接管{browser_label}，但尚未定位到飞书目标页；执行截图测试时会自动尝试补开。",
                "profile_text": profile_text,
                "profile_label": profile_label,
            }
        if status == "missing_login":
            if error == "browser_not_started":
                return {
                    "text": "浏览器未接管",
                    "tone": "warning",
                    "error": f"{browser_label} 尚未被程序接管，请点击“初始化飞书截图登录态”。",
                    "profile_text": profile_text,
                    "profile_label": profile_label,
                }
            if error == "browser_started_without_pages":
                return {
                    "text": "待打开飞书页",
                    "tone": "warning",
                    "error": f"{browser_label} 登录页已被关闭，请点击“初始化飞书截图登录态”重新打开。",
                    "profile_text": profile_text,
                    "profile_label": profile_label,
                }
            if error == "feishu_page_not_open":
                return {
                    "text": "已登录",
                    "tone": "success",
                    "error": f"当前{browser_label}中未检测到飞书目标页；执行截图测试时会自动尝试补开。",
                    "profile_text": profile_text,
                    "profile_label": profile_label,
                }
            return {
                "text": "待登录",
                "tone": "warning",
                "error": error if error and error != "login_required" else f"当前{browser_label}中的飞书登录态未就绪，请完成扫码登录。",
                "profile_text": profile_text,
                "profile_label": profile_label,
            }
        if status == "expired":
            return {
                "text": "已失效",
                "tone": "warning",
                "error": error or "当前飞书截图登录态已失效，请重新初始化并扫码登录。",
                "profile_text": profile_text,
                "profile_label": profile_label,
            }
        if status == "browser_unavailable":
            if "未找到系统 Edge 或 Chrome" in error:
                return {
                    "text": "浏览器不可用",
                    "tone": "danger",
                    "error": "未找到可用系统浏览器（Edge/Chrome），请安装 Microsoft Edge 或 Google Chrome。",
                    "profile_text": profile_text,
                    "profile_label": profile_label,
                }
            if "browser_debug_port_unavailable" in error:
                return {
                    "text": "浏览器未接管",
                    "tone": "danger",
                    "error": f"请先关闭所有 {browser_label} 窗口后，再点击“初始化飞书截图登录态”。",
                    "profile_text": profile_text,
                    "profile_label": profile_label,
                }
            return {
                "text": "浏览器不可用",
                "tone": "danger",
                "error": error or f"当前无法接管{browser_label}，请检查其是否已安装并可正常启动。",
                "profile_text": profile_text,
                "profile_label": profile_label,
            }
        return {
            "text": "未初始化",
            "tone": "neutral",
            "error": error or "截图登录态尚未初始化。",
            "profile_text": profile_text,
            "profile_label": profile_label,
        }

    def _present_export_display(self, raw_export: Dict[str, Any] | None, raw_auth: Dict[str, Any] | None) -> Dict[str, Any]:
        export_state = self._normalize_export_state(raw_export)
        status = str(export_state.get("status", "") or "").strip().lower()
        error_code = str(export_state.get("error_code", "") or "").strip()
        raw_error = str(export_state.get("error", "") or "").strip()
        raw_detail = str(export_state.get("error_detail", "") or "").strip()
        browser_label = self._daily_report_browser_label(raw_auth)
        if raw_error == "login_required":
            error = f"当前{browser_label}中的飞书登录态未就绪，请完成扫码登录。"
        elif raw_error:
            error = raw_error
        elif error_code == "daily_report_url_field_invalid":
            error = "日报链接字段写入失败，请检查飞书多维表“交接班日报”字段类型。"
        elif error_code == "missing_spreadsheet_url":
            error = "当前批次缺少云文档链接，无法重写日报记录。"
        elif error_code == "missing_effective_asset":
            error = "当前最终生效截图不完整，无法重写日报记录。"
        else:
            error = raw_detail
        if status == "success":
            return {"text": "日报多维记录已写入", "tone": "success", "error": error}
        if status == "pending":
            return {"text": "日报多维待写入", "tone": "warning", "error": error}
        if status == "skipped_due_to_cloud_sync_not_ok":
            return {"text": "等待本批次云文档全部成功", "tone": "neutral", "error": error}
        if status == "login_required":
            return {"text": "需要登录飞书后才能自动截图", "tone": "warning", "error": error}
        if status == "capture_failed":
            return {"text": "截图失败，日报记录未写入", "tone": "danger", "error": error}
        if status == "pending_asset_rewrite":
            return {"text": "截图已更新，待重写日报记录", "tone": "warning", "error": error}
        if status == "failed":
            return {"text": "日报多维写入失败", "tone": "danger", "error": error}
        if status == "skipped":
            return {"text": "日报多维已跳过", "tone": "neutral", "error": error}
        return {"text": "日报多维未执行", "tone": "neutral", "error": error}

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

    def _present_daily_report_actions(
        self,
        *,
        export_state: Dict[str, Any],
        capture_assets: Dict[str, Any],
    ) -> Dict[str, Any]:
        summary_asset = capture_assets.get("summary_sheet_image", {}) if isinstance(capture_assets, dict) else {}
        external_asset = capture_assets.get("external_page_image", {}) if isinstance(capture_assets, dict) else {}
        has_summary = bool(summary_asset.get("exists", False))
        has_external = bool(external_asset.get("exists", False))
        spreadsheet_url = str(export_state.get("spreadsheet_url", "") or "").strip()

        rewrite_allowed = bool(spreadsheet_url) and has_summary and has_external
        rewrite_disabled_reason = ""
        rewrite_reason_code = ""
        if not rewrite_allowed:
            if not spreadsheet_url:
                rewrite_disabled_reason = "当前批次缺少云文档链接，无法重写日报记录。"
                rewrite_reason_code = "missing_spreadsheet_url"
            elif not has_summary and not has_external:
                rewrite_disabled_reason = "当前最终生效截图不完整，无法重写日报记录。"
                rewrite_reason_code = "missing_effective_assets"
            elif not has_summary:
                rewrite_disabled_reason = "缺少今日航图截图，无法重写日报记录。"
                rewrite_reason_code = "missing_summary_asset"
            else:
                rewrite_disabled_reason = "缺少排班截图，无法重写日报记录。"
                rewrite_reason_code = "missing_external_asset"

        return {
            "open_auth": self._action_payload(label="初始化飞书截图登录态"),
            "screenshot_test": self._action_payload(label="截图测试"),
            "rewrite_record": self._action_payload(
                label="重新写入日报多维表",
                allowed=rewrite_allowed,
                disabled_reason=rewrite_disabled_reason,
                reason_code=rewrite_reason_code,
            ),
        }

    @staticmethod
    def _build_daily_report_asset_download_name(duty_date: str, duty_shift: str, title: str) -> str:
        duty_date_text = str(duty_date or "").strip() or "unknown-date"
        duty_shift_text = str(duty_shift or "").strip().lower() or "unknown-shift"
        title_text = str(title or "").strip() or "日报截图"
        return f"{duty_date_text}_{duty_shift_text}_{title_text}.png"

    @staticmethod
    def _present_last_written_source(source: str) -> Dict[str, Any]:
        text = str(source or "").strip().lower()
        if text == "manual":
            return {"text": "上次入库：手工图", "tone": "warning", "exists": True}
        if text == "auto":
            return {"text": "上次入库：自动图", "tone": "info", "exists": True}
        return {"text": "尚未写入日报", "tone": "neutral", "exists": False}

    def _present_capture_asset_card(
        self,
        raw_asset: Dict[str, Any] | None,
        *,
        title: str,
        duty_date: str,
        duty_shift: str,
        last_written_source: str,
    ) -> Dict[str, Any]:
        asset = raw_asset if isinstance(raw_asset, dict) else {}
        auto = asset.get("auto", {}) if isinstance(asset.get("auto", {}), dict) else {}
        manual = asset.get("manual", {}) if isinstance(asset.get("manual", {}), dict) else {}
        source = str(asset.get("source", "") or "").strip().lower()
        effective_exists = bool(asset.get("exists", False))
        has_manual = bool(manual.get("exists", False))
        source_text = "手工" if source == "manual" else "自动" if source == "auto" else "未生成"
        last_written = self._present_last_written_source(last_written_source)
        return {
            "title": title,
            "exists": effective_exists,
            "source": source,
            "source_text": source_text,
            "stored_path": str(asset.get("stored_path", "") or "").strip(),
            "captured_at": str(asset.get("captured_at", "") or "").strip(),
            "preview_url": str(asset.get("thumbnail_url", "") or asset.get("preview_url", "") or "").strip(),
            "thumbnail_url": str(asset.get("thumbnail_url", "") or asset.get("preview_url", "") or "").strip(),
            "full_image_url": str(asset.get("full_image_url", "") or asset.get("preview_url", "") or "").strip(),
            "auto": auto,
            "manual": manual,
            "has_manual": has_manual,
            "has_auto": bool(auto.get("exists", False)),
            "download_name": self._build_daily_report_asset_download_name(duty_date, duty_shift, title),
            "last_written_source": str(last_written_source or "").strip().lower(),
            "last_written_source_text": str(last_written.get("text", "") or "").strip(),
            "last_written_source_tone": str(last_written.get("tone", "") or "").strip() or "neutral",
            "has_last_written_source": bool(last_written.get("exists", False)),
            "actions": {
                "preview": self._action_payload(
                    label="放大查看",
                    allowed=effective_exists,
                    disabled_reason="" if effective_exists else f"当前还没有{title}",
                    reason_code="" if effective_exists else "missing_asset",
                ),
                "recapture": self._action_payload(label="重新截图"),
                "upload": self._action_payload(label="上传/粘贴替换"),
                "restore_auto": self._action_payload(
                    label="恢复自动图",
                    allowed=has_manual,
                    disabled_reason="" if has_manual else "当前没有手工替换图",
                    reason_code="" if has_manual else "manual_asset_missing",
                ),
            },
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
        normalized_auth = self._normalize_auth_state(screenshot_auth)
        normalized_capture_assets = capture_assets if isinstance(capture_assets, dict) else self._default_capture_assets()
        display_actions = self._present_daily_report_actions(
            export_state=export_state,
            capture_assets=normalized_capture_assets,
        )
        return {
            "ok": True,
            "batch_key": batch_key,
            "duty_date": str(duty_date or "").strip(),
            "duty_shift": str(duty_shift or "").strip().lower(),
            "daily_report_record_export": export_state,
            "screenshot_auth": normalized_auth,
            "capture_assets": normalized_capture_assets,
            "display": {
                "auth": self._present_auth_display(normalized_auth),
                "export": self._present_export_display(export_state, normalized_auth),
                "actions": display_actions,
                "capture_assets": {
                    "summary_sheet_image": self._present_capture_asset_card(
                        normalized_capture_assets.get("summary_sheet_image", {}),
                        title="今日航图截图",
                        duty_date=str(duty_date or "").strip(),
                        duty_shift=str(duty_shift or "").strip().lower(),
                        last_written_source=str(export_state.get("summary_screenshot_source_used", "") or "").strip().lower(),
                    ),
                    "external_page_image": self._present_capture_asset_card(
                        normalized_capture_assets.get("external_page_image", {}),
                        title="排班截图",
                        duty_date=str(duty_date or "").strip(),
                        duty_shift=str(duty_shift or "").strip().lower(),
                        last_written_source=str(export_state.get("external_screenshot_source_used", "") or "").strip().lower(),
                    ),
                },
            },
        }
