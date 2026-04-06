from __future__ import annotations

import copy
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from app.modules.feishu.service.im_file_message_client import FeishuImFileMessageClient
from app.shared.utils.atomic_file import atomic_write_text
from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root
from handover_log_module.api.facade import load_handover_config
from handover_log_module.repository.shift_roster_repository import ShiftRosterRepository
from handover_log_module.service.monthly_change_report_service import MonthlyChangeReportService
from handover_log_module.service.monthly_event_report_service import MonthlyEventReportService
from pipeline_utils import get_app_dir


_ALL_BUILDINGS = ("A楼", "B楼", "C楼", "D楼", "E楼")
_LEGACY_LAST_RUN_FILE = "monthly_report_delivery_last_run.json"
_LAST_RUN_FILE_PATTERN = "monthly_report_delivery_last_run_{report_type}.json"
_SUPPORTED_REPORT_TYPES = {"event", "change"}
_DEFAULT_TEST_OPEN_ID = "ou_902e364a6c2c6c20893c02abe505a7b2"
_RECEIVE_ID_SPLIT_PATTERN = re.compile(r"[\s,，;；\r\n]+")


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(base or {})
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


class MonthlyReportDeliveryService:
    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        self.runtime_config = runtime_config if isinstance(runtime_config, dict) else {}

    @staticmethod
    def all_buildings() -> List[str]:
        return list(_ALL_BUILDINGS)

    @staticmethod
    def normalize_report_type(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text not in _SUPPORTED_REPORT_TYPES:
            raise ValueError("报表类型非法，仅支持 event / change")
        return text

    @staticmethod
    def normalize_scope(scope: Any, building: Any = None) -> Tuple[str, str | None]:
        return MonthlyEventReportService.normalize_scope(scope, building)

    @staticmethod
    def normalize_receive_ids(raw_value: Any) -> List[str]:
        values: List[str] = []
        if isinstance(raw_value, (list, tuple, set)):
            source_items = list(raw_value)
        else:
            source_items = [raw_value]
        for item in source_items:
            text = str(item or "").strip()
            if not text:
                continue
            if _RECEIVE_ID_SPLIT_PATTERN.search(text):
                values.extend(seg.strip() for seg in _RECEIVE_ID_SPLIT_PATTERN.split(text) if seg and seg.strip())
            else:
                values.append(text)
        output: List[str] = []
        seen = set()
        for item in values:
            if item in seen:
                continue
            seen.add(item)
            output.append(item)
        return output

    @staticmethod
    def job_name(report_type: str, scope: str, building: str | None = None) -> str:
        normalized_report_type = MonthlyReportDeliveryService.normalize_report_type(report_type)
        normalized_scope, normalized_building = MonthlyReportDeliveryService.normalize_scope(scope, building)
        report_label = "事件" if normalized_report_type == "event" else "变更"
        if normalized_scope == "building" and normalized_building:
            return f"月度统计表发送-{report_label}-{normalized_building}"
        return f"月度统计表发送-{report_label}-全部楼栋"

    @staticmethod
    def test_job_name(report_type: str) -> str:
        normalized_report_type = MonthlyReportDeliveryService.normalize_report_type(report_type)
        report_label = "事件" if normalized_report_type == "event" else "变更"
        return f"月度统计表测试发送-{report_label}"

    @staticmethod
    def dedupe_key(report_type: str, scope: str, building: str | None = None, *, target_month: str) -> str:
        normalized_report_type = MonthlyReportDeliveryService.normalize_report_type(report_type)
        normalized_scope, normalized_building = MonthlyReportDeliveryService.normalize_scope(scope, building)
        month_text = str(target_month or "").strip()
        if normalized_scope == "building" and normalized_building:
            return f"monthly_report_send:{normalized_report_type}:building:{normalized_building}:{month_text}"
        return f"monthly_report_send:{normalized_report_type}:all:{month_text}"

    @staticmethod
    def test_dedupe_key(report_type: str, *, target_month: str, receive_ids: Any) -> str:
        normalized_report_type = MonthlyReportDeliveryService.normalize_report_type(report_type)
        month_text = str(target_month or "").strip()
        normalized_receive_ids = MonthlyReportDeliveryService.normalize_receive_ids(receive_ids)
        digest = hashlib.sha1("|".join(normalized_receive_ids).encode("utf-8")).hexdigest()[:12] if normalized_receive_ids else "empty"
        return f"monthly_report_send:{normalized_report_type}:test:{digest}:{month_text}"

    @staticmethod
    def default_test_open_id() -> str:
        return _DEFAULT_TEST_OPEN_ID

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "shift_roster": {
                "engineer_directory": {
                    "enabled": True,
                    "fields": {
                        "recipient_id": "飞书用户ID",
                    },
                    "delivery": {
                        "receive_id_type": "user_id",
                        "position_keyword": "设施运维主管",
                    },
                },
            },
        }

    def _handover_cfg(self) -> Dict[str, Any]:
        return load_handover_config(self.runtime_config)

    def _normalize_cfg(self) -> Dict[str, Any]:
        return _deep_merge(self._defaults(), self._handover_cfg())

    def _runtime_state_root(self) -> Path:
        return resolve_runtime_state_root(runtime_config=self.runtime_config, app_dir=get_app_dir())

    def _last_run_path(self, report_type: str) -> Path:
        normalized_report_type = self.normalize_report_type(report_type)
        file_name = _LAST_RUN_FILE_PATTERN.format(report_type=normalized_report_type)
        return self._runtime_state_root() / file_name

    def _legacy_last_run_path(self) -> Path:
        return self._runtime_state_root() / _LEGACY_LAST_RUN_FILE

    @staticmethod
    def _empty_last_run() -> Dict[str, Any]:
        return {
            "started_at": "",
            "finished_at": "",
            "status": "",
            "report_type": "",
            "scope": "",
            "building": "",
            "target_month": "",
            "successful_buildings": [],
            "failed_buildings": [],
            "sent_count": 0,
            "message_ids": {},
            "error": "",
            "test_mode": False,
            "test_receive_id": "",
            "test_receive_id_type": "",
            "test_receive_ids": [],
            "test_successful_receivers": [],
            "test_failed_receivers": [],
            "test_file_building": "",
            "test_file_name": "",
        }

    def get_last_run_snapshot(self, report_type: str = "event") -> Dict[str, Any]:
        normalized_report_type = self.normalize_report_type(report_type)
        path = self._last_run_path(normalized_report_type)
        if not path.exists():
            if normalized_report_type == "event":
                legacy_path = self._legacy_last_run_path()
                path = legacy_path if legacy_path.exists() else path
            if not path.exists():
                snapshot = self._empty_last_run()
                snapshot["report_type"] = normalized_report_type
                return snapshot
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            snapshot = self._empty_last_run()
            snapshot["report_type"] = normalized_report_type
            return snapshot
        if not isinstance(payload, dict):
            snapshot = self._empty_last_run()
            snapshot["report_type"] = normalized_report_type
            return snapshot
        snapshot = self._empty_last_run()
        snapshot.update(payload)
        snapshot["report_type"] = str(snapshot.get("report_type", "") or "").strip().lower() or normalized_report_type
        snapshot["sent_count"] = int(snapshot.get("sent_count", 0) or 0)
        snapshot["successful_buildings"] = [
            str(item or "").strip()
            for item in (snapshot.get("successful_buildings", []) or [])
            if str(item or "").strip()
        ]
        snapshot["failed_buildings"] = [
            str(item or "").strip()
            for item in (snapshot.get("failed_buildings", []) or [])
            if str(item or "").strip()
        ]
        message_ids = snapshot.get("message_ids", {})
        snapshot["message_ids"] = dict(message_ids) if isinstance(message_ids, dict) else {}
        snapshot["test_mode"] = bool(snapshot.get("test_mode", False))
        snapshot["test_receive_id"] = str(snapshot.get("test_receive_id", "") or "").strip()
        snapshot["test_receive_id_type"] = str(snapshot.get("test_receive_id_type", "") or "").strip()
        snapshot["test_receive_ids"] = self.normalize_receive_ids(snapshot.get("test_receive_ids", []))
        snapshot["test_successful_receivers"] = self.normalize_receive_ids(snapshot.get("test_successful_receivers", []))
        snapshot["test_failed_receivers"] = self.normalize_receive_ids(snapshot.get("test_failed_receivers", []))
        snapshot["test_file_building"] = str(snapshot.get("test_file_building", "") or "").strip()
        snapshot["test_file_name"] = str(snapshot.get("test_file_name", "") or "").strip()
        return snapshot

    def _save_last_run_snapshot(self, report_type: str, payload: Dict[str, Any]) -> None:
        normalized_report_type = self.normalize_report_type(report_type)
        snapshot = self._empty_last_run()
        snapshot.update(payload if isinstance(payload, dict) else {})
        snapshot["report_type"] = normalized_report_type
        atomic_write_text(
            self._last_run_path(normalized_report_type),
            json.dumps(snapshot, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _build_feishu_client(self) -> FeishuImFileMessageClient:
        handover_cfg = self._handover_cfg()
        global_feishu = handover_cfg.get("_global_feishu", {})
        if not isinstance(global_feishu, dict):
            global_feishu = {}
        app_id = str(global_feishu.get("app_id", "") or "").strip()
        app_secret = str(global_feishu.get("app_secret", "") or "").strip()
        if not app_id or not app_secret:
            raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")
        return FeishuImFileMessageClient(
            app_id=app_id,
            app_secret=app_secret,
            timeout=int(global_feishu.get("timeout", 30) or 30),
            request_retry_count=int(global_feishu.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2) or 2),
        )

    @staticmethod
    def _normalize_report_files_map(last_run: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        raw = last_run.get("files_by_building", {})
        if not isinstance(raw, dict):
            return {}
        output: Dict[str, Dict[str, Any]] = {}
        for building, item in raw.items():
            building_text = str(building or "").strip()
            if not building_text or not isinstance(item, dict):
                continue
            file_path = str(item.get("file_path", "") or "").strip()
            file_name = str(item.get("file_name", "") or "").strip()
            exists = bool(item.get("exists", False))
            output[building_text] = {
                "building": building_text,
                "file_path": file_path,
                "file_name": file_name,
                "exists": exists and bool(file_path) and Path(file_path).exists(),
            }
        return output

    def _resolve_report_generation_snapshot(self, report_type: str) -> Dict[str, Any]:
        normalized_report_type = self.normalize_report_type(report_type)
        if normalized_report_type == "event":
            snapshot = MonthlyEventReportService(self.runtime_config).get_last_run_snapshot()
        elif normalized_report_type == "change":
            snapshot = MonthlyChangeReportService(self.runtime_config).get_last_run_snapshot()
        else:
            snapshot = {}
        if not isinstance(snapshot, dict):
            snapshot = {}
        report_type_text = str(snapshot.get("report_type", "") or "").strip().lower()
        if report_type_text and report_type_text != normalized_report_type:
            return {}
        snapshot["files_by_building"] = self._normalize_report_files_map(snapshot)
        return snapshot

    def _resolve_recipient_directory_rows(self, emit_log: Callable[[str], None]) -> List[Dict[str, str]]:
        handover_cfg = self._handover_cfg()
        repo = ShiftRosterRepository(handover_cfg)
        return repo.list_engineer_directory(emit_log=emit_log)

    def _recipient_config(self) -> Dict[str, str]:
        cfg = self._normalize_cfg()
        engineer_directory = (
            cfg.get("shift_roster", {}).get("engineer_directory", {})
            if isinstance(cfg.get("shift_roster", {}), dict)
            else {}
        )
        if not isinstance(engineer_directory, dict):
            engineer_directory = {}
        delivery_cfg = engineer_directory.get("delivery", {})
        if not isinstance(delivery_cfg, dict):
            delivery_cfg = {}
        fields_cfg = engineer_directory.get("fields", {})
        if not isinstance(fields_cfg, dict):
            fields_cfg = {}
        return {
            "receive_id_type": str(delivery_cfg.get("receive_id_type", "user_id") or "user_id").strip() or "user_id",
            "position_keyword": str(delivery_cfg.get("position_keyword", "设施运维主管") or "设施运维主管").strip() or "设施运维主管",
            "recipient_field": str(fields_cfg.get("recipient_id", "飞书用户ID") or "飞书用户ID").strip() or "飞书用户ID",
        }

    @staticmethod
    def _resolve_effective_receive_id_type(recipient_id: str, configured_receive_id_type: str) -> str:
        rid = str(recipient_id or "").strip()
        configured = str(configured_receive_id_type or "").strip().lower() or "user_id"
        if rid.startswith("ou_"):
            return "open_id"
        if "@" in rid and configured == "user_id":
            return "email"
        digits_only = rid.removeprefix("+")
        if digits_only.isdigit() and len(digits_only) >= 11 and configured == "user_id":
            return "mobile"
        return configured

    @staticmethod
    def _choose_test_file(files_by_building: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        for building in _ALL_BUILDINGS:
            item = files_by_building.get(building, {})
            if isinstance(item, dict) and bool(item.get("exists", False)):
                return dict(item)
        return {}

    def build_recipient_status_by_building(
        self,
        *,
        report_type: str = "event",
        emit_log: Callable[[str], None] = print,
    ) -> List[Dict[str, Any]]:
        normalized_report_type = self.normalize_report_type(report_type)
        report_snapshot = self._resolve_report_generation_snapshot(normalized_report_type)
        file_map = report_snapshot.get("files_by_building", {}) if isinstance(report_snapshot, dict) else {}
        file_map = file_map if isinstance(file_map, dict) else {}
        recipient_cfg = self._recipient_config()
        configured_receive_id_type = recipient_cfg["receive_id_type"]
        position_keyword = recipient_cfg["position_keyword"]
        directory_rows = self._resolve_recipient_directory_rows(emit_log=emit_log)

        statuses: List[Dict[str, Any]] = []
        for building in self.all_buildings():
            report_file = file_map.get(building, {}) if isinstance(file_map.get(building, {}), dict) else {}
            file_exists = bool(report_file.get("exists", False))
            building_rows = [row for row in directory_rows if str(row.get("building", "") or "").strip() == building]
            keyword_rows = [
                row for row in building_rows if position_keyword in str(row.get("position", "") or "").strip()
            ]
            status = {
                "building": building,
                "supervisor": "",
                "position": "",
                "recipient_id": "",
                "receive_id_type": configured_receive_id_type,
                "send_ready": False,
                "reason": "",
                "file_name": str(report_file.get("file_name", "") or "").strip(),
                "file_path": str(report_file.get("file_path", "") or "").strip(),
                "file_exists": file_exists,
                "report_type": normalized_report_type,
                "target_month": str(report_snapshot.get("target_month", "") or "").strip(),
            }
            preview_row = keyword_rows[0] if keyword_rows else (building_rows[0] if building_rows else {})
            if isinstance(preview_row, dict):
                status["supervisor"] = str(preview_row.get("supervisor", "") or "").strip()
                status["position"] = str(preview_row.get("position", "") or "").strip()
                status["recipient_id"] = str(preview_row.get("recipient_id", "") or "").strip()
                status["receive_id_type"] = self._resolve_effective_receive_id_type(
                    status["recipient_id"],
                    configured_receive_id_type,
                )

            if not file_exists:
                status["reason"] = "当前楼栋没有可发送的已生成文件"
                statuses.append(status)
                continue
            if not building_rows:
                status["reason"] = "工程师目录未匹配到该楼栋"
                statuses.append(status)
                continue
            if not keyword_rows:
                status["reason"] = f"工程师目录未找到职位包含“{position_keyword}”的收件人"
                statuses.append(status)
                continue
            if len(keyword_rows) > 1:
                status["reason"] = f"工程师目录匹配到多个“{position_keyword}”收件人，请清理数据"
                statuses.append(status)
                continue

            selected = keyword_rows[0]
            status["supervisor"] = str(selected.get("supervisor", "") or "").strip()
            status["position"] = str(selected.get("position", "") or "").strip()
            status["recipient_id"] = str(selected.get("recipient_id", "") or "").strip()
            status["receive_id_type"] = self._resolve_effective_receive_id_type(
                status["recipient_id"],
                configured_receive_id_type,
            )
            if not status["recipient_id"]:
                status["reason"] = "工程师目录缺少可直发的飞书身份字段"
                statuses.append(status)
                continue

            status["send_ready"] = True
            statuses.append(status)
        return statuses

    def build_delivery_health_snapshot(
        self,
        *,
        report_type: str = "event",
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        normalized_report_type = self.normalize_report_type(report_type)
        return {
            "last_run": self.get_last_run_snapshot(normalized_report_type),
            "recipient_status_by_building": self.build_recipient_status_by_building(
                report_type=normalized_report_type,
                emit_log=emit_log,
            ),
        }

    @staticmethod
    def _build_result_payload(
        *,
        started_at: datetime,
        status: str,
        report_type: str,
        scope: str,
        building: str,
        target_month: str,
        successful_buildings: List[str],
        failed_buildings: List[str],
        message_ids: Dict[str, str],
        error: str,
        test_mode: bool = False,
        test_receive_id: str = "",
        test_receive_id_type: str = "",
        test_receive_ids: List[str] | None = None,
        test_successful_receivers: List[str] | None = None,
        test_failed_receivers: List[str] | None = None,
        test_file_building: str = "",
        test_file_name: str = "",
    ) -> Dict[str, Any]:
        normalized_report_type = MonthlyReportDeliveryService.normalize_report_type(report_type)
        report_label = "事件" if normalized_report_type == "event" else "变更"
        return {
            "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "report_type": normalized_report_type,
            "report_type_text": report_label,
            "scope": scope,
            "building": building,
            "target_month": target_month,
            "successful_buildings": successful_buildings,
            "failed_buildings": failed_buildings,
            "sent_count": len(successful_buildings),
            "message_ids": message_ids,
            "error": error,
            "test_mode": test_mode,
            "test_receive_id": test_receive_id,
            "test_receive_id_type": test_receive_id_type,
            "test_receive_ids": test_receive_ids or [],
            "test_successful_receivers": test_successful_receivers or [],
            "test_failed_receivers": test_failed_receivers or [],
            "test_file_building": test_file_building,
            "test_file_name": test_file_name,
        }

    def run_send(
        self,
        *,
        report_type: str,
        scope: str,
        building: str | None = None,
        emit_log: Callable[[str], None] = print,
        source: str = "manual",
    ) -> Dict[str, Any]:
        normalized_report_type = self.normalize_report_type(report_type)
        normalized_scope, normalized_building = self.normalize_scope(scope, building)
        selected_buildings = [normalized_building] if normalized_scope == "building" and normalized_building else self.all_buildings()
        report_snapshot = self._resolve_report_generation_snapshot(normalized_report_type)
        target_month = str(report_snapshot.get("target_month", "") or "").strip()
        if not target_month:
            raise RuntimeError("缺少最近成功生成的月度统计表文件，请先生成后再发送")

        files_by_building = report_snapshot.get("files_by_building", {})
        if not isinstance(files_by_building, dict) or not files_by_building:
            raise RuntimeError("缺少最近成功生成的月度统计表文件，请先生成后再发送")

        recipient_status_by_building = {
            str(item.get("building", "") or "").strip(): item
            for item in self.build_recipient_status_by_building(report_type=normalized_report_type, emit_log=emit_log)
            if isinstance(item, dict) and str(item.get("building", "") or "").strip()
        }
        client = self._build_feishu_client()
        started_at = datetime.now()
        emit_log(
            f"[月度统计表发送] 开始: report_type={normalized_report_type}, scope={normalized_scope}, "
            f"building={normalized_building or 'all'}, target_month={target_month}, source={source}"
        )

        successful_buildings: List[str] = []
        failed_buildings: List[str] = []
        failure_details: List[str] = []
        message_ids: Dict[str, str] = {}

        for current_building in selected_buildings:
            recipient_status = recipient_status_by_building.get(current_building, {})
            reason = str(recipient_status.get("reason", "") or "").strip()
            if not bool(recipient_status.get("send_ready", False)):
                failed_buildings.append(current_building)
                failure_details.append(f"{current_building}: {reason or '当前楼栋不可发送'}")
                emit_log(
                    f"[月度统计表发送] 跳过: report_type={normalized_report_type}, building={current_building}, "
                    f"reason={reason or '当前楼栋不可发送'}"
                )
                continue

            file_path = str(recipient_status.get("file_path", "") or "").strip()
            recipient_id = str(recipient_status.get("recipient_id", "") or "").strip()
            receive_id_type = str(recipient_status.get("receive_id_type", "") or "").strip() or "user_id"
            try:
                upload_result = client.upload_file(file_path)
                send_result = client.send_file_message(
                    receive_id=recipient_id,
                    receive_id_type=receive_id_type,
                    file_key=str(upload_result.get("file_key", "") or "").strip(),
                )
                message_id = str(send_result.get("message_id", "") or "").strip()
                successful_buildings.append(current_building)
                if message_id:
                    message_ids[current_building] = message_id
                emit_log(
                    f"[月度统计表发送] 发送成功: report_type={normalized_report_type}, building={current_building}, "
                    f"recipient_id={recipient_id}, receive_id_type={receive_id_type}, file_path={file_path}, "
                    f"message_id={message_id or '-'}"
                )
            except Exception as exc:
                failed_buildings.append(current_building)
                failure_details.append(f"{current_building}: {exc}")
                emit_log(
                    f"[月度统计表发送] 发送失败: report_type={normalized_report_type}, building={current_building}, "
                    f"recipient_id={recipient_id}, receive_id_type={receive_id_type}, file_path={file_path}, error={exc}"
                )

        if failed_buildings and successful_buildings:
            status = "partial_failed"
        elif failed_buildings:
            status = "failed"
        else:
            status = "success"

        result = self._build_result_payload(
            started_at=started_at,
            status=status,
            report_type=normalized_report_type,
            scope=normalized_scope,
            building=normalized_building or "",
            target_month=target_month,
            successful_buildings=successful_buildings,
            failed_buildings=failed_buildings,
            message_ids=message_ids,
            error="; ".join(failure_details),
        )
        self._save_last_run_snapshot(normalized_report_type, result)

        emit_log(
            f"[月度统计表发送] 完成: report_type={normalized_report_type}, status={status}, "
            f"target_month={target_month}, successful={','.join(successful_buildings) or '-'}, "
            f"failed={','.join(failed_buildings) or '-'}"
        )
        if status == "failed":
            raise RuntimeError(result["error"] or "月度统计表发送失败")
        return result

    def run_send_test(
        self,
        *,
        report_type: str,
        receive_ids: Any,
        receive_id_type: str = "open_id",
        emit_log: Callable[[str], None] = print,
        source: str = "manual_test",
    ) -> Dict[str, Any]:
        normalized_report_type = self.normalize_report_type(report_type)
        target_receive_ids = self.normalize_receive_ids(receive_ids)
        configured_receive_id_type = str(receive_id_type or "").strip().lower() or "open_id"
        if not target_receive_ids:
            raise RuntimeError("测试发送缺少目标接收人 ID")

        report_snapshot = self._resolve_report_generation_snapshot(normalized_report_type)
        target_month = str(report_snapshot.get("target_month", "") or "").strip()
        if not target_month:
            raise RuntimeError("缺少最近成功生成的月度统计表文件，请先生成后再测试发送")

        files_by_building = report_snapshot.get("files_by_building", {})
        if not isinstance(files_by_building, dict) or not files_by_building:
            raise RuntimeError("缺少最近成功生成的月度统计表文件，请先生成后再测试发送")

        file_item = self._choose_test_file(files_by_building)
        if not file_item:
            report_label = "事件" if normalized_report_type == "event" else "变更"
            raise RuntimeError(f"当前没有可用于测试发送的{report_label}月度统计表文件")

        file_building = str(file_item.get("building", "") or "").strip()
        file_name = str(file_item.get("file_name", "") or "").strip()
        file_path = str(file_item.get("file_path", "") or "").strip()
        client = self._build_feishu_client()
        started_at = datetime.now()
        emit_log(
            f"[月度统计表测试发送] 开始: report_type={normalized_report_type}, target_month={target_month}, "
            f"receive_ids={','.join(target_receive_ids)}, receive_id_type={configured_receive_id_type}, "
            f"file_building={file_building}, file_name={file_name}, source={source}"
        )

        message_ids: Dict[str, str] = {}
        successful_receivers: List[str] = []
        failed_receivers: List[str] = []
        failure_details: List[str] = []

        try:
            upload_result = client.upload_file(file_path)
            file_key = str(upload_result.get("file_key", "") or "").strip()
            if not file_key:
                raise RuntimeError("测试发送文件上传后未返回 file_key")

            for recipient_id in target_receive_ids:
                effective_receive_id_type = self._resolve_effective_receive_id_type(recipient_id, configured_receive_id_type)
                try:
                    send_result = client.send_file_message(
                        receive_id=recipient_id,
                        receive_id_type=effective_receive_id_type,
                        file_key=file_key,
                    )
                    message_id = str(send_result.get("message_id", "") or "").strip()
                    successful_receivers.append(recipient_id)
                    if message_id:
                        message_ids[recipient_id] = message_id
                    emit_log(
                        f"[月度统计表测试发送] 发送成功: report_type={normalized_report_type}, recipient_id={recipient_id}, "
                        f"receive_id_type={effective_receive_id_type}, file_building={file_building}, file_name={file_name}, "
                        f"message_id={message_id or '-'}"
                    )
                except Exception as exc:
                    failed_receivers.append(recipient_id)
                    failure_details.append(f"{recipient_id}: {exc}")
                    emit_log(
                        f"[月度统计表测试发送] 发送失败: report_type={normalized_report_type}, recipient_id={recipient_id}, "
                        f"receive_id_type={effective_receive_id_type}, file_building={file_building}, file_name={file_name}, error={exc}"
                    )
        except Exception as exc:
            failed_receivers = list(target_receive_ids)
            failure_details = [str(exc)]
            emit_log(
                f"[月度统计表测试发送] 文件准备失败: report_type={normalized_report_type}, file_building={file_building}, "
                f"file_name={file_name}, error={exc}"
            )

        if failed_receivers and successful_receivers:
            status = "partial_failed"
        elif failed_receivers:
            status = "failed"
        else:
            status = "success"

        result = self._build_result_payload(
            started_at=started_at,
            status=status,
            report_type=normalized_report_type,
            scope="test",
            building=file_building,
            target_month=target_month,
            successful_buildings=[file_building] if successful_receivers and file_building else [],
            failed_buildings=[file_building] if failed_receivers and file_building else [],
            message_ids=message_ids,
            error="; ".join(failure_details),
            test_mode=True,
            test_receive_id=",".join(target_receive_ids),
            test_receive_id_type=configured_receive_id_type,
            test_receive_ids=target_receive_ids,
            test_successful_receivers=successful_receivers,
            test_failed_receivers=failed_receivers,
            test_file_building=file_building,
            test_file_name=file_name,
        )
        self._save_last_run_snapshot(normalized_report_type, result)

        emit_log(
            f"[月度统计表测试发送] 完成: report_type={normalized_report_type}, status={status}, target_month={target_month}, "
            f"success_receivers={','.join(successful_receivers) or '-'}, failed_receivers={','.join(failed_receivers) or '-'}, "
            f"file_building={file_building}, file_name={file_name}"
        )
        if status == "failed":
            raise RuntimeError(result["error"] or "月度统计表测试发送失败")
        return result
