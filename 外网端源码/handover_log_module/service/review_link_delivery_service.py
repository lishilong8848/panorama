from __future__ import annotations

import copy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.config.handover_segment_store import (
    HANDOVER_SEGMENT_BUILDINGS,
    building_code_from_name,
    handover_building_segment_path,
    read_segment_document,
)
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from app.modules.feishu.service.im_file_message_client import FeishuImFileMessageClient
from handover_log_module.service.review_access_snapshot_service import (
    load_review_access_state,
    materialize_review_access_snapshot,
    normalize_review_base_url,
)
from handover_log_module.service.review_session_service import ReviewSessionService


STATION_110_BUILDING = "110站"
STATION_110_CODE = "110"


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _looks_like_runtime_config(cfg: Dict[str, Any]) -> bool:
    return (
        isinstance(cfg.get("download"), dict)
        and isinstance(cfg.get("network"), dict)
        and isinstance(cfg.get("handover_log"), dict)
    )


def _resolve_handover_config(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    from app.config.config_adapter import adapt_runtime_config, ensure_v3_config

    cfg = runtime_config if isinstance(runtime_config, dict) else {}
    if "review_ui" in cfg and isinstance(cfg.get("review_ui"), dict):
        return copy.deepcopy(cfg)

    if _looks_like_runtime_config(cfg):
        runtime_cfg = copy.deepcopy(cfg)
    else:
        runtime_cfg = adapt_runtime_config(ensure_v3_config(cfg))

    handover_cfg = (
        copy.deepcopy(runtime_cfg.get("handover_log", {}))
        if isinstance(runtime_cfg.get("handover_log", {}), dict)
        else {}
    )
    handover_cfg["_global_feishu"] = copy.deepcopy(
        runtime_cfg.get("feishu", {}) if isinstance(runtime_cfg.get("feishu", {}), dict) else {}
    )
    handover_cfg["_global_paths"] = copy.deepcopy(
        runtime_cfg.get("paths", {}) if isinstance(runtime_cfg.get("paths", {}), dict) else {}
    )
    handover_cfg["_shared_bridge"] = copy.deepcopy(
        runtime_cfg.get("shared_bridge", {}) if isinstance(runtime_cfg.get("shared_bridge", {}), dict) else {}
    )
    return handover_cfg


def _normalize_delivery_state(raw: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    return {
        "status": str(payload.get("status", "") or "").strip().lower(),
        "last_attempt_at": str(payload.get("last_attempt_at", "") or "").strip(),
        "last_sent_at": str(payload.get("last_sent_at", "") or "").strip(),
        "error": str(payload.get("error", "") or "").strip(),
        "url": str(payload.get("url", "") or "").strip(),
        "successful_recipients": [
            str(item or "").strip()
            for item in (payload.get("successful_recipients", []) if isinstance(payload.get("successful_recipients", []), list) else [])
            if str(item or "").strip()
        ],
        "failed_recipients": [
            {
                "open_id": str(item.get("open_id", "") or "").strip(),
                "note": str(item.get("note", "") or "").strip(),
                "error": str(item.get("error", "") or "").strip(),
            }
            for item in (payload.get("failed_recipients", []) if isinstance(payload.get("failed_recipients", []), list) else [])
            if isinstance(item, dict)
        ],
        "source": str(payload.get("source", "") or "").strip().lower(),
        "auto_attempted": bool(payload.get("auto_attempted", False)),
        "auto_attempted_at": str(payload.get("auto_attempted_at", "") or "").strip(),
    }


class ReviewLinkDeliveryService:
    def __init__(self, runtime_config: Dict[str, Any], *, config_path: str | Path | None = None) -> None:
        self.runtime_config = runtime_config if isinstance(runtime_config, dict) else {}
        self.config_path = Path(config_path) if config_path else None
        self.handover_cfg = _resolve_handover_config(self.runtime_config)
        self._review_service = ReviewSessionService(self.handover_cfg)

    def _review_cfg(self) -> Dict[str, Any]:
        review_ui = self.handover_cfg.get("review_ui", {})
        return review_ui if isinstance(review_ui, dict) else {}

    def _build_feishu_client(self) -> FeishuImFileMessageClient:
        global_feishu = require_feishu_auth_settings(self.handover_cfg, config_path=self.config_path)
        return FeishuImFileMessageClient(
            app_id=str(global_feishu.get("app_id", "") or "").strip(),
            app_secret=str(global_feishu.get("app_secret", "") or "").strip(),
            timeout=int(global_feishu.get("timeout", 30) or 30),
            request_retry_count=int(global_feishu.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2) or 2),
        )

    @staticmethod
    def _normalize_recipient_rows(raw_items: Any) -> Dict[str, Any]:
        recipients: List[Dict[str, str]] = []
        seen_open_ids: set[str] = set()
        invalid_count = 0
        enabled_count = 0
        disabled_count = 0
        normalized_items = raw_items if isinstance(raw_items, list) else []
        for raw in normalized_items:
            if not isinstance(raw, dict):
                invalid_count += 1
                continue
            open_id = str(raw.get("open_id", "") or "").strip()
            note = str(raw.get("note", "") or "").strip()
            if not open_id or open_id in seen_open_ids:
                invalid_count += 1
                continue
            seen_open_ids.add(open_id)
            enabled = raw.get("enabled", True)
            enabled_bool = enabled if isinstance(enabled, bool) else True
            if enabled_bool:
                enabled_count += 1
                recipients.append({"open_id": open_id, "note": note, "enabled": True})
            else:
                disabled_count += 1
        return {
            "raw_count": len(normalized_items),
            "invalid_count": invalid_count,
            "enabled_count": enabled_count,
            "disabled_count": disabled_count,
            "recipients": recipients,
            "open_ids": [item["open_id"] for item in recipients],
        }

    def _recipient_snapshot_for_building(self, building: str) -> Dict[str, Any]:
        building_text = str(building or "").strip()
        if not building_text:
            return {
                "building": "",
                "revision": 0,
                "updated_at": "",
                "raw_count": 0,
                "invalid_count": 0,
                "enabled_count": 0,
                "disabled_count": 0,
                "recipients": [],
                "open_ids": [],
                "source": "empty",
            }

        if self.config_path:
            try:
                document = read_segment_document(
                    handover_building_segment_path(
                        self.config_path,
                        building_code_from_name(building_text),
                    )
                )
                payload = document.get("data", {}) if isinstance(document, dict) else {}
                review_ui = payload.get("review_ui", {}) if isinstance(payload, dict) else {}
                by_building = (
                    review_ui.get("review_link_recipients_by_building", {})
                    if isinstance(review_ui.get("review_link_recipients_by_building", {}), dict)
                    else {}
                )
                normalized = self._normalize_recipient_rows(by_building.get(building_text, []))
                return {
                    "building": building_text,
                    "revision": int(document.get("revision", 0) or 0) if isinstance(document, dict) else 0,
                    "updated_at": str(document.get("updated_at", "") or "").strip() if isinstance(document, dict) else "",
                    "source": "segment",
                    **normalized,
                }
            except Exception:
                pass

        review_cfg = self._review_cfg()
        by_building = (
            review_cfg.get("review_link_recipients_by_building", {})
            if isinstance(review_cfg.get("review_link_recipients_by_building", {}), dict)
            else {}
        )
        normalized = self._normalize_recipient_rows(by_building.get(building_text, []) if isinstance(by_building, dict) else [])
        return {
            "building": building_text,
            "revision": 0,
            "updated_at": "",
            "source": "runtime",
            **normalized,
        }

    def _recipients_for_building(self, building: str) -> List[Dict[str, str]]:
        return list(self._recipient_snapshot_for_building(building).get("recipients", []))

    def build_recipient_status_by_building(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for building in [*HANDOVER_SEGMENT_BUILDINGS, STATION_110_BUILDING]:
            snapshot = self._recipient_snapshot_for_building(building)
            recipient_count = len(snapshot.get("recipients", []))
            raw_count = int(snapshot.get("raw_count", 0) or 0)
            invalid_count = int(snapshot.get("invalid_count", 0) or 0)
            enabled_count = int(snapshot.get("enabled_count", 0) or 0)
            disabled_count = int(snapshot.get("disabled_count", 0) or 0)
            if recipient_count > 0:
                status_text = "已保存，可发送"
                reason = ""
            elif raw_count > 0 and disabled_count > 0 and invalid_count == 0:
                status_text = "已配置但全部停用"
                reason = "当前楼审核链接接收人均未启用"
            elif raw_count > 0 and invalid_count >= raw_count:
                status_text = "当前楼未配置接收人"
                reason = "当前楼审核链接接收人无有效 open_id"
            elif raw_count > 0 and disabled_count > 0:
                status_text = "已配置但无启用接收人"
                reason = "当前楼无启用接收人，且存在无效接收人配置"
            else:
                status_text = "当前楼未配置接收人"
                reason = "当前楼未配置审核链接接收人"
            rows.append(
                {
                    "building": building,
                    "revision": int(snapshot.get("revision", 0) or 0),
                    "updated_at": str(snapshot.get("updated_at", "") or "").strip(),
                    "recipient_count": recipient_count,
                    "raw_count": raw_count,
                    "invalid_count": invalid_count,
                    "enabled_count": enabled_count,
                    "disabled_count": disabled_count,
                    "open_ids": [
                        str(item or "").strip()
                        for item in snapshot.get("open_ids", [])
                        if str(item or "").strip()
                    ],
                    "has_recipients": recipient_count > 0,
                    "status_text": status_text,
                    "reason": reason,
                    "source": str(snapshot.get("source", "") or "").strip(),
                }
            )
        return rows

    @staticmethod
    def _build_message(session: Dict[str, Any], url: str, *, manual_test: bool = False) -> str:
        building = str(session.get("building", "") or "").strip()
        duty_date = str(session.get("duty_date", "") or "").strip()
        duty_shift = str(session.get("duty_shift", "") or "").strip().lower()
        shift_text = "白班" if duty_shift == "day" else "夜班" if duty_shift == "night" else duty_shift or ""
        lines = [
            (
                "这是一条交接班审核链接测试消息，请在办公电脑的浏览器中打开。"
                if manual_test
                else "这是一条交接班审核访问链接，请在办公电脑的浏览器中打开。"
            )
        ]
        if building:
            lines.append(f"楼栋：{building}")
        if duty_date:
            lines.append(f"日期：{duty_date}")
        if shift_text:
            lines.append(f"班次：{shift_text}")
        if url:
            lines.append(f"审核链接：{url}")
        elif manual_test:
            lines.append("审核链接：当前尚未生成，本次仅测试发送通道")
        return "\n".join(lines)

    @staticmethod
    def _resolve_effective_receive_id_type(recipient_id: str, configured_receive_id_type: str = "open_id") -> str:
        rid = str(recipient_id or "").strip()
        configured = str(configured_receive_id_type or "").strip().lower() or "open_id"
        if rid.startswith("ou_"):
            return "open_id"
        if "@" in rid and configured == "user_id":
            return "email"
        digits_only = rid.removeprefix("+")
        if digits_only.isdigit() and len(digits_only) >= 11 and configured == "user_id":
            return "mobile"
        return configured

    @staticmethod
    def _review_url_for_building(snapshot: Dict[str, Any], building: str) -> str:
        building_text = str(building or "").strip()
        rows = snapshot.get("review_links", []) if isinstance(snapshot, dict) else []
        for item in rows if isinstance(rows, list) else []:
            if not isinstance(item, dict):
                continue
            if str(item.get("building", "") or "").strip() == building_text:
                return str(item.get("url", "") or "").strip()
        return ""

    @staticmethod
    def _fallback_review_url_for_building(snapshot: Dict[str, Any], building: str) -> str:
        building_text = str(building or "").strip()
        if not building_text:
            return ""
        effective_base_url = str(
            snapshot.get("review_base_url_effective", "") or snapshot.get("review_base_url", "") or ""
        ).strip().rstrip("/")
        if building_text in {STATION_110_BUILDING, STATION_110_CODE}:
            return f"{effective_base_url}/handover/review/{STATION_110_CODE}" if effective_base_url else ""
        try:
            building_code = building_code_from_name(building_text)
        except Exception:  # noqa: BLE001
            building_code = ""
        if not building_code:
            return ""
        if not effective_base_url:
            return ""
        return f"{effective_base_url}/handover/review/{building_code.lower()}"

    @staticmethod
    def _default_station_110_context(now: datetime | None = None) -> Dict[str, str]:
        current = now or datetime.now()
        if current.hour < 8:
            return {"duty_date": (current - timedelta(days=1)).strftime("%Y-%m-%d"), "duty_shift": "night"}
        if current.hour < 20:
            return {"duty_date": current.strftime("%Y-%m-%d"), "duty_shift": "day"}
        return {"duty_date": current.strftime("%Y-%m-%d"), "duty_shift": "night"}

    def _manual_test_review_url_for_building(self, snapshot: Dict[str, Any], building: str) -> str:
        url = self._review_url_for_building(snapshot, building)
        if url:
            return url
        url = self._fallback_review_url_for_building(snapshot, building)
        if url:
            return url

        review_cfg = self._review_cfg()
        configured_base_url = normalize_review_base_url(
            review_cfg.get("public_base_url", "") if isinstance(review_cfg, dict) else ""
        )
        if configured_base_url:
            return self._fallback_review_url_for_building(
                {"review_base_url_effective": configured_base_url},
                building,
            )

        persisted_state = load_review_access_state(self.handover_cfg)
        persisted_base_url = normalize_review_base_url(persisted_state.get("effective_base_url", ""))
        if persisted_base_url:
            return self._fallback_review_url_for_building(
                {"review_base_url_effective": persisted_base_url},
                building,
            )
        return ""

    def _persist_delivery_state(
        self,
        session_id: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        return self._review_service.update_review_link_delivery(
            session_id=session_id,
            review_link_delivery=payload,
        )

    def _resolve_batch_sessions(
        self,
        *,
        batch_key: str,
        building: str = "",
    ) -> List[Dict[str, Any]]:
        batch_key_text = str(batch_key or "").strip()
        building_text = str(building or "").strip()
        if not batch_key_text:
            return []
        parse_batch_key = getattr(self._review_service, "parse_batch_key", None)
        if callable(parse_batch_key):
            duty_date, duty_shift = parse_batch_key(batch_key_text)
        else:
            if "|" in batch_key_text:
                duty_date, duty_shift = batch_key_text.split("|", 1)
                duty_date = str(duty_date or "").strip()
                duty_shift = str(duty_shift or "").strip().lower()
            else:
                duty_date, duty_shift = "", ""
        sessions = self._review_service.list_batch_sessions(batch_key_text)
        deduped: Dict[str, Dict[str, Any]] = {}
        for session in sessions:
            if not isinstance(session, dict):
                continue
            session_id = str(session.get("session_id", "") or "").strip()
            if session_id:
                deduped[session_id] = session

        # Sending review links should tolerate stale review-session state:
        # if the output file already exists, recover the batch session from disk.
        list_buildings = getattr(self._review_service, "list_buildings", None)
        get_session_for_building_duty = getattr(self._review_service, "get_session_for_building_duty", None)
        if building_text:
            target_buildings = [building_text]
        elif callable(list_buildings):
            target_buildings = list_buildings()
        else:
            target_buildings = []
        if duty_date and duty_shift and callable(get_session_for_building_duty):
            for building_name in target_buildings:
                recovered = get_session_for_building_duty(
                    building_name,
                    duty_date,
                    duty_shift,
                )
                if not isinstance(recovered, dict):
                    continue
                session_id = str(recovered.get("session_id", "") or "").strip()
                if session_id:
                    deduped[session_id] = recovered

        output = list(deduped.values())
        if building_text:
            output = [item for item in output if str(item.get("building", "") or "").strip() == building_text]
        output.sort(key=lambda item: str(item.get("building", "") or "").strip())
        return output

    def send_for_session(
        self,
        session: Dict[str, Any],
        *,
        source: str = "auto",
        force: bool = False,
        emit_log: Callable[[str], None] = print,
        review_access_snapshot: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        normalized_session = dict(session) if isinstance(session, dict) else {}
        session_id = str(normalized_session.get("session_id", "") or "").strip()
        building = str(normalized_session.get("building", "") or "").strip()
        if not session_id or not building:
            raise ValueError("审核链接发送缺少 session_id/building")

        source_text = str(source or "auto").strip().lower() or "auto"
        delivery_state = _normalize_delivery_state(normalized_session.get("review_link_delivery", {}))
        if source_text == "auto" and delivery_state["auto_attempted"] and not force:
            emit_log(
                "[交接班][审核链接发送] 跳过自动发送 "
                f"building={building}, session_id={session_id}, reason=当前会话已自动发送过"
            )
            return delivery_state

        recipient_snapshot = self._recipient_snapshot_for_building(building)
        recipients = list(recipient_snapshot.get("recipients", []))
        snapshot = (
            review_access_snapshot
            if isinstance(review_access_snapshot, dict)
            else materialize_review_access_snapshot(self.handover_cfg)
        )
        url = self._review_url_for_building(snapshot, building) or self._manual_test_review_url_for_building(snapshot, building)
        emit_log(
            "[交接班][审核链接发送] 开始发送 "
            f"building={building}, session_id={session_id}, revision={int(recipient_snapshot.get('revision', 0) or 0)}, "
            f"recipients={len(recipients)}, raw={int(recipient_snapshot.get('raw_count', 0) or 0)}, "
            f"enabled={int(recipient_snapshot.get('enabled_count', 0) or 0)}, "
            f"disabled={int(recipient_snapshot.get('disabled_count', 0) or 0)}, "
            f"invalid={int(recipient_snapshot.get('invalid_count', 0) or 0)}, "
            f"open_ids={recipient_snapshot.get('open_ids', [])}, access_ready={bool(url)}, source={source_text}, "
            f"recipient_source={str(recipient_snapshot.get('source', '') or '').strip() or 'unknown'}"
        )
        if not recipients:
            raw_count = int(recipient_snapshot.get("raw_count", 0) or 0)
            invalid_count = int(recipient_snapshot.get("invalid_count", 0) or 0)
            disabled_count = int(recipient_snapshot.get("disabled_count", 0) or 0)
            reason = (
                "当前楼审核链接接收人均未启用"
                if raw_count > 0 and disabled_count > 0 and invalid_count == 0
                else (
                    "当前楼审核链接接收人无有效 open_id"
                    if raw_count > 0 and invalid_count >= raw_count
                    else "当前楼未配置审核链接接收人"
                )
            )
            next_state = {
                **delivery_state,
                "status": "disabled" if "未启用" in reason else "unconfigured",
                "error": reason,
                "source": source_text,
            }
            self._persist_delivery_state(session_id, next_state)
            emit_log(
                "[交接班][审核链接发送] 跳过发送 "
                f"building={building}, session_id={session_id}, reason={reason}"
            )
            return next_state

        if not url:
            next_state = {
                **delivery_state,
                "status": "pending_access",
                "error": "审核访问地址尚未就绪",
                "url": "",
                "source": source_text,
            }
            self._persist_delivery_state(session_id, next_state)
            emit_log(
                "[交接班][审核链接发送] 跳过发送 "
                f"building={building}, session_id={session_id}, reason=审核访问地址尚未就绪"
            )
            return next_state

        message_text = self._build_message(normalized_session, url)
        attempt_at = _now_text()
        successful_recipients: List[str] = []
        failed_recipients: List[Dict[str, str]] = []
        client = self._build_feishu_client()
        for recipient in recipients:
            open_id = recipient["open_id"]
            note = recipient["note"]
            receive_id_type = self._resolve_effective_receive_id_type(open_id, "open_id")
            try:
                client.send_text_message(
                    receive_id=open_id,
                    receive_id_type=receive_id_type,
                    text=message_text,
                )
                successful_recipients.append(open_id)
                emit_log(
                    "[交接班][审核链接发送] 发送成功 "
                    f"building={building}, session_id={session_id}, open_id={open_id}, "
                    f"receive_id_type={receive_id_type}, note={note or '-'}"
                )
            except Exception as exc:  # noqa: BLE001
                failed_recipients.append(
                    {
                        "open_id": open_id,
                        "note": note,
                        "error": str(exc),
                    }
                )
                emit_log(
                    "[交接班][审核链接发送] 发送失败 "
                    f"building={building}, session_id={session_id}, open_id={open_id}, "
                    f"receive_id_type={receive_id_type}, note={note or '-'}, error={exc}"
                )

        if successful_recipients and failed_recipients:
            status = "partial_failed"
            error = "部分收件人发送失败"
        elif successful_recipients:
            status = "success"
            error = ""
        else:
            status = "failed"
            error = "全部收件人发送失败"

        next_state = {
            **delivery_state,
            "status": status,
            "last_attempt_at": attempt_at,
            "last_sent_at": attempt_at if successful_recipients else str(delivery_state.get("last_sent_at", "") or "").strip(),
            "error": error,
            "url": url,
            "successful_recipients": successful_recipients,
            "failed_recipients": failed_recipients,
            "source": source_text,
            "auto_attempted": bool(delivery_state.get("auto_attempted", False)) or source_text == "auto",
            "auto_attempted_at": attempt_at if source_text == "auto" else str(delivery_state.get("auto_attempted_at", "") or "").strip(),
        }
        self._persist_delivery_state(session_id, next_state)
        emit_log(
            "[交接班][审核链接发送] 完成 "
            f"building={building}, session_id={session_id}, status={status}, "
            f"successful={len(successful_recipients)}, failed={len(failed_recipients)}"
        )
        return next_state

    def send_for_generation_result(
        self,
        result: Dict[str, Any],
        *,
        emit_log: Callable[[str], None] = print,
    ) -> None:
        rows = result.get("results", []) if isinstance(result, dict) else []
        if not isinstance(rows, list) or not rows:
            return
        snapshot = materialize_review_access_snapshot(self.handover_cfg)
        for row in rows:
            if not isinstance(row, dict) or not bool(row.get("success", False)):
                continue
            review_session = row.get("review_session", {})
            if not isinstance(review_session, dict) or not str(review_session.get("session_id", "") or "").strip():
                continue
            try:
                state = self.send_for_session(
                    review_session,
                    source="auto",
                    force=True,
                    emit_log=emit_log,
                    review_access_snapshot=snapshot,
                )
                row["review_session"] = {
                    **review_session,
                    "review_link_delivery": state,
                }
            except Exception as exc:  # noqa: BLE001
                emit_log(
                    "[交接班][审核链接发送] 自动发送失败但不阻断主流程 "
                    f"building={str(review_session.get('building', '') or '-').strip() or '-'}, error={exc}"
                )

    def send_for_batch(
        self,
        *,
        batch_key: str,
        building: str = "",
        force: bool = False,
        source: str = "manual",
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        batch_key_text = str(batch_key or "").strip()
        building_text = str(building or "").strip()
        if not batch_key_text:
            raise ValueError("batch_key 不能为空")
        emit_log(
            "[交接班][审核链接发送] 批次开始 "
            f"batch={batch_key_text}, building={building_text or '-'}, source={str(source or '').strip().lower() or 'manual'}, force={bool(force)}"
        )
        sessions = self._resolve_batch_sessions(
            batch_key=batch_key_text,
            building=building_text,
        )
        if not sessions:
            emit_log(
                "[交接班][审核链接发送] 批次跳过 "
                f"batch={batch_key_text}, building={building_text or '-'}, reason=未找到对应审核会话"
            )
            raise ValueError("未找到对应审核会话")
        snapshot = materialize_review_access_snapshot(self.handover_cfg)
        results: List[Dict[str, Any]] = []
        for session in sessions:
            results.append(
                {
                    "session_id": str(session.get("session_id", "") or "").strip(),
                    "building": str(session.get("building", "") or "").strip(),
                    "delivery": self.send_for_session(
                        session,
                        source=source,
                        force=force,
                        emit_log=emit_log,
                        review_access_snapshot=snapshot,
                    ),
                }
            )
        success_count = sum(
            1
            for item in results
            if str(item.get("delivery", {}).get("status", "") or "").strip().lower() == "success"
        )
        partial_failed_count = sum(
            1
            for item in results
            if str(item.get("delivery", {}).get("status", "") or "").strip().lower() == "partial_failed"
        )
        unsent_rows = [
            item
            for item in results
            if str(item.get("delivery", {}).get("status", "") or "").strip().lower()
            not in {"success", "partial_failed"}
        ]
        emit_log(
            "[交接班][审核链接发送] 批次完成 "
            f"batch={batch_key_text}, building={building_text or '-'}, "
            f"success={success_count}, partial_failed={partial_failed_count}, unsent={len(unsent_rows)}"
        )
        if source != "auto" and success_count == 0 and partial_failed_count == 0 and unsent_rows:
            first_error = str(unsent_rows[0].get("delivery", {}).get("error", "") or "").strip() or "审核链接发送失败"
            raise RuntimeError(first_error)
        return {
            "batch_key": batch_key_text,
            "building": building_text,
            "results": results,
        }

    def send_manual_test(
        self,
        *,
        building: str,
        batch_key: str = "",
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        building_text = str(building or "").strip()
        batch_key_text = str(batch_key or "").strip()
        if not building_text:
            raise ValueError("building 不能为空")
        if building_text in {STATION_110_BUILDING, STATION_110_CODE}:
            duty_date, duty_shift = self._resolve_duty_context_from_batch_key(batch_key_text)
            return self.send_station_110_review_link(
                duty_date=duty_date,
                duty_shift=duty_shift,
                source="manual_test",
                emit_log=emit_log,
            )
        recipient_snapshot = self._recipient_snapshot_for_building(building_text)
        recipients = list(recipient_snapshot.get("recipients", []))
        if not recipients:
            raw_count = int(recipient_snapshot.get("raw_count", 0) or 0)
            invalid_count = int(recipient_snapshot.get("invalid_count", 0) or 0)
            disabled_count = int(recipient_snapshot.get("disabled_count", 0) or 0)
            if raw_count > 0 and disabled_count > 0 and invalid_count == 0:
                raise ValueError("当前楼审核链接接收人均未启用")
            if raw_count > 0 and invalid_count >= raw_count:
                raise ValueError("当前楼审核链接接收人无有效 open_id")
            raise ValueError("当前楼未配置审核链接接收人")

        duty_date = ""
        duty_shift = ""
        parse_batch_key = getattr(self._review_service, "parse_batch_key", None)
        if callable(parse_batch_key):
            duty_date, duty_shift = parse_batch_key(batch_key_text)
        elif "|" in batch_key_text:
            duty_date, duty_shift = batch_key_text.split("|", 1)
            duty_date = str(duty_date or "").strip()
            duty_shift = str(duty_shift or "").strip().lower()

        snapshot = materialize_review_access_snapshot(self.handover_cfg)
        url = self._manual_test_review_url_for_building(snapshot, building_text)
        emit_log(
            "[交接班][审核链接发送测试] 开始发送 "
            f"building={building_text}, batch={batch_key_text or '-'}, revision={int(recipient_snapshot.get('revision', 0) or 0)}, "
            f"recipients={len(recipients)}, raw={int(recipient_snapshot.get('raw_count', 0) or 0)}, "
            f"enabled={int(recipient_snapshot.get('enabled_count', 0) or 0)}, "
            f"disabled={int(recipient_snapshot.get('disabled_count', 0) or 0)}, "
            f"invalid={int(recipient_snapshot.get('invalid_count', 0) or 0)}, "
            f"open_ids={recipient_snapshot.get('open_ids', [])}, access_ready={bool(url)}, "
            f"recipient_source={str(recipient_snapshot.get('source', '') or '').strip() or 'unknown'}"
        )
        message_text = self._build_message(
            {
                "building": building_text,
                "duty_date": duty_date,
                "duty_shift": duty_shift,
            },
            url,
            manual_test=True,
        )
        successful_recipients: List[str] = []
        failed_recipients: List[Dict[str, str]] = []
        client = self._build_feishu_client()
        for recipient in recipients:
            open_id = recipient["open_id"]
            note = recipient["note"]
            receive_id_type = self._resolve_effective_receive_id_type(open_id, "open_id")
            try:
                client.send_text_message(
                    receive_id=open_id,
                    receive_id_type=receive_id_type,
                    text=message_text,
                )
                successful_recipients.append(open_id)
                emit_log(
                    "[交接班][审核链接发送测试] 发送成功 "
                    f"building={building_text}, open_id={open_id}, receive_id_type={receive_id_type}, note={note or '-'}"
                )
            except Exception as exc:  # noqa: BLE001
                failed_recipients.append(
                    {
                        "open_id": open_id,
                        "note": note,
                        "error": str(exc),
                    }
                )
                emit_log(
                    "[交接班][审核链接发送测试] 发送失败 "
                    f"building={building_text}, open_id={open_id}, receive_id_type={receive_id_type}, note={note or '-'}, error={exc}"
                )

        if successful_recipients and failed_recipients:
            status = "partial_failed"
            error = "部分收件人发送失败"
        elif successful_recipients:
            status = "success"
            error = ""
        else:
            status = "failed"
            error = "全部收件人发送失败"
        emit_log(
            "[交接班][审核链接发送测试] 完成 "
            f"building={building_text}, batch={batch_key_text or '-'}, status={status}, "
            f"successful={len(successful_recipients)}, failed={len(failed_recipients)}"
        )
        return {
            "batch_key": batch_key_text,
            "building": building_text,
            "status": status,
            "error": error,
            "message_text": message_text,
            "successful_recipients": successful_recipients,
            "failed_recipients": failed_recipients,
            "review_url": url,
        }

    def _resolve_duty_context_from_batch_key(self, batch_key: str) -> tuple[str, str]:
        batch_key_text = str(batch_key or "").strip()
        duty_date = ""
        duty_shift = ""
        parse_batch_key = getattr(self._review_service, "parse_batch_key", None)
        if callable(parse_batch_key):
            duty_date, duty_shift = parse_batch_key(batch_key_text)
        elif "|" in batch_key_text:
            duty_date, duty_shift = batch_key_text.split("|", 1)
        return str(duty_date or "").strip(), str(duty_shift or "").strip().lower()

    def send_station_110_review_link(
        self,
        *,
        duty_date: str = "",
        duty_shift: str = "",
        source: str = "scheduler",
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        context = self._default_station_110_context()
        duty_date_text = str(duty_date or "").strip() or context["duty_date"]
        duty_shift_text = str(duty_shift or "").strip().lower() or context["duty_shift"]
        if duty_shift_text not in {"day", "night"}:
            raise ValueError("110站审核链接发送班次必须是 day/night")

        building_text = STATION_110_BUILDING
        recipient_snapshot = self._recipient_snapshot_for_building(building_text)
        recipients = list(recipient_snapshot.get("recipients", []))
        snapshot = materialize_review_access_snapshot(self.handover_cfg)
        url = self._review_url_for_building(snapshot, building_text) or self._manual_test_review_url_for_building(
            snapshot,
            building_text,
        )
        source_text = str(source or "scheduler").strip().lower() or "scheduler"
        emit_log(
            "[交接班][110站审核链接发送] 开始发送 "
            f"duty_date={duty_date_text}, duty_shift={duty_shift_text}, "
            f"recipients={len(recipients)}, raw={int(recipient_snapshot.get('raw_count', 0) or 0)}, "
            f"enabled={int(recipient_snapshot.get('enabled_count', 0) or 0)}, "
            f"disabled={int(recipient_snapshot.get('disabled_count', 0) or 0)}, "
            f"invalid={int(recipient_snapshot.get('invalid_count', 0) or 0)}, "
            f"open_ids={recipient_snapshot.get('open_ids', [])}, access_ready={bool(url)}, source={source_text}, "
            f"recipient_source={str(recipient_snapshot.get('source', '') or '').strip() or 'unknown'}"
        )
        if not recipients:
            raw_count = int(recipient_snapshot.get("raw_count", 0) or 0)
            invalid_count = int(recipient_snapshot.get("invalid_count", 0) or 0)
            disabled_count = int(recipient_snapshot.get("disabled_count", 0) or 0)
            reason = (
                "110站审核链接接收人均未启用"
                if raw_count > 0 and disabled_count > 0 and invalid_count == 0
                else (
                    "110站审核链接接收人无有效 open_id"
                    if raw_count > 0 and invalid_count >= raw_count
                    else "110站未配置审核链接接收人"
                )
            )
            emit_log(f"[交接班][110站审核链接发送] 跳过发送: {reason}")
            return {
                "building": building_text,
                "status": "disabled" if "未启用" in reason else "unconfigured",
                "error": reason,
                "duty_date": duty_date_text,
                "duty_shift": duty_shift_text,
                "review_url": url,
                "successful_recipients": [],
                "failed_recipients": [],
            }
        if not url:
            reason = "审核访问地址尚未就绪"
            emit_log(f"[交接班][110站审核链接发送] 跳过发送: {reason}")
            return {
                "building": building_text,
                "status": "pending_access",
                "error": reason,
                "duty_date": duty_date_text,
                "duty_shift": duty_shift_text,
                "review_url": "",
                "successful_recipients": [],
                "failed_recipients": [],
            }

        message_text = self._build_message(
            {
                "building": building_text,
                "duty_date": duty_date_text,
                "duty_shift": duty_shift_text,
            },
            url,
        )
        successful_recipients: List[str] = []
        failed_recipients: List[Dict[str, str]] = []
        client = self._build_feishu_client()
        for recipient in recipients:
            open_id = recipient["open_id"]
            note = recipient["note"]
            receive_id_type = self._resolve_effective_receive_id_type(open_id, "open_id")
            try:
                client.send_text_message(
                    receive_id=open_id,
                    receive_id_type=receive_id_type,
                    text=message_text,
                )
                successful_recipients.append(open_id)
                emit_log(
                    "[交接班][110站审核链接发送] 发送成功 "
                    f"open_id={open_id}, receive_id_type={receive_id_type}, note={note or '-'}"
                )
            except Exception as exc:  # noqa: BLE001
                failed_recipients.append(
                    {
                        "open_id": open_id,
                        "note": note,
                        "error": str(exc),
                    }
                )
                emit_log(
                    "[交接班][110站审核链接发送] 发送失败 "
                    f"open_id={open_id}, receive_id_type={receive_id_type}, note={note or '-'}, error={exc}"
                )

        if successful_recipients and failed_recipients:
            status = "partial_failed"
            error = "部分收件人发送失败"
        elif successful_recipients:
            status = "success"
            error = ""
        else:
            status = "failed"
            error = "全部收件人发送失败"
        emit_log(
            "[交接班][110站审核链接发送] 完成 "
            f"duty_date={duty_date_text}, duty_shift={duty_shift_text}, status={status}, "
            f"successful={len(successful_recipients)}, failed={len(failed_recipients)}"
        )
        return {
            "building": building_text,
            "status": status,
            "error": error,
            "duty_date": duty_date_text,
            "duty_shift": duty_shift_text,
            "message_text": message_text,
            "successful_recipients": successful_recipients,
            "failed_recipients": failed_recipients,
            "review_url": url,
        }

    def validate_manual_send_preflight(
        self,
        *,
        batch_key: str,
        building: str = "",
    ) -> Dict[str, Any]:
        batch_key_text = str(batch_key or "").strip()
        building_text = str(building or "").strip()
        if building_text:
            recipient_snapshot = self._recipient_snapshot_for_building(building_text)
            recipients = list(recipient_snapshot.get("recipients", []))
            if not recipients:
                raw_count = int(recipient_snapshot.get("raw_count", 0) or 0)
                invalid_count = int(recipient_snapshot.get("invalid_count", 0) or 0)
                disabled_count = int(recipient_snapshot.get("disabled_count", 0) or 0)
                if raw_count > 0 and disabled_count > 0 and invalid_count == 0:
                    raise ValueError("当前楼审核链接接收人均未启用")
                if raw_count > 0 and invalid_count >= raw_count:
                    raise ValueError("当前楼审核链接接收人无有效 open_id")
                raise ValueError("当前楼未配置审核链接接收人")
        return {
            "batch_key": batch_key_text,
            "building": building_text,
            "session_count": len(
                self._resolve_batch_sessions(
                    batch_key=batch_key_text,
                    building=building_text,
                )
            )
            if batch_key_text
            else 0,
        }

    def dispatch_pending_review_links(
        self,
        *,
        emit_log: Callable[[str], None] = print,
    ) -> List[Dict[str, Any]]:
        snapshot = materialize_review_access_snapshot(self.handover_cfg)
        if not str(snapshot.get("review_base_url_effective", "") or "").strip():
            return []
        pending_sessions = [
            session
            for session in self._review_service.list_sessions()
            if str(session.get("review_link_delivery", {}).get("status", "") or "").strip().lower() == "pending_access"
            and not bool(session.get("review_link_delivery", {}).get("auto_attempted", False))
        ]
        results: List[Dict[str, Any]] = []
        for session in pending_sessions:
            try:
                results.append(
                    {
                        "session_id": str(session.get("session_id", "") or "").strip(),
                        "building": str(session.get("building", "") or "").strip(),
                        "delivery": self.send_for_session(
                            session,
                            source="auto",
                            force=False,
                            emit_log=emit_log,
                            review_access_snapshot=snapshot,
                        ),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                emit_log(
                    "[交接班][审核链接发送] 待发送补发失败 "
                    f"session_id={str(session.get('session_id', '') or '-').strip() or '-'}, error={exc}"
                )
        return results
