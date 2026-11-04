from __future__ import annotations

import hashlib
import json
import threading
import uuid
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.bitable_target_resolver import BitableTargetResolver
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from app.modules.internal_bridge_http.service.client import InternalBridgeHttpClient
from app.shared.utils.cached_json_file import load_cached_json, save_cached_json
from app.shared.utils.xlsx_archive import validate_xlsx_archive


FEATURE = "energy_report_upload"
TARGET_TOKEN = "HScCwZt9QiqPCUkSrHjcbBL1ngb"
TABLE_ID = "tblZEGGCmrdQH162"
BUILDINGS = ("A楼", "B楼", "C楼", "D楼", "E楼")
ATTACHMENTS = {
    "pue_confirmation_family": "PUE确认表",
    "utility_feeder_deviation_family": "市电及馈线偏差（无市电进线）",
}
FIELD_TYPES = {"月份": 1, "楼栋": 1, **dict.fromkeys(ATTACHMENTS.values(), 17), "同步标识": 1}
MIME_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
_QUEUE_LOCK = threading.Lock()


def due_period(now: datetime) -> str:
    if now < now.replace(day=1, hour=8, minute=30, second=0, microsecond=0):
        return ""
    return (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")


def _state_path(config: dict[str, Any]) -> Path:
    root = config.get("paths", {}).get("runtime_state_root") or ".runtime"
    return Path(root) / "energy_report_upload_completed.json"


def completed(config: dict[str, Any], period: str) -> bool:
    for path in (_completion_path(config, period), _state_path(config)):
        saved = load_cached_json(path, {}, encoding="utf-8")
        if isinstance(saved, dict) and saved.get("period") == period and saved.get("table_id") == TABLE_ID:
            return True
    return False


def _completion_path(config, period):
    return _state_path(config).with_name(f"energy_report_upload_completed_{period}.json")


def _queue_path(config):
    return _state_path(config).with_name("energy_report_upload_queue.json")


def _validate_period(period: str) -> None:
    if datetime.strptime(period, "%Y-%m").strftime("%Y-%m") != period:
        raise ValueError("月度能耗报表月份必须为 YYYY-MM")


def pending_periods(config, now: datetime) -> tuple[str, ...]:
    with _QUEUE_LOCK:
        path = _queue_path(config)
        state = load_cached_json(path, {}, encoding="utf-8")
        pending = set(state.get("pending", []))
        last = state.get("last_enrolled", "")
        current = due_period(now)
        if current and current > last:
            # Start at the first observed period; keep tracking every missed month thereafter.
            cursor = current if not last else (datetime.strptime(last, "%Y-%m") + timedelta(days=32)).strftime("%Y-%m")
            while cursor <= current:
                pending.add(cursor)
                cursor = (datetime.strptime(cursor, "%Y-%m") + timedelta(days=32)).strftime("%Y-%m")
            last = current
        if not state:
            for journal in path.parent.glob("energy_report_upload_pending_????-??.json"):
                period = journal.stem.removeprefix("energy_report_upload_pending_")
                try:
                    _validate_period(period)
                except ValueError:
                    continue
                if period < now.strftime("%Y-%m"):
                    pending.add(period)
        pending = sorted(period for period in pending if not completed(config, period))
        updated = {"last_enrolled": last, "pending": pending}
        if updated != state:
            save_cached_json(path, updated, encoding="utf-8")
        return tuple(pending)


def enqueue_period(config, period: str, now: datetime) -> bool:
    _validate_period(period)
    if period >= now.strftime("%Y-%m"):
        raise ValueError("仅可补传已结束的月份")
    with _QUEUE_LOCK:
        if completed(config, period):
            return False
        path = _queue_path(config)
        state = load_cached_json(path, {}, encoding="utf-8")
        state["pending"] = sorted(set(state.get("pending", [])) | {period})
        save_cached_json(path, state, encoding="utf-8")
        return True


def _text(value: Any) -> str:
    if isinstance(value, list):
        return "".join(_text(item) for item in value).strip()
    if isinstance(value, dict):
        return _text(value.get("text", ""))
    return str(value or "").strip()


class EnergyReportUploadService:
    def __init__(self, config: dict[str, Any], *, internal=None, bitable=None) -> None:
        self.config = config
        self.internal = internal
        self.bitable = bitable
        self.primary_field = ""

    def client(self) -> FeishuBitableClient:
        if self.bitable is None:
            auth = require_feishu_auth_settings(self.config)
            target = BitableTargetResolver(**auth).resolve_token_pair_preview(
                configured_app_token=TARGET_TOKEN, table_id=TABLE_ID,
            )
            app_token = target.get("operation_app_token")
            if not app_token:
                raise RuntimeError(f"月度能耗报表目标不可用: {target.get('message') or TABLE_ID}")
            self.bitable = FeishuBitableClient(
                **auth, app_token=app_token, calc_table_id=TABLE_ID, attachment_table_id=TABLE_ID,
                date_text_to_timestamp_ms_fn=lambda **_: 0,
                canonical_metric_name_fn=str, dimension_mapping={},
            )
        return self.bitable

    def ensure_schema(self) -> dict[str, int]:
        client = self.client()
        listed = client.list_fields(TABLE_ID)
        fields = {row["field_name"]: row["type"] for row in listed}
        self.primary_field = next(
            (row["field_name"] for row in listed
             if row.get("is_primary") and row.get("type") == 1 and row["field_name"] not in FIELD_TYPES),
            "",
        )
        conflicts = [name for name, kind in FIELD_TYPES.items() if name in fields and fields[name] != kind]
        if conflicts:
            raise ValueError(f"月度能耗报表字段类型不符，未修改已有字段: {', '.join(conflicts)}")
        url = client.LIST_FIELD_URL.format(app_token=client.app_token, table_id=TABLE_ID)
        missing = {name: kind for name, kind in FIELD_TYPES.items() if name not in fields}
        for name, kind in missing.items():
            client._post_json(url, {"field_name": name, "type": kind})
        if missing:
            fields = {row["field_name"]: row["type"] for row in client.list_fields(TABLE_ID)}
            if any(fields.get(name) != kind for name, kind in FIELD_TYPES.items()):
                raise RuntimeError("月度能耗报表字段创建后复核失败")
        return missing

    def _bridge(self):
        bridge = self.internal or InternalBridgeHttpClient.from_runtime_config(self.config)
        if bridge is None:
            raise RuntimeError("月度能耗报表需要配置内网 HTTP 桥接")
        return bridge

    @staticmethod
    def _request_repairs(bridge, period, pairs, *, force, log):
        for family in ATTACHMENTS:
            buildings = [b for b in BUILDINGS if (b, family) in pairs]
            if not buildings:
                continue
            try:
                result = bridge.refresh_latest_source_cache(
                    source_family=family, buildings=buildings, target_bucket_key=period,
                    **({"force_refresh": True} if force else {}),
                )
                log(f"[月度能耗报表上传] 已请求内网补采 period={period}, family={family}, buildings={','.join(buildings)}, force={force}, accepted={result.get('accepted', 0)}")
            except RuntimeError as exc:
                log(f"[月度能耗报表上传] 内网补采请求失败，后续重试: {exc}")

    def source_paths(self, period: str, *, request_missing: bool = False, log=lambda _: None) -> dict[tuple[str, str], Path]:
        _validate_period(period)
        bridge = self._bridge()
        required = [(building, family) for building in BUILDINGS for family in ATTACHMENTS]
        replies = bridge.source_index_batch([
            {"source_family": family, "building": building, "bucket_kind": "latest",
             "bucket_or_date": period, "status": "ready", "limit": 5}
            for building, family in required
        ])
        config = self.config.get("shared_bridge", {})
        root = config.get("external_root_dir") or config.get("root_dir")
        found = {}
        invalid = set()
        for key, reply in zip(required, replies):
            building, family = key
            for entry in reply.get("entries", []) if reply.get("ok") else []:
                if (entry.get("source_family"), entry.get("building"), entry.get("bucket_key"),
                        entry.get("bucket_kind"), entry.get("status")) != (family, building, period, "latest", "ready"):
                    continue
                if entry.get("file_verified") is False:
                    continue
                relative = PurePosixPath(str(entry.get("relative_path") or "").replace("\\", "/"))
                if root and relative.parts:
                    if relative.is_absolute() or ".." in relative.parts or ":" in str(relative):
                        continue
                    path = Path(root).joinpath(*relative.parts)
                else:
                    path = Path(str(entry.get("file_path") or ""))
                try:
                    if path.suffix.lower() == ".xlsx" and path.is_file():
                        if request_missing:
                            try:
                                validate_xlsx_archive(path)
                            except Exception as exc:
                                invalid.add(key)
                                log(f"[月度能耗报表上传] 工作簿校验失败: {building}/{ATTACHMENTS[family]}: {exc}")
                                continue
                        found[key] = path
                        invalid.discard(key)
                        break
                except OSError:
                    continue
        missing = [f"{b}/{ATTACHMENTS[f]}" for b, f in required if (b, f) not in found]
        if missing:
            if request_missing:
                self._request_repairs(bridge, period, set(required) - found.keys() - invalid, force=False, log=log)
                self._request_repairs(bridge, period, invalid, force=True, log=log)
            raise FileNotFoundError(f"{period} 月度能耗报表未齐: {'、'.join(missing)}")
        if len(set(found.values())) != len(required):
            raise ValueError("月度能耗报表索引重复指向同一文件")
        return found

    @staticmethod
    def _month_records(client, period: str) -> dict:
        records = client.list_records(TABLE_ID, field_names=list(FIELD_TYPES))
        existing = {}
        for row in records:
            fields = row.get("fields", {})
            building = _text(fields.get("楼栋"))
            if _text(fields.get("月份")) != period or building not in BUILDINGS:
                continue
            if building in existing:
                raise ValueError(f"月度能耗报表存在重复记录: {period}/{building}，请人工核对")
            if not _text(row.get("record_id")):
                raise ValueError(f"月度能耗报表记录缺少ID: {period}/{building}")
            existing[building] = row
        return existing

    @staticmethod
    def _matches(row: dict | None, digest: str) -> bool:
        fields = row["fields"] if row else {}
        return _text(fields.get("同步标识")) == digest and all(fields.get(name) for name in ATTACHMENTS.values())

    def run(self, period: str, *, log, check_cancel=lambda: None) -> dict[str, Any]:
        paths = self.source_paths(period)
        files = {}
        for key, path in paths.items():
            check_cancel()
            try:
                content = path.read_bytes()
                validate_xlsx_archive(BytesIO(content))
            except Exception as exc:
                self._request_repairs(self._bridge(), period, {key}, force=True, log=log)
                raise ValueError(f"月度能耗报表文件无效: {path.name}，未写入飞书") from exc
            files[key] = (path.name, content)
        check_cancel()
        self.ensure_schema()
        client = self.client()
        existing = self._month_records(client, period)
        pending_path = _state_path(self.config).with_name(f"energy_report_upload_pending_{period}.json")
        try:
            pending = json.loads(pending_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            pending = {}
        digests = {}
        changed = 0
        for building in BUILDINGS:
            check_cancel()
            digest = ":".join(hashlib.sha256(files[building, family][1]).hexdigest() for family in ATTACHMENTS)
            digests[building] = digest
            row = existing.get(building)
            if self._matches(row, digest):
                continue
            if not row and building in pending:
                request = pending[building]
                client.create_record(TABLE_ID, request["fields"], client_token=request["client_token"])
                row = self._month_records(client, period).get(building)
                if row is None:
                    raise RuntimeError(f"{period} 月度能耗报表回查未确认: {building}")
                if self._matches(row, digest):
                    changed += 1
                    continue
            fields = {"月份": period, "楼栋": building, "同步标识": digest}
            for family, field in ATTACHMENTS.items():
                check_cancel()
                filename, content = files[building, family]
                token = client.upload_attachment_bytes(filename, content, MIME_XLSX)
                fields[field] = [{"file_token": token}]
            check_cancel()
            if row:
                client.update_record(TABLE_ID, row["record_id"], fields)
            else:
                if self.primary_field:
                    fields[self.primary_field] = f"{period} {building}"
                client_token = str(uuid.uuid4())
                pending[building] = {"fields": fields, "client_token": client_token}
                save_cached_json(pending_path, pending, encoding="utf-8")
                client.create_record(TABLE_ID, fields, client_token=client_token)
            changed += 1
            log(f"[月度能耗报表上传] 已写入 {period}/{building}")
        if changed:
            confirmed = self._month_records(client, period)
            missing = [building for building in BUILDINGS if not self._matches(confirmed.get(building), digests[building])]
            if missing:
                raise RuntimeError(f"{period} 月度能耗报表回查未确认: {'、'.join(missing)}")
        save_cached_json(_completion_path(self.config, period), {"period": period, "table_id": TABLE_ID}, encoding="utf-8")
        pending_path.unlink(missing_ok=True)
        log(f"[月度能耗报表上传] 完成 period={period}, 更新={changed}, 复用={len(BUILDINGS) - changed}")
        return {"status": "success", "period": period, "updated_buildings": changed}
