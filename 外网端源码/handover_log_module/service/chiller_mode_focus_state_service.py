from __future__ import annotations

import hashlib
import json
import re
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List

from app.core.app_state import AppStateRepository
from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.bitable_target_resolver import BitableTargetResolver
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms
from pipeline_utils import get_app_dir


CHILLER_FOCUS_STATE_NAMESPACE = "station_h_chiller_focus"
CHILLER_FOCUS_LATEST_KEY = "latest"
CHILLER_FOCUS_BUILDINGS = ("A楼", "B楼", "C楼", "D楼", "E楼")
CHILLER_FOCUS_MODE_ALIASES = ("冷机状态", "冷机模式", "冷机运行模式")
CHILLER_FOCUS_MODE_MAP = {"1": "制冷", "2": "预冷", "3": "板换", "4": "停机"}
CHILLER_FOCUS_RUNNING_MODES = frozenset({"制冷", "预冷", "板换"})
_CHILLER_FOCUS_ZONES = {"west": (1, 2, 3), "east": (4, 5, 6)}
_REMOTE_REFRESH_LOCK = threading.Lock()


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value).strip()
    if isinstance(value, list):
        return "、".join(item for item in (_text(item) for item in value) if item)
    if isinstance(value, dict):
        for key in ("text", "name", "value", "display_name"):
            result = _text(value.get(key))
            if result:
                return result
        return "、".join(item for item in (_text(item) for item in value.values()) if item)
    return str(value).strip()


def _runtime_config(config: Dict[str, Any]) -> Dict[str, Any]:
    payload = config if isinstance(config, dict) else {}
    paths = payload.get("paths", {}) if isinstance(payload.get("paths", {}), dict) else {}
    global_paths = payload.get("_global_paths", {}) if isinstance(payload.get("_global_paths", {}), dict) else {}
    runtime_state_root = _text(paths.get("runtime_state_root")) or _text(global_paths.get("runtime_state_root"))
    return {"paths": {"runtime_state_root": runtime_state_root}} if runtime_state_root else {}


def _parse_datetime(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    for candidate in (text, text.replace("T", " ")):
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            continue
    return None


def _mode(value: Any, *, mode_map: Dict[str, str] | None = None) -> str:
    text = _text(value)
    if not text:
        return ""
    aliases = {
        "制冷": "制冷",
        "预冷": "预冷",
        "板换": "板换",
        "停机": "停机",
        "停止": "停机",
        "未启动": "停机",
        "△": "停机",
        "▲": "停机",
    }
    if text in aliases:
        return aliases[text]
    try:
        number = float(text)
        code = str(int(number)) if number.is_integer() else str(number)
    except ValueError:
        code = text
    return _text((mode_map or CHILLER_FOCUS_MODE_MAP).get(code))


def _building(value: Any) -> str:
    text = _text(value).upper()
    for candidate in CHILLER_FOCUS_BUILDINGS:
        if candidate.upper() in text:
            return candidate
    match = re.search(r"(?<![A-Z])([A-E])(?![A-Z])", text)
    return f"{match.group(1)}楼" if match else ""


def _unit_from_point(value: Any) -> int:
    point = re.sub(r"\s+", "", _text(value))
    if not point or "模式" not in point or not ("冷机" in point or "制冷单元" in point):
        return 0
    match = re.search(r"(?<!\d)([1-6])(?:号|#)?(?:冷机|制冷单元)", point)
    return int(match.group(1)) if match else 0


def _batch_key_for(moment: datetime) -> str:
    if moment.hour < 9:
        duty_date = (moment.date() - timedelta(days=1)).isoformat()
        shift = "night"
    elif moment.hour < 18:
        duty_date = moment.date().isoformat()
        shift = "day"
    else:
        duty_date = moment.date().isoformat()
        shift = "night"
    return f"{duty_date}|{shift}"


def _format_units(units: Iterable[int]) -> str:
    values = sorted({int(unit) for unit in units if 1 <= int(unit) <= 6})
    return ("、".join(str(unit) for unit in values) + "#") if values else ""


class ChillerModeFocusStateService:
    """Persist 10-minute chiller states and confirm same-shift rotation events."""

    DEFAULT_APP_TOKEN = "ASLxbfESPahdTKs0A9NccgbrnXc"
    DEFAULT_TABLE_ID = "tblkvVCNRbtMmjQg"

    def __init__(
        self,
        config: Dict[str, Any] | None = None,
        *,
        app_state_repository: AppStateRepository | None = None,
        app_dir: Path | None = None,
        client_factory: Callable[[], FeishuBitableClient] | None = None,
        emit_log: Callable[[str], None] | None = None,
    ) -> None:
        self.config = config if isinstance(config, dict) else {}
        self._repository = app_state_repository or self._build_repository(app_dir=app_dir)
        self._client_factory = client_factory
        self._emit_log = emit_log

    def _build_repository(self, *, app_dir: Path | None) -> AppStateRepository | None:
        try:
            repository = AppStateRepository(
                runtime_config=_runtime_config(self.config),
                app_dir=app_dir or get_app_dir(),
            )
            repository.ensure_ready()
            return repository
        except Exception:
            return None

    def _log(self, message: str) -> None:
        if callable(self._emit_log):
            self._emit_log(message)

    def _chiller_config(self) -> Dict[str, Any]:
        direct = self.config.get("chiller_mode_upload", {})
        if isinstance(direct, dict) and direct:
            return direct
        inherited = self.config.get("_global_chiller_mode_upload", {})
        return inherited if isinstance(inherited, dict) else {}

    def _target_config(self) -> tuple[str, str, Dict[str, str], Dict[str, str]]:
        cfg = self._chiller_config()
        target = cfg.get("target", {}) if isinstance(cfg.get("target", {}), dict) else {}
        fields = cfg.get("fields", {}) if isinstance(cfg.get("fields", {}), dict) else {}
        raw_map = cfg.get("mode_value_map", {}) if isinstance(cfg.get("mode_value_map", {}), dict) else {}
        mode_map = dict(CHILLER_FOCUS_MODE_MAP)
        mode_map.update({_text(key): _text(value) for key, value in raw_map.items() if _text(key) and _text(value)})
        normalized_fields = {
            "building": _text(fields.get("building")) or "楼栋",
            "point": _text(fields.get("point")) or "采集点",
            "value": _text(fields.get("value")) or "数据",
            "chiller_mode": _text(fields.get("chiller_mode")) or "冷机模式",
        }
        return (
            _text(target.get("app_token")) or self.DEFAULT_APP_TOKEN,
            _text(target.get("table_id")) or self.DEFAULT_TABLE_ID,
            normalized_fields,
            mode_map,
        )

    @staticmethod
    def extract_modes(
        rows: Iterable[Dict[str, Any]],
        *,
        fields: Dict[str, str] | None = None,
        mode_map: Dict[str, str] | None = None,
    ) -> Dict[str, Dict[str, str]]:
        names = fields if isinstance(fields, dict) else {}
        building_field = _text(names.get("building")) or "楼栋"
        point_field = _text(names.get("point")) or "采集点"
        value_field = _text(names.get("value")) or "数据"
        configured_mode_field = _text(names.get("chiller_mode")) or "冷机模式"
        mode_fields: List[str] = []
        for name in (configured_mode_field, *CHILLER_FOCUS_MODE_ALIASES):
            if name and name not in mode_fields:
                mode_fields.append(name)

        output: Dict[str, Dict[str, str]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            payload = row.get("fields", row)
            if not isinstance(payload, dict):
                continue
            building = _building(payload.get(building_field))
            unit = _unit_from_point(payload.get(point_field))
            if not building or not unit:
                continue
            mode_text = ""
            for field_name in mode_fields:
                mode_text = _mode(payload.get(field_name), mode_map=mode_map)
                if mode_text:
                    break
            if not mode_text:
                mode_text = _mode(payload.get(value_field), mode_map=mode_map)
            if mode_text:
                output.setdefault(building, {})[str(unit)] = mode_text
        return output

    @staticmethod
    def _normalized_modes(value: Any) -> Dict[str, str]:
        payload = value if isinstance(value, dict) else {}
        return {
            str(unit): _mode(payload.get(str(unit)))
            for unit in range(1, 7)
            if _mode(payload.get(str(unit)))
        }

    @staticmethod
    def _running(modes: Dict[str, str], zone_units: Iterable[int]) -> List[int]:
        return [unit for unit in zone_units if _mode(modes.get(str(unit))) in CHILLER_FOCUS_RUNNING_MODES]

    @staticmethod
    def _pending(value: Any) -> Dict[str, Any]:
        payload = value if isinstance(value, dict) else {}
        origin = sorted({int(unit) for unit in payload.get("origin", []) if str(unit).isdigit()})
        started = sorted({int(unit) for unit in payload.get("started", []) if str(unit).isdigit()})
        if not origin or not started:
            return {}
        return {
            "origin": origin,
            "started": started,
            "started_at": _text(payload.get("started_at")),
        }

    @classmethod
    def _advance_zone(
        cls,
        *,
        previous: List[int],
        current: List[int],
        pending_raw: Any,
        observed_at: str,
        zone: str,
    ) -> tuple[Dict[str, Any], Dict[str, Any] | None]:
        previous_set = set(previous)
        current_set = set(current)
        newly_started = sorted(current_set - previous_set)
        pending = cls._pending(pending_raw)

        if pending:
            active_started = sorted(set(pending["started"]) & current_set)
            if not active_started:
                pending = {}
            else:
                pending["started"] = sorted(set(active_started) | set(newly_started))

        if not pending and newly_started and previous:
            pending = {
                "origin": sorted(previous_set),
                "started": newly_started,
                "started_at": observed_at,
            }

        if not pending:
            return {}, None

        active_started = sorted(set(pending["started"]) & current_set)
        if not active_started:
            return {}, None
        stopped_origin = sorted(set(pending["origin"]) - current_set)
        if not stopped_origin:
            pending["started"] = active_started
            return pending, None

        left = _format_units(stopped_origin)
        right = _format_units(active_started)
        if not left or not right:
            return {}, None
        return {}, {
            "zone": zone,
            "origin": stopped_origin,
            "started": active_started,
            "note": f"{left}→{right}",
            "observed_at": observed_at,
        }

    @staticmethod
    def _revision(buildings: Dict[str, Any]) -> str:
        payload: Dict[str, Any] = {}
        for building in CHILLER_FOCUS_BUILDINGS:
            current = buildings.get(building, {}) if isinstance(buildings, dict) else {}
            if not isinstance(current, dict):
                continue
            payload[building] = {
                "modes": current.get("modes", {}),
                "change_note": _text(current.get("change_note")) or "无",
            }
        if not payload:
            return ""
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def get_latest(self) -> Dict[str, Any]:
        if self._repository is None:
            return {}
        try:
            payload = self._repository.get_runtime_kv(
                CHILLER_FOCUS_STATE_NAMESPACE,
                CHILLER_FOCUS_LATEST_KEY,
            )
        except Exception:
            return {}
        return dict(payload) if isinstance(payload, dict) else {}

    def get_batch_state(self, batch_key: str) -> Dict[str, Any]:
        key = _text(batch_key)
        if not key or self._repository is None:
            return {}
        try:
            payload = self._repository.get_runtime_kv(CHILLER_FOCUS_STATE_NAMESPACE, key)
        except Exception:
            return {}
        return dict(payload) if isinstance(payload, dict) else {}

    def record_modes(
        self,
        modes_by_building: Dict[str, Dict[str, str]],
        *,
        observed_at: str = "",
        source: str = "chiller_mode_upload",
    ) -> Dict[str, Any]:
        moment = _parse_datetime(observed_at) or datetime.now()
        observed_text = moment.strftime("%Y-%m-%d %H:%M:%S")
        batch_key = _batch_key_for(moment)
        existing = self.get_batch_state(batch_key)
        previous_observed = _parse_datetime(existing.get("observed_at"))
        if previous_observed and moment < previous_observed:
            return existing

        batch_buildings = dict(existing.get("buildings", {})) if isinstance(existing.get("buildings", {}), dict) else {}
        accepted: List[str] = []
        incomplete: List[str] = []
        for building in CHILLER_FOCUS_BUILDINGS:
            incoming = self._normalized_modes(modes_by_building.get(building, {}))
            if set(incoming) != {str(unit) for unit in range(1, 7)}:
                if incoming:
                    incomplete.append(building)
                continue
            accepted.append(building)
            previous_state = batch_buildings.get(building, {})
            previous_state = previous_state if isinstance(previous_state, dict) else {}
            previous_modes = self._normalized_modes(previous_state.get("modes", {}))
            events = [
                dict(item)
                for item in previous_state.get("switch_events", [])
                if isinstance(item, dict) and _text(item.get("note"))
            ][-20:]
            pending_by_zone = (
                dict(previous_state.get("pending_by_zone", {}))
                if isinstance(previous_state.get("pending_by_zone", {}), dict)
                else {}
            )

            if previous_modes:
                for zone, zone_units in _CHILLER_FOCUS_ZONES.items():
                    pending, event = self._advance_zone(
                        previous=self._running(previous_modes, zone_units),
                        current=self._running(incoming, zone_units),
                        pending_raw=pending_by_zone.get(zone),
                        observed_at=observed_text,
                        zone=zone,
                    )
                    if pending:
                        pending_by_zone[zone] = pending
                    else:
                        pending_by_zone.pop(zone, None)
                    if event:
                        events.append(event)
            else:
                pending_by_zone = {}

            notes = [_text(item.get("note")) for item in events if _text(item.get("note"))]
            batch_buildings[building] = {
                "modes": incoming,
                "change_note": "、".join(notes) if notes else "无",
                "switch_events": events[-20:],
                "pending_by_zone": pending_by_zone,
                "observed_at": observed_text,
            }

        payload = {
            "batch_key": batch_key,
            "observed_at": observed_text,
            "source": _text(source) or "chiller_mode_upload",
            "buildings": batch_buildings,
            "accepted_buildings": accepted,
            "incomplete_buildings": incomplete,
        }
        payload["mode_revision"] = self._revision(batch_buildings)
        if self._repository is not None:
            self._repository.put_runtime_kv(CHILLER_FOCUS_STATE_NAMESPACE, batch_key, payload)
            self._repository.put_runtime_kv(
                CHILLER_FOCUS_STATE_NAMESPACE,
                CHILLER_FOCUS_LATEST_KEY,
                payload,
            )
        return payload

    def record_rows(
        self,
        rows: Iterable[Dict[str, Any]],
        *,
        observed_at: str = "",
        source: str = "chiller_mode_upload",
        fields: Dict[str, str] | None = None,
        mode_map: Dict[str, str] | None = None,
    ) -> Dict[str, Any]:
        modes = self.extract_modes(rows, fields=fields, mode_map=mode_map)
        return self.record_modes(modes, observed_at=observed_at, source=source)

    def _new_target_client(self) -> tuple[FeishuBitableClient, str, Dict[str, str], Dict[str, str]]:
        if callable(self._client_factory):
            client = self._client_factory()
            _app_token, table_id, fields, mode_map = self._target_config()
            return client, table_id, fields, mode_map
        configured_app_token, table_id, fields, mode_map = self._target_config()
        auth = require_feishu_auth_settings(self.config)
        resolver = BitableTargetResolver(
            app_id=_text(auth.get("app_id")),
            app_secret=_text(auth.get("app_secret")),
            timeout=int(auth.get("timeout", 30) or 30),
            request_retry_count=int(auth.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(auth.get("request_retry_interval_sec", 2) or 2),
        )
        resolved = resolver.resolve_token_pair_preview(
            configured_app_token=configured_app_token,
            table_id=table_id,
            force_refresh=False,
        )
        operation_app_token = _text(resolved.get("operation_app_token"))
        resolved_table_id = _text(resolved.get("table_id")) or table_id
        if not operation_app_token or not resolved_table_id:
            raise ValueError(_text(resolved.get("message")) or "制冷模式目标多维表不可用")
        client = FeishuBitableClient(
            app_id=_text(auth.get("app_id")),
            app_secret=_text(auth.get("app_secret")),
            app_token=operation_app_token,
            calc_table_id=resolved_table_id,
            attachment_table_id=resolved_table_id,
            timeout=int(auth.get("timeout", 30) or 30),
            request_retry_count=int(auth.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(auth.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=date_text_to_timestamp_ms,
            canonical_metric_name_fn=lambda value: _text(value),
            dimension_mapping={},
            emit_log=self._emit_log,
        )
        return client, resolved_table_id, fields, mode_map

    def refresh_from_target(self) -> Dict[str, Any]:
        client, table_id, fields, mode_map = self._new_target_client()
        field_defs = client.list_fields(table_id=table_id, page_size=500)
        existing = {
            _text(item.get("field_name") or item.get("name"))
            for item in field_defs
            if isinstance(item, dict)
        }
        mode_candidates = [fields["chiller_mode"], *CHILLER_FOCUS_MODE_ALIASES]
        mode_field = next((name for name in mode_candidates if name in existing), "")
        field_names = [fields["building"], fields["point"], fields["value"]]
        if mode_field:
            field_names.append(mode_field)
        records = client.list_records(
            table_id=table_id,
            page_size=100,
            max_records=100,
            filter_formula=f'CurrentValue.[{fields["point"]}].contains("模式")',
            field_names=[name for name in field_names if name in existing],
        )
        if mode_field and mode_field != fields["chiller_mode"]:
            fields = dict(fields)
            fields["chiller_mode"] = mode_field
        result = self.record_rows(
            records,
            observed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            source="feishu_chiller_mode_target",
            fields=fields,
            mode_map=mode_map,
        )
        self._log(
            f"[交接班][值班关注点] 制冷模式快照已刷新: records={len(records)}, "
            f"batch={result.get('batch_key', '-')}, buildings={','.join(result.get('accepted_buildings', [])) or '-'}"
        )
        return result

    def ensure_batch_state(self, batch_key: str, *, max_age_minutes: int = 20) -> Dict[str, Any]:
        requested = _text(batch_key)
        cached = self.get_batch_state(requested)
        if requested != _batch_key_for(datetime.now()):
            return cached
        observed = _parse_datetime(cached.get("observed_at"))
        if observed and datetime.now() - observed <= timedelta(minutes=max(1, int(max_age_minutes or 20))):
            return cached
        with _REMOTE_REFRESH_LOCK:
            cached = self.get_batch_state(requested)
            observed = _parse_datetime(cached.get("observed_at"))
            if observed and datetime.now() - observed <= timedelta(minutes=max(1, int(max_age_minutes or 20))):
                return cached
            try:
                refreshed = self.refresh_from_target()
            except Exception as exc:
                if cached:
                    cached = dict(cached)
                    cached["refresh_error"] = str(exc)
                    return cached
                return {"batch_key": requested, "buildings": {}, "refresh_error": str(exc)}
            return refreshed if _text(refreshed.get("batch_key")) == requested else cached


__all__ = [
    "CHILLER_FOCUS_BUILDINGS",
    "CHILLER_FOCUS_STATE_NAMESPACE",
    "ChillerModeFocusStateService",
]
