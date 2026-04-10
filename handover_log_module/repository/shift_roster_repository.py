from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Tuple

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.bitable_target_resolver import BitableTargetResolver
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms
from handover_log_module.core.shift_window import normalize_duty_shift, parse_duty_date
from handover_log_module.repository.event_followup_cache_store import EventFollowupCacheStore


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base or {})
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _extract_building_code(text: Any) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    by_cn = re.search(r"([A-Za-z])\s*[楼棟栋]", raw)
    if by_cn:
        return by_cn.group(1).upper()
    first = re.search(r"[A-Za-z]", raw)
    return first.group(0).upper() if first else ""


def _normalize_building_text(text: Any) -> str:
    return str(text or "").strip().replace(" ", "").casefold()


def _first_non_empty(items: Iterable[str]) -> str:
    for item in items:
        text = str(item or "").strip()
        if text:
            return text
    return ""


def _field_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return _first_non_empty(
            [
                str(value.get("text", "")),
                str(value.get("name", "")),
                str(value.get("value", "")),
                str(value.get("label", "")),
            ]
        )
    if isinstance(value, list):
        parts = [_field_text(item) for item in value]
        return "、".join([x for x in parts if x])
    return str(value).strip()


def _extract_contact_identity(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        for key in (
            "user_id",
            "userId",
            "open_id",
            "openId",
            "id",
            "union_id",
            "unionId",
            "email",
            "mail",
            "mobile",
            "phone",
        ):
            text = str(value.get(key, "") or "").strip()
            if text:
                return text
        for key in ("id", "value"):
            nested = value.get(key)
            if isinstance(nested, dict):
                resolved = _extract_contact_identity(nested)
                if resolved:
                    return resolved
        for nested in value.values():
            resolved = _extract_contact_identity(nested)
            if resolved:
                return resolved
        return ""
    if isinstance(value, list):
        for item in value:
            resolved = _extract_contact_identity(item)
            if resolved:
                return resolved
        return ""
    return ""


def _normalize_date_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        for key in ("value", "text", "name", "label"):
            if key in value:
                resolved = _normalize_date_text(value.get(key))
                if resolved:
                    return resolved
        return ""
    if isinstance(value, list):
        for item in value:
            resolved = _normalize_date_text(item)
            if resolved:
                return resolved
        return ""
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        num = int(float(value))
        if abs(num) >= 10**12:
            return datetime.fromtimestamp(num / 1000).strftime("%Y-%m-%d")
        return datetime.fromtimestamp(num).strftime("%Y-%m-%d")

    text = str(value).strip()
    if not text:
        return ""
    if text.isdigit():
        num = int(text)
        if len(text) >= 13 or abs(num) >= 10**12:
            return datetime.fromtimestamp(num / 1000).strftime("%Y-%m-%d")
        return datetime.fromtimestamp(num).strftime("%Y-%m-%d")
    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(text[:19], fmt).strftime("%Y-%m-%d")
        except Exception:  # noqa: BLE001
            continue
    return ""


@dataclass(frozen=True)
class ShiftRosterAssignment:
    current_team: str
    current_people: str
    next_team: str
    next_people: str
    next_first_person: str
    source_records: int


class ShiftRosterRepository:
    PREFERRED_PEOPLE_TEXT_FIELD = "值班人员（实际）"
    LEGACY_PEOPLE_TEXT_FIELD = "人员（文本）"

    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg
        self._cache_store: EventFollowupCacheStore | None = None
        self._memory_records_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._fetched_empty_cache_keys: set[str] = set()

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "source": {
                "app_token": "G7oUwGdwaiTmimk8i2ecGTWOn4d",
                "table_id": "tblRV9KeWFh9xCkm",
                "page_size": 500,
                "max_records": 5000,
            },
            "fields": {
                "duty_date": "排班日期",
                "building": "机楼",
                "team": "班组",
                "shift": "班次",
                "people_text": "值班人员（实际）",
            },
            "cells": {
                "current_people": "C3",
                "next_people": "G3",
                "next_first_person_cells": ["H52", "H53", "H54", "H55"],
            },
            "match": {
                "building_mode": "exact_then_code",
            },
            "shift_alias": {
                "day": ["白班", "day", "DAY"],
                "night": ["夜班", "night", "NIGHT"],
            },
            "people_split_regex": r"[、,/，；;\s]+",
            "long_day": {
                "enabled": True,
                "source": {
                    "app_token": "",
                    "table_id": "tblyyU7BbO4vB1oO",
                    "page_size": 500,
                    "max_records": 5000,
                },
                "fields": {
                    "duty_date": "排班日期",
                    "building": "机楼",
                    "shift": "班次",
                    "people_text": "值班人员（实际）",
                },
                "shift_value": "长白",
                "day_cell": "B4",
                "night_cell": "F4",
                "prefix": "长白岗：",
                "rest_text": "/",
                "match": {
                    "building_mode": "exact_then_code",
                },
            },
            "engineer_directory": {
                "enabled": True,
                "source": {
                    "app_token": "",
                    "table_id": "tblZsQ6UmLdg9a2m",
                    "page_size": 500,
                    "max_records": 5000,
                },
                "fields": {
                    "building": "楼栋/专业",
                    "specialty": "专业",
                    "supervisor_text": "主管（文本）",
                    "supervisor_person": "主管",
                    "position": "职位",
                    "recipient_id": "飞书用户ID",
                },
                "delivery": {
                    "receive_id_type": "user_id",
                    "position_keyword": "设施运维主管",
                },
                "match": {
                    "building_mode": "exact_then_code",
                },
            },
        }

    @classmethod
    def _normalize_people_field_name(cls, field_name: Any) -> str:
        text = str(field_name or "").strip()
        if not text or text == cls.LEGACY_PEOPLE_TEXT_FIELD:
            return cls.PREFERRED_PEOPLE_TEXT_FIELD
        return text

    @classmethod
    def _people_field_candidates(cls, field_name: Any) -> List[str]:
        configured = cls._normalize_people_field_name(field_name)
        candidates: List[str] = []
        if configured and configured != cls.LEGACY_PEOPLE_TEXT_FIELD:
            candidates.append(configured)
        if cls.PREFERRED_PEOPLE_TEXT_FIELD not in candidates:
            candidates.append(cls.PREFERRED_PEOPLE_TEXT_FIELD)
        if configured and configured not in candidates:
            candidates.append(configured)
        if cls.LEGACY_PEOPLE_TEXT_FIELD not in candidates:
            candidates.append(cls.LEGACY_PEOPLE_TEXT_FIELD)
        return [name for name in candidates if name]

    def _people_text_from_fields(self, *, fields: Dict[str, Any], configured_field: Any) -> str:
        for field_name in self._people_field_candidates(configured_field):
            text = _field_text(fields.get(field_name)).strip()
            if text:
                return text
        return ""

    def _normalize_cfg(self) -> Dict[str, Any]:
        raw = self.handover_cfg.get("shift_roster", {})
        cfg = _deep_merge(self._defaults(), raw if isinstance(raw, dict) else {})
        source = cfg.get("source", {})
        source["app_token"] = str(source.get("app_token", "")).strip()
        source["table_id"] = str(source.get("table_id", "")).strip()
        source["page_size"] = max(1, int(source.get("page_size", 500)))
        source["max_records"] = max(1, int(source.get("max_records", 5000)))
        cfg["source"] = source
        cfg["people_split_regex"] = str(cfg.get("people_split_regex", "")).strip() or r"[、,/，；;\s]+"
        fields_cfg = cfg.get("fields", {})
        if not isinstance(fields_cfg, dict):
            fields_cfg = {}
        fields_cfg["people_text"] = self._normalize_people_field_name(
            fields_cfg.get("people_text", self.PREFERRED_PEOPLE_TEXT_FIELD)
        )
        cfg["fields"] = fields_cfg
        long_day_cfg = cfg.get("long_day", {})
        if not isinstance(long_day_cfg, dict):
            long_day_cfg = {}
        long_day_fields_cfg = long_day_cfg.get("fields", {})
        if not isinstance(long_day_fields_cfg, dict):
            long_day_fields_cfg = {}
        long_day_fields_cfg["people_text"] = self._normalize_people_field_name(
            long_day_fields_cfg.get("people_text", self.PREFERRED_PEOPLE_TEXT_FIELD)
        )
        long_day_cfg["fields"] = long_day_fields_cfg
        cfg["long_day"] = long_day_cfg
        return cfg

    def _new_client(self, cfg: Dict[str, Any]) -> FeishuBitableClient:
        global_feishu = self.handover_cfg.get("_global_feishu", {})
        if not isinstance(global_feishu, dict):
            global_feishu = {}
        app_id = str(global_feishu.get("app_id", "")).strip()
        app_secret = str(global_feishu.get("app_secret", "")).strip()
        if not app_id or not app_secret:
            raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")

        source = cfg.get("source", {})
        app_token = str(source.get("app_token", "")).strip()
        table_id = str(source.get("table_id", "")).strip()
        if not app_token or not table_id:
            raise ValueError("交接班排班多维配置缺失: app_token/table_id")

        return FeishuBitableClient(
            app_id=app_id,
            app_secret=app_secret,
            app_token=app_token,
            calc_table_id=table_id,
            attachment_table_id=table_id,
            timeout=int(global_feishu.get("timeout", 30) or 30),
            request_retry_count=int(global_feishu.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=date_text_to_timestamp_ms,
            canonical_metric_name_fn=lambda x: str(x or "").strip(),
            dimension_mapping={},
        )

    def _new_client_by_source(self, source_cfg: Dict[str, Any], fallback_cfg: Dict[str, Any]) -> FeishuBitableClient:
        global_feishu = self.handover_cfg.get("_global_feishu", {})
        if not isinstance(global_feishu, dict):
            global_feishu = {}
        app_id = str(global_feishu.get("app_id", "")).strip()
        app_secret = str(global_feishu.get("app_secret", "")).strip()
        if not app_id or not app_secret:
            raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")

        app_token = str(source_cfg.get("app_token", "")).strip()
        table_id = str(source_cfg.get("table_id", "")).strip()
        if not app_token:
            app_token = str(_deep_merge(self._defaults(), fallback_cfg).get("source", {}).get("app_token", "")).strip()
        if not app_token or not table_id:
            raise ValueError("交接班多维配置缺失: app_token/table_id")

        return FeishuBitableClient(
            app_id=app_id,
            app_secret=app_secret,
            app_token=app_token,
            calc_table_id=table_id,
            attachment_table_id=table_id,
            timeout=int(global_feishu.get("timeout", 30) or 30),
            request_retry_count=int(global_feishu.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=date_text_to_timestamp_ms,
            canonical_metric_name_fn=lambda x: str(x or "").strip(),
            dimension_mapping={},
        )

    def build_engineer_directory_target_descriptor(self, *, force_refresh: bool = False) -> Dict[str, str]:
        cfg = self._normalize_cfg()
        engineer_cfg = cfg.get("engineer_directory", {})
        if not isinstance(engineer_cfg, dict):
            engineer_cfg = {}
        source_cfg = engineer_cfg.get("source", {})
        if not isinstance(source_cfg, dict):
            source_cfg = {}
        fallback_source = cfg.get("source", {})
        if not isinstance(fallback_source, dict):
            fallback_source = {}

        configured_app_token = str(source_cfg.get("app_token", "")).strip() or str(
            fallback_source.get("app_token", "")
        ).strip()
        table_id = str(source_cfg.get("table_id", "")).strip()

        def _preview(target_kind: str, message: str = "") -> Dict[str, str]:
            return {
                "configured_app_token": configured_app_token,
                "operation_app_token": "",
                "app_token": "",
                "table_id": table_id,
                "target_kind": str(target_kind or "").strip(),
                "resolved_from": str(target_kind or "").strip(),
                "display_url": "",
                "bitable_url": "",
                "source_url": "",
                "wiki_node_token": "",
                "wiki_obj_type": "",
                "message": str(message or "").strip(),
                "resolved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

        if not bool(engineer_cfg.get("enabled", True)):
            return _preview("invalid", "工程师目录未启用")
        if not configured_app_token and not table_id:
            return _preview("invalid", "请先填写工程师目录多维 App Token 和 Table ID")
        if not configured_app_token:
            return _preview("invalid", "请先填写工程师目录多维 App Token")
        if not table_id:
            return _preview("invalid", "请先填写工程师目录多维 Table ID")

        global_feishu = self.handover_cfg.get("_global_feishu", {})
        if not isinstance(global_feishu, dict):
            global_feishu = {}

        try:
            resolver = BitableTargetResolver(
                app_id=str(global_feishu.get("app_id", "")).strip(),
                app_secret=str(global_feishu.get("app_secret", "")).strip(),
                timeout=int(global_feishu.get("timeout", 30) or 30),
                request_retry_count=int(global_feishu.get("request_retry_count", 3) or 3),
                request_retry_interval_sec=float(global_feishu.get("request_retry_interval_sec", 2) or 2),
            )
            return dict(
                resolver.resolve_token_pair_preview(
                    configured_app_token=configured_app_token,
                    table_id=table_id,
                    force_refresh=force_refresh,
                )
            )
        except Exception as exc:  # noqa: BLE001
            return _preview("probe_error", str(exc))

    @staticmethod
    def _build_shift_alias_map(cfg: Dict[str, Any]) -> Dict[str, str]:
        out: Dict[str, str] = {"day": "day", "night": "night"}
        alias_cfg = cfg.get("shift_alias", {})
        if not isinstance(alias_cfg, dict):
            return out
        for target in ("day", "night"):
            aliases = alias_cfg.get(target, [])
            if isinstance(aliases, list):
                for alias in aliases:
                    text = str(alias or "").strip()
                    if text:
                        out[text.casefold()] = target
        return out

    @staticmethod
    def _normalize_shift(value: Any, alias_map: Dict[str, str]) -> str:
        text = _field_text(value)
        if not text:
            return ""
        lowered = text.casefold()
        if lowered in alias_map:
            return alias_map[lowered]
        if "白" in lowered or "day" in lowered:
            return "day"
        if "夜" in lowered or "night" in lowered:
            return "night"
        return ""

    @staticmethod
    def _match_building(target_building: str, record_building: str, mode: str) -> bool:
        target_text = _normalize_building_text(target_building)
        record_text = _normalize_building_text(record_building)
        if target_text and record_text and target_text == record_text:
            return True
        if str(mode).strip().lower() != "exact_then_code":
            return False
        target_code = _extract_building_code(target_building)
        return bool(target_code and target_code == _extract_building_code(record_building))

    @staticmethod
    def _pick_best(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not rows:
            return {}
        with_people = [row for row in rows if str(row.get("people_text", "")).strip()]
        if with_people:
            return with_people[0]
        return rows[0]

    @staticmethod
    def _next_duty(duty_day: date, duty_shift: str) -> Tuple[date, str]:
        if duty_shift == "day":
            return duty_day, "night"
        return duty_day + timedelta(days=1), "day"

    @staticmethod
    def _infer_duty_by_now(now: datetime | None = None) -> tuple[str, str]:
        cursor = now or datetime.now()
        second_of_day = cursor.hour * 3600 + cursor.minute * 60 + cursor.second
        if second_of_day < 9 * 3600:
            day = cursor.date() - timedelta(days=1)
            return day.strftime("%Y-%m-%d"), "night"
        if second_of_day < 18 * 3600:
            return cursor.strftime("%Y-%m-%d"), "day"
        return cursor.strftime("%Y-%m-%d"), "night"

    def _normalize_duty_marker(self, duty_date: str | None, duty_shift: str | None) -> str:
        date_text = str(duty_date or "").strip()
        shift_text = str(duty_shift or "").strip().lower()
        if date_text and shift_text:
            try:
                parsed = parse_duty_date(date_text).strftime("%Y-%m-%d")
            except Exception:  # noqa: BLE001
                parsed = date_text
            return f"{parsed}/{normalize_duty_shift(shift_text)}"
        now_date, now_shift = self._infer_duty_by_now()
        return f"{now_date}/{normalize_duty_shift(now_shift)}"

    def _get_cache_store(self) -> EventFollowupCacheStore:
        if self._cache_store is not None:
            return self._cache_store
        global_paths = self.handover_cfg.get("_global_paths", {})
        self._cache_store = EventFollowupCacheStore(
            cache_state_file=EventFollowupCacheStore.SHARED_STATE_FILE,
            global_paths=global_paths if isinstance(global_paths, dict) else {},
        )
        return self._cache_store

    def _purge_memory_cache_for_scope(self, keep_scope: str) -> None:
        keep_scope_text = str(keep_scope or "").strip()
        if not keep_scope_text:
            self._memory_records_cache.clear()
            self._fetched_empty_cache_keys.clear()
            return
        suffix = f"|{keep_scope_text}"
        new_cache: Dict[str, List[Dict[str, Any]]] = {}
        new_empty_keys: set[str] = set()
        for key, value in self._memory_records_cache.items():
            if str(key).endswith(suffix):
                new_cache[key] = value
                if key in self._fetched_empty_cache_keys:
                    new_empty_keys.add(key)
        self._memory_records_cache = new_cache
        self._fetched_empty_cache_keys = new_empty_keys

    def _load_records_with_cache(
        self,
        *,
        namespace: str,
        cache_scope: str,
        source_cfg: Dict[str, Any],
        loader: Callable[[], List[Dict[str, Any]]],
        emit_log: Callable[[str], None],
    ) -> List[Dict[str, Any]]:
        app_token = str(source_cfg.get("app_token", "")).strip()
        table_id = str(source_cfg.get("table_id", "")).strip()
        page_size = max(1, int(source_cfg.get("page_size", 500) or 500))
        max_records = max(1, int(source_cfg.get("max_records", 5000) or 5000))
        cache_key = f"{app_token}|{table_id}|{page_size}|{max_records}|{cache_scope}"
        self._purge_memory_cache_for_scope(cache_scope)
        cache_store = self._get_cache_store()
        cache_store.purge_feishu_cache_namespace(namespace=namespace, keep_scope=cache_scope)

        memory_hit = self._memory_records_cache.get(cache_key)
        if isinstance(memory_hit, list):
            if memory_hit or cache_key in self._fetched_empty_cache_keys:
                emit_log(f"[交接班][{namespace}] 命中内存缓存: marker={cache_scope}, records={len(memory_hit)}")
                return [dict(x) for x in memory_hit if isinstance(x, dict)]
            emit_log(f"[交接班][{namespace}] 内存缓存为空，回查飞书: marker={cache_scope}")

        cached_records = cache_store.get_feishu_cached_records(namespace=namespace, cache_key=cache_key)
        if isinstance(cached_records, list):
            if cached_records:
                self._memory_records_cache[cache_key] = [dict(x) for x in cached_records if isinstance(x, dict)]
                emit_log(f"[交接班][{namespace}] 命中文件缓存: marker={cache_scope}, records={len(cached_records)}")
                return [dict(x) for x in cached_records if isinstance(x, dict)]
            emit_log(f"[交接班][{namespace}] 文件缓存为空，回查飞书: marker={cache_scope}")

        fresh = loader()
        normalized = [dict(x) for x in fresh if isinstance(x, dict)]
        self._memory_records_cache[cache_key] = normalized
        if normalized:
            self._fetched_empty_cache_keys.discard(cache_key)
        else:
            self._fetched_empty_cache_keys.add(cache_key)
        cache_store.set_feishu_cached_records(
            namespace=namespace,
            cache_key=cache_key,
            records=normalized,
            meta={"scope": cache_scope, "table_id": table_id},
            max_entries_per_namespace=16,
        )
        return [dict(x) for x in normalized]

    @staticmethod
    def _split_first_person(raw_people: str, split_regex: str) -> str:
        people = str(raw_people or "").strip()
        if not people:
            return ""
        try:
            parts = re.split(split_regex, people)
        except Exception:  # noqa: BLE001
            parts = re.split(r"[、,/，；;\s]+", people)
        for part in parts:
            text = str(part or "").strip()
            if text:
                return text
        return ""

    def _load_records(
        self,
        cfg: Dict[str, Any],
        *,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None],
    ) -> List[Dict[str, Any]]:
        source = cfg.get("source", {})
        table_id = str(source.get("table_id", "")).strip()
        page_size = int(source.get("page_size", 500))
        max_records = int(source.get("max_records", 5000))
        marker = self._normalize_duty_marker(duty_date, duty_shift)

        def _loader() -> List[Dict[str, Any]]:
            client = self._new_client(cfg)
            emit_log(
                f"[交接班][排班查询] 读取飞书排班: table_id={table_id}, "
                f"page_size={page_size}, max_records={max_records}"
            )
            return client.list_records(
                table_id=table_id,
                page_size=page_size,
                max_records=max_records,
            )

        return self._load_records_with_cache(
            namespace="排班查询",
            cache_scope=marker,
            source_cfg=source,
            loader=_loader,
            emit_log=emit_log,
        )

    def _load_records_from_source(
        self,
        *,
        source_cfg: Dict[str, Any],
        fallback_cfg: Dict[str, Any],
        stage: str,
        duty_date: str | None,
        duty_shift: str | None,
        emit_log: Callable[[str], None],
    ) -> List[Dict[str, Any]]:
        table_id = str(source_cfg.get("table_id", "")).strip()
        page_size = max(1, int(source_cfg.get("page_size", 500) or 500))
        max_records = max(1, int(source_cfg.get("max_records", 5000) or 5000))
        marker = self._normalize_duty_marker(duty_date, duty_shift)

        def _loader() -> List[Dict[str, Any]]:
            client = self._new_client_by_source(source_cfg, fallback_cfg)
            emit_log(
                f"[交接班][{stage}] 读取飞书: table_id={table_id}, "
                f"page_size={page_size}, max_records={max_records}"
            )
            return client.list_records(
                table_id=table_id,
                page_size=page_size,
                max_records=max_records,
            )

        return self._load_records_with_cache(
            namespace=stage,
            cache_scope=marker,
            source_cfg=source_cfg,
            loader=_loader,
            emit_log=emit_log,
        )

    def _resolve_from_records(
        self,
        *,
        records: List[Dict[str, Any]],
        building: str,
        duty_date: str,
        duty_shift: str,
        cfg: Dict[str, Any],
    ) -> ShiftRosterAssignment:
        fields_cfg = cfg.get("fields", {})
        match_cfg = cfg.get("match", {})
        alias_map = self._build_shift_alias_map(cfg)
        mode = str(match_cfg.get("building_mode", "exact_then_code")).strip().lower() or "exact_then_code"

        duty_day = parse_duty_date(duty_date)
        normalized_shift = normalize_duty_shift(duty_shift)
        next_day, next_shift = self._next_duty(duty_day, normalized_shift)
        current_date_text = duty_day.strftime("%Y-%m-%d")
        next_date_text = next_day.strftime("%Y-%m-%d")

        current_rows: List[Dict[str, Any]] = []
        next_rows: List[Dict[str, Any]] = []
        for idx, item in enumerate(records):
            if not isinstance(item, dict):
                continue
            fields = item.get("fields", {})
            if not isinstance(fields, dict):
                continue

            row_date = _normalize_date_text(fields.get(str(fields_cfg.get("duty_date", "排班日期"))))
            if not row_date:
                continue
            row_building = _field_text(fields.get(str(fields_cfg.get("building", "机楼"))))
            if not self._match_building(building, row_building, mode):
                continue
            row_shift = self._normalize_shift(fields.get(str(fields_cfg.get("shift", "班次"))), alias_map)
            if not row_shift:
                continue
            row = {
                "idx": idx,
                "date": row_date,
                "shift": row_shift,
                "building": row_building,
                "team": _field_text(fields.get(str(fields_cfg.get("team", "班组")))),
                "people_text": self._people_text_from_fields(
                    fields=fields,
                    configured_field=fields_cfg.get("people_text", self.PREFERRED_PEOPLE_TEXT_FIELD),
                ),
            }
            if row_date == current_date_text and row_shift == normalized_shift:
                current_rows.append(row)
            if row_date == next_date_text and row_shift == next_shift:
                next_rows.append(row)

        current_row = self._pick_best(current_rows)
        next_row = self._pick_best(next_rows)
        next_people = str(next_row.get("people_text", "")).strip()
        return ShiftRosterAssignment(
            current_team=str(current_row.get("team", "")).strip(),
            current_people=str(current_row.get("people_text", "")).strip(),
            next_team=str(next_row.get("team", "")).strip(),
            next_people=next_people,
            next_first_person=self._split_first_person(next_people, str(cfg.get("people_split_regex", r"[、,/，；;\s]+"))),
            source_records=len(records),
        )

    def query_assignment(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
    ) -> ShiftRosterAssignment:
        cfg = self._normalize_cfg()
        if not bool(cfg.get("enabled", True)):
            return ShiftRosterAssignment("", "", "", "", "", 0)
        records = self._load_records(
            cfg,
            duty_date=duty_date,
            duty_shift=duty_shift,
            emit_log=emit_log,
        )
        result = self._resolve_from_records(
            records=records,
            building=building,
            duty_date=duty_date,
            duty_shift=duty_shift,
            cfg=cfg,
        )
        emit_log(
            f"[交接班][排班查询] building={building}, duty={duty_date}/{duty_shift}, "
            f"current={'有' if result.current_people else '无'}, next={'有' if result.next_people else '无'}"
        )
        return result

    def query_assignments(
        self,
        *,
        buildings: List[str],
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, ShiftRosterAssignment]:
        cfg = self._normalize_cfg()
        target = [str(x).strip() for x in buildings if str(x).strip()]
        if not target or not bool(cfg.get("enabled", True)):
            return {}
        records = self._load_records(
            cfg,
            duty_date=duty_date,
            duty_shift=duty_shift,
            emit_log=emit_log,
        )
        out: Dict[str, ShiftRosterAssignment] = {}
        for building in target:
            out[building] = self._resolve_from_records(
                records=records,
                building=building,
                duty_date=duty_date,
                duty_shift=duty_shift,
                cfg=cfg,
            )
        emit_log(f"[交接班][排班查询] 预取完成: buildings={len(out)}, records={len(records)}")
        return out

    def query_long_day_cell_values(
        self,
        *,
        building: str,
        duty_date: str,
        duty_shift: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, str]:
        cfg = self._normalize_cfg()
        long_day_cfg = cfg.get("long_day", {})
        if not isinstance(long_day_cfg, dict) or not bool(long_day_cfg.get("enabled", True)):
            return {}

        source_cfg = long_day_cfg.get("source", {})
        if not isinstance(source_cfg, dict):
            source_cfg = {}
        fields_cfg = long_day_cfg.get("fields", {})
        if not isinstance(fields_cfg, dict):
            fields_cfg = {}
        match_cfg = long_day_cfg.get("match", {})
        if not isinstance(match_cfg, dict):
            match_cfg = {}
        mode = str(match_cfg.get("building_mode", "exact_then_code")).strip().lower() or "exact_then_code"
        normalized_shift = normalize_duty_shift(duty_shift)

        day_cell = str(long_day_cfg.get("day_cell", "B4")).strip().upper() or "B4"
        night_cell = str(long_day_cfg.get("night_cell", "F4")).strip().upper() or "F4"
        prefix = str(long_day_cfg.get("prefix", "长白岗：")).strip() or "长白岗："
        rest_text = str(long_day_cfg.get("rest_text", "/")).strip() or "/"

        default_values: Dict[str, str] = {}
        if re.fullmatch(r"[A-Z]+[1-9]\d*", day_cell):
            default_values[day_cell] = f"{prefix}{rest_text}"
        if re.fullmatch(r"[A-Z]+[1-9]\d*", night_cell):
            default_values[night_cell] = f"{prefix}{rest_text}"

        records = self._load_records_from_source(
            source_cfg=source_cfg,
            fallback_cfg=cfg,
            stage="长白岗查询",
            duty_date=duty_date,
            duty_shift=duty_shift,
            emit_log=emit_log,
        )

        duty_day = parse_duty_date(duty_date)
        target_day = duty_day if normalized_shift == "day" else duty_day + timedelta(days=1)
        target_date_text = target_day.strftime("%Y-%m-%d")
        target_shift_value = str(long_day_cfg.get("shift_value", "长白")).strip().casefold()

        row_people = ""
        for item in records:
            if not isinstance(item, dict):
                continue
            fields = item.get("fields", {})
            if not isinstance(fields, dict):
                continue
            row_date = _normalize_date_text(fields.get(str(fields_cfg.get("duty_date", "排班日期"))))
            if row_date != target_date_text:
                continue
            row_building = _field_text(fields.get(str(fields_cfg.get("building", "机楼"))))
            if not self._match_building(building, row_building, mode):
                continue
            shift_field_name = str(fields_cfg.get("shift", "")).strip()
            row_shift = _field_text(fields.get(shift_field_name)).casefold() if shift_field_name else ""
            if target_shift_value:
                # 班次仅作为可选过滤：有班次值时才匹配“长白”，无班次值不拦截
                if row_shift and target_shift_value not in row_shift:
                    continue
            row_people = self._people_text_from_fields(
                fields=fields,
                configured_field=fields_cfg.get("people_text", self.PREFERRED_PEOPLE_TEXT_FIELD),
            )
            if row_people:
                break

        result = dict(default_values)
        target_cell = day_cell if normalized_shift == "day" else night_cell
        if target_cell in result:
            result[target_cell] = f"{prefix}{row_people or rest_text}"

        emit_log(
            f"[交接班][长白岗查询] building={building}, duty={duty_date}/{normalized_shift}, "
            f"query_date={target_date_text}, result={'命中' if row_people else '休息'}"
        )
        return result

    def list_engineer_directory(
        self,
        *,
        emit_log: Callable[[str], None] = print,
    ) -> List[Dict[str, str]]:
        cfg = self._normalize_cfg()
        engineer_cfg = cfg.get("engineer_directory", {})
        if not isinstance(engineer_cfg, dict) or not bool(engineer_cfg.get("enabled", True)):
            return []

        source_cfg = engineer_cfg.get("source", {})
        if not isinstance(source_cfg, dict):
            source_cfg = {}
        fields_cfg = engineer_cfg.get("fields", {})
        if not isinstance(fields_cfg, dict):
            fields_cfg = {}
        records = self._load_records_from_source(
            source_cfg=source_cfg,
            fallback_cfg=cfg,
            stage="工程师目录",
            duty_date=None,
            duty_shift=None,
            emit_log=emit_log,
        )

        def _pick_field(fields: Dict[str, Any], keys: List[str]) -> str:
            for key in keys:
                name = str(key or "").strip()
                if not name:
                    continue
                value = _field_text(fields.get(name)).strip()
                if value:
                    return value
            return ""

        def _pick_raw_field(fields: Dict[str, Any], keys: List[str]) -> Any:
            for key in keys:
                name = str(key or "").strip()
                if not name or name not in fields:
                    continue
                value = fields.get(name)
                if value in (None, "", []):
                    continue
                return value
            return None

        rows: List[Dict[str, str]] = []
        dropped_empty = 0
        for item in records:
            if not isinstance(item, dict):
                continue
            fields = item.get("fields", {})
            if not isinstance(fields, dict):
                continue
            building = _pick_field(
                fields,
                [
                    str(fields_cfg.get("building", "楼栋/专业")),
                    "楼栋/专业",
                    "机楼",
                    "归属楼栋",
                    "楼栋",
                ],
            )
            specialty = _pick_field(
                fields,
                [
                    str(fields_cfg.get("specialty", "专业")),
                    "专业",
                ],
            )
            supervisor = _pick_field(
                fields,
                [
                    str(fields_cfg.get("supervisor_text", "主管（文本）")),
                    "主管（文本）",
                    "主管",
                    "值班人员（实际）",
                    "人员（文本）",
                    "人员",
                ],
            )
            supervisor_person_raw = _pick_raw_field(
                fields,
                [
                    str(fields_cfg.get("supervisor_person", "主管")),
                    "主管",
                    "主管（文本）",
                    "值班人员（实际）",
                    "人员",
                    "人员（文本）",
                ],
            )
            if not supervisor:
                supervisor = _field_text(supervisor_person_raw).strip()
            position = _pick_field(
                fields,
                [
                    str(fields_cfg.get("position", "职位")),
                    "职位",
                ],
            )
            recipient_id = _pick_field(
                fields,
                [
                    str(fields_cfg.get("recipient_id", "飞书用户ID")),
                    "飞书用户ID",
                    "user_id",
                    "open_id",
                    "open id",
                    "邮箱",
                    "email",
                    "手机号",
                    "mobile",
                ],
            )
            if not recipient_id:
                recipient_id = _extract_contact_identity(supervisor_person_raw)

            building_text = str(building or "").strip()
            if building_text and ("/" in building_text or "／" in building_text):
                parts = [seg.strip() for seg in re.split(r"[\\/／]+", building_text) if seg and str(seg).strip()]
                if parts:
                    building = parts[0]
                    if len(parts) >= 2 and not specialty:
                        specialty = parts[1]

            if not building:
                code = _extract_building_code(position or specialty or supervisor)
                if code:
                    building = f"{code}楼"
            if not building:
                building = "未配置楼栋"

            if not (specialty or supervisor or position):
                dropped_empty += 1
                continue

            rows.append(
                {
                    "building": building,
                    "specialty": specialty,
                    "supervisor": supervisor,
                    "position": position,
                    "recipient_id": recipient_id,
                }
            )

        rows.sort(key=lambda x: (x.get("building", ""), x.get("specialty", ""), x.get("supervisor", "")))
        emit_log(
            f"[交接班][工程师目录] 读取完成: source_records={len(records)}, "
            f"valid_records={len(rows)}, dropped_empty={dropped_empty}"
        )
        return rows
