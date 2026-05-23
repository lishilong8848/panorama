from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.modules.internal_bridge_http.service.client import InternalBridgeHttpClient
from app.modules.shared_bridge.service.alarm_event_page_export_service import load_alarm_event_json
from handover_log_module.repository.alarm_summary import AlarmSummary


FAMILY_ALARM_EVENT = "alarm_event_family"
EVENT_TIME_KEYS = ("event_time", "告警时间", "告警发生时间")
RECOVER_STATUS_KEYS = ("is_recover", "恢复状态", "recover_status")
ACCEPT_CONTENT_KEYS = ("accept_content", "accept_description", "处理内容", "处理描述")


def _parse_datetime_text(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("/", "-")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _pick_row_value(row: Dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in row:
            value = row.get(key)
            if value not in (None, ""):
                return value
    return row.get(keys[0]) if keys else None


def _is_recovered(value: Any) -> bool:
    text = str(value or "").strip()
    lowered = text.lower()
    if lowered in {"1", "true", "yes", "y", "是"}:
        return True
    if text in {"已恢复", "恢复"}:
        return True
    if lowered in {"0", "false", "no", "n", "否"}:
        return False
    return "已恢复" in text


class AlarmJsonRepository:
    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg if isinstance(handover_cfg, dict) else {}

    def _shared_root(self) -> Path:
        shared_bridge = self.handover_cfg.get("_shared_bridge", {})
        root_text = str(shared_bridge.get("root_dir", "") or "").strip() if isinstance(shared_bridge, dict) else ""
        if not root_text:
            raise RuntimeError("shared_bridge.root_dir 未配置，无法读取告警 JSON")
        return Path(root_text)

    def _http_client(self) -> InternalBridgeHttpClient | None:
        role_mode = str(self.handover_cfg.get("_deployment_role_mode", "") or "").strip().lower()
        if role_mode != "external":
            return None
        runtime_cfg = {
            "shared_bridge": self.handover_cfg.get("_shared_bridge", {})
            if isinstance(self.handover_cfg.get("_shared_bridge", {}), dict)
            else {},
            "internal_bridge_http": self.handover_cfg.get("_internal_bridge_http", {})
            if isinstance(self.handover_cfg.get("_internal_bridge_http", {}), dict)
            else {},
        }
        return InternalBridgeHttpClient.from_runtime_config(runtime_cfg)

    def _entry_file_path(self, entry: Dict[str, Any]) -> str:
        relative_path = str(entry.get("relative_path", "") or "").replace("\\", "/").strip()
        if relative_path:
            relative_candidate = Path(relative_path)
            if (
                not relative_candidate.is_absolute()
                and not relative_candidate.drive
                and not relative_candidate.root
                and not any(part == ".." for part in relative_candidate.parts)
            ):
                return str(self._shared_root() / relative_path.replace("/", "\\"))
            return ""
        return str(entry.get("file_path", "") or entry.get("source_file", "") or "").strip()

    def _http_alarm_entries(self, *, buildings: List[str]) -> List[Dict[str, Any]]:
        client = self._http_client()
        if client is None:
            return []
        target_buildings = self._target_buildings(buildings)
        if not target_buildings:
            return []
        queries = [
            {
                "source_family": FAMILY_ALARM_EVENT,
                "building": name,
                "limit": 200,
            }
            for name in target_buildings
        ]
        rows: List[Dict[str, Any]] = []
        try:
            results = client.source_index_batch(queries, default_limit=200)
        except Exception:
            results = []
        if results:
            for index, result in enumerate(results if isinstance(results, list) else []):
                if not isinstance(result, dict) or not bool(result.get("ok", False)):
                    continue
                fallback_building = target_buildings[index] if 0 <= index < len(target_buildings) else ""
                for entry in result.get("entries", []) if isinstance(result.get("entries", []), list) else []:
                    if not isinstance(entry, dict):
                        continue
                    item = dict(entry)
                    item.setdefault("building", fallback_building)
                    rows.append(item)
            return rows
        for name in target_buildings:
            try:
                entries = client.source_index(
                    source_family=FAMILY_ALARM_EVENT,
                    building=name,
                    limit=200,
                )
            except Exception:
                entries = []
            for entry in entries if isinstance(entries, list) else []:
                if not isinstance(entry, dict):
                    continue
                item = dict(entry)
                item.setdefault("building", name)
                rows.append(item)
        return rows

    def _empty_selection_snapshot(self, *, buildings: List[str], reference_date: date | None) -> Dict[str, Any]:
        reference_day = reference_date or datetime.now().date()
        target_buildings = self._target_buildings(buildings)
        return {
            "selection_policy": "http_source_index_required",
            "selection_reference_date": reference_day.isoformat(),
            "used_previous_day_fallback": [],
            "missing_today_buildings": list(target_buildings),
            "missing_both_days_buildings": list(target_buildings),
            "ready_count": 0,
            "failed_buildings": [],
            "blocked_buildings": [],
            "last_success_at": "",
            "current_bucket": reference_day.isoformat(),
            "buildings": [
                {
                    "building": name,
                    "bucket_key": "",
                    "status": "waiting",
                    "ready": False,
                    "downloaded_at": "",
                    "selected_downloaded_at": "",
                    "last_error": "外网端已移除共享 bridge.db 告警索引读取，请通过内网 HTTP source-index 补齐告警源文件索引",
                    "relative_path": "",
                    "resolved_file_path": "",
                    "blocked": False,
                    "blocked_reason": "",
                    "next_probe_at": "",
                    "source_kind": "",
                    "selection_scope": "missing",
                }
                for name in target_buildings
            ],
            "latest_selection": {},
            "selected_entries": [],
            "selected_by_building": {},
        }

    def _target_buildings(self, buildings: List[str] | None = None) -> List[str]:
        if buildings:
            return [str(item or "").strip() for item in buildings if str(item or "").strip()]
        sites = self.handover_cfg.get("sites", [])
        target: List[str] = []
        if isinstance(sites, list):
            for site in sites:
                if not isinstance(site, dict):
                    continue
                building = str(site.get("building", "") or "").strip()
                if building and building not in target:
                    target.append(building)
        return target

    def build_selection_snapshot(
        self,
        *,
        buildings: List[str],
        reference_date: date | None = None,
    ) -> Dict[str, Any]:
        target_buildings = self._target_buildings(buildings)
        reference_day = reference_date or datetime.now().date()
        previous_day = reference_day - timedelta(days=1)
        rows = self._http_alarm_entries(buildings=target_buildings)
        if not rows:
            return self._empty_selection_snapshot(buildings=target_buildings, reference_date=reference_day)

        grouped: Dict[str, List[Dict[str, Any]]] = {name: [] for name in target_buildings}
        for row in rows:
            row_building = str(row.get("building", "") or "").strip()
            if row_building not in grouped:
                continue
            bucket_kind = str(row.get("bucket_kind", "") or "").strip().lower()
            if bucket_kind not in {"latest", "manual"}:
                continue
            downloaded_at_dt = _parse_datetime_text(str(row.get("downloaded_at", "") or "").strip())
            file_path = self._entry_file_path(row)
            if downloaded_at_dt is None or not file_path:
                continue
            item = dict(row)
            item["file_path"] = file_path
            item["_downloaded_at_dt"] = downloaded_at_dt
            grouped[row_building].append(item)

        selected_entries: List[Dict[str, Any]] = []
        selected_by_building: Dict[str, Dict[str, Any]] = {}
        building_rows: List[Dict[str, Any]] = []
        used_previous_day_fallback: List[str] = []
        missing_today_buildings: List[str] = []
        missing_both_days_buildings: List[str] = []
        for name in target_buildings:
            candidates = sorted(
                grouped.get(name, []),
                key=lambda item: (
                    item.get("_downloaded_at_dt") or datetime.min,
                    str(item.get("updated_at", "") or "").strip(),
                    str(item.get("entry_id", "") or "").strip(),
                ),
                reverse=True,
            )
            today_candidates = [
                item
                for item in candidates
                if isinstance(item.get("_downloaded_at_dt"), datetime)
                and item["_downloaded_at_dt"].date() == reference_day
            ]
            yesterday_candidates = [
                item
                for item in candidates
                if isinstance(item.get("_downloaded_at_dt"), datetime)
                and item["_downloaded_at_dt"].date() == previous_day
            ]
            selected: Dict[str, Any] | None = None
            selection_scope = ""
            if today_candidates:
                selected = today_candidates[0]
                selection_scope = "today"
            else:
                missing_today_buildings.append(name)
                if yesterday_candidates:
                    selected = yesterday_candidates[0]
                    selection_scope = "yesterday_fallback"
                    used_previous_day_fallback.append(name)
                else:
                    missing_both_days_buildings.append(name)

            if selected is None:
                building_rows.append(
                    {
                        "building": name,
                        "bucket_key": "",
                        "status": "waiting",
                        "ready": False,
                        "downloaded_at": "",
                        "selected_downloaded_at": "",
                        "last_error": "",
                        "relative_path": "",
                        "resolved_file_path": "",
                        "blocked": False,
                        "blocked_reason": "",
                        "next_probe_at": "",
                        "source_kind": "",
                        "selection_scope": "missing",
                    }
                )
                continue

            normalized_selected = {
                key: value
                for key, value in selected.items()
                if key != "_downloaded_at_dt"
            }
            normalized_selected["selection_scope"] = selection_scope
            normalized_selected["source_kind"] = str(selected.get("bucket_kind", "") or "").strip().lower()
            selected_entries.append(normalized_selected)
            selected_by_building[name] = normalized_selected
            downloaded_at_text = str(selected.get("downloaded_at", "") or "").strip()
            building_rows.append(
                {
                    "building": name,
                    "bucket_key": str(selected.get("bucket_key", "") or "").strip(),
                    "status": "ready",
                    "ready": True,
                    "downloaded_at": downloaded_at_text,
                    "selected_downloaded_at": downloaded_at_text,
                    "last_error": "",
                    "relative_path": str(selected.get("relative_path", "") or "").strip(),
                    "resolved_file_path": str(selected.get("file_path", "") or "").strip(),
                    "blocked": False,
                    "blocked_reason": "",
                    "next_probe_at": "",
                    "source_kind": normalized_selected["source_kind"],
                    "selection_scope": selection_scope,
                }
            )

        last_success_candidates = [
            str(item.get("selected_downloaded_at", "") or "").strip()
            for item in building_rows
            if str(item.get("selected_downloaded_at", "") or "").strip()
        ]
        return {
            "selection_policy": "today_latest_else_yesterday_fallback",
            "selection_reference_date": reference_day.isoformat(),
            "used_previous_day_fallback": used_previous_day_fallback,
            "missing_today_buildings": missing_today_buildings,
            "missing_both_days_buildings": missing_both_days_buildings,
            "ready_count": len(selected_entries),
            "failed_buildings": [],
            "blocked_buildings": [],
            "last_success_at": max(last_success_candidates) if last_success_candidates else "",
            "current_bucket": reference_day.isoformat(),
            "buildings": building_rows,
            "latest_selection": {},
            "selected_entries": selected_entries,
            "selected_by_building": selected_by_building,
            "transport": "http",
        }

    @staticmethod
    def _coverage_result(
        payload: Dict[str, Any],
        *,
        start_dt: datetime,
        end_dt: datetime,
        source_kind: str,
        selection_scope: str,
        now_dt: datetime | None = None,
    ) -> Dict[str, Any]:
        query_start_dt = _parse_datetime_text(payload.get("query_start"))
        query_end_dt = _parse_datetime_text(payload.get("query_end"))
        if query_start_dt is None or query_end_dt is None:
            return {
                "ok": False,
                "mode": "insufficient",
                "available_end": "",
            }
        if query_start_dt > start_dt:
            return {
                "ok": False,
                "mode": "insufficient",
                "available_end": query_end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            }
        if query_end_dt >= end_dt:
            return {
                "ok": True,
                "full": True,
                "mode": "full",
                "available_end": query_end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            }
        source_kind_text = str(source_kind or "").strip().lower()
        selection_scope_text = str(selection_scope or "").strip().lower()
        current_now = now_dt or datetime.now()
        is_latest_like = source_kind_text == "latest" or selection_scope_text in {"today", "latest"}
        is_current_shift = start_dt <= current_now < end_dt
        if is_latest_like and is_current_shift:
            return {
                "ok": True,
                "full": False,
                "mode": "partial_latest",
                "available_end": query_end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            }
        if query_end_dt > start_dt:
            return {
                "ok": True,
                "full": False,
                "mode": "partial_history",
                "available_end": query_end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            }
        return {
            "ok": False,
            "full": False,
            "mode": "insufficient",
            "available_end": query_end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        }

    @staticmethod
    def _event_in_window(*, event_dt: datetime | None, start_dt: datetime, end_dt: datetime) -> bool:
        return bool(event_dt is not None and start_dt <= event_dt < end_dt)

    def query_alarm_summary(
        self,
        *,
        building: str,
        start_time: str,
        end_time: str,
        time_format: str = "%Y-%m-%d %H:%M:%S",
        emit_log: Callable[[str], None] = print,
        selection_snapshot: Dict[str, Any] | None = None,
        document_cache: Dict[str, Dict[str, Any]] | None = None,
    ) -> AlarmSummary:
        start_dt = datetime.strptime(str(start_time).strip(), time_format)
        end_dt = datetime.strptime(str(end_time).strip(), time_format)
        if end_dt <= start_dt:
            raise ValueError(f"告警 JSON 查询时间窗无效: start={start_time}, end={end_time}")

        building_text = str(building or "").strip()
        if not building_text:
            raise ValueError("building 不能为空")

        if isinstance(selection_snapshot, dict):
            snapshot = selection_snapshot
        else:
            snapshot = self.build_selection_snapshot(
                buildings=[building_text],
                reference_date=start_dt.date(),
            )
        if not isinstance(snapshot, dict):
            snapshot = {}
        selected_by_building = snapshot.get("selected_by_building", {}) if isinstance(snapshot, dict) else {}
        selected = selected_by_building.get(building_text) if isinstance(selected_by_building, dict) else None
        if not isinstance(selected, dict):
            raise RuntimeError("未找到当天最新或昨天回退的告警 JSON")

        file_path = Path(str(selected.get("file_path", "") or "").strip())
        if not str(file_path).strip() or not file_path.exists():
            raise RuntimeError("选中的告警 JSON 文件不存在")

        cache = document_cache if isinstance(document_cache, dict) else {}
        cache_key = str(file_path.resolve())
        payload = cache.get(cache_key)
        if not isinstance(payload, dict):
            payload = load_alarm_event_json(file_path)
            cache[cache_key] = payload

        payload_building = str(payload.get("building", "") or "").strip()
        if payload_building and payload_building != building_text:
            raise RuntimeError(f"告警 JSON building 不匹配: payload={payload_building}, building={building_text}")
        selection_scope = str(selected.get("selection_scope", "") or "").strip()
        source_kind = str(selected.get("source_kind", "") or selected.get("bucket_kind", "") or "").strip().lower()
        coverage = self._coverage_result(
            payload,
            start_dt=start_dt,
            end_dt=end_dt,
            source_kind=source_kind,
            selection_scope=selection_scope,
        )
        if not coverage["ok"]:
            raise RuntimeError(
                "告警 JSON coverage 不足: "
                f"window={start_time}~{end_time}, "
                f"query={payload.get('query_start', '')}~{payload.get('query_end', '')}"
            )
        available_end_dt = _parse_datetime_text(coverage.get("available_end"))
        effective_end_dt = (
            min(end_dt, available_end_dt)
            if isinstance(available_end_dt, datetime)
            else end_dt
        )

        total_count = 0
        unrecovered_count = 0
        accept_description = ""
        latest_unrecovered_dt = datetime.min
        rows = payload.get("rows", [])
        rows_list = rows if isinstance(rows, list) else []
        rows_total = len(rows_list)
        parsed_time_count = 0
        parse_failed_count = 0
        window_hit_count = 0
        unrecovered_hit_count = 0
        first_event_dt: datetime | None = None
        last_event_dt: datetime | None = None
        for row in rows_list:
            if not isinstance(row, dict):
                continue
            event_time_raw = _pick_row_value(row, EVENT_TIME_KEYS)
            event_dt = _parse_datetime_text(event_time_raw)
            if str(event_time_raw or "").strip():
                if event_dt is None:
                    parse_failed_count += 1
                else:
                    parsed_time_count += 1
            if not self._event_in_window(event_dt=event_dt, start_dt=start_dt, end_dt=effective_end_dt):
                continue
            window_hit_count += 1
            total_count += 1
            if first_event_dt is None or (event_dt is not None and event_dt < first_event_dt):
                first_event_dt = event_dt
            if last_event_dt is None or (event_dt is not None and event_dt > last_event_dt):
                last_event_dt = event_dt
            is_recover_text = _pick_row_value(row, RECOVER_STATUS_KEYS)
            if _is_recovered(is_recover_text):
                continue
            unrecovered_count += 1
            unrecovered_hit_count += 1
            desc_text = str(_pick_row_value(row, ACCEPT_CONTENT_KEYS) or "").strip()
            if desc_text and event_dt is not None and event_dt >= latest_unrecovered_dt:
                latest_unrecovered_dt = event_dt
                accept_description = desc_text

        selected_downloaded_at = str(selected.get("downloaded_at", "") or "").strip()
        emit_log(
            "[交接班][告警JSON] "
            f"building={building_text}, selected={selection_scope}/{source_kind}, downloaded_at={selected_downloaded_at}, "
            f"total={total_count}, unrecovered={unrecovered_count}, accept_desc={'有' if bool(accept_description) else '无'}"
        )
        if not bool(coverage.get("full", False)):
            emit_log(
                "[交接班][告警JSON] "
                f"building={building_text}, coverage部分命中，已按可用区间继续统计: "
                f"window={start_time}~{end_time}, "
                f"query={str(payload.get('query_start', '') or '').strip()}~{str(payload.get('query_end', '') or '').strip()}"
            )
        emit_log(
            "[交接班][告警JSON][诊断] "
            f"building={building_text}, file={str(file_path)}, "
            f"query_start={start_time}, query_end={end_time}, "
            f"coverage_mode={str(coverage.get('mode', '') or '-').strip() or '-'}, "
            f"available_end={str(coverage.get('available_end', '') or '-').strip() or '-'}, "
            f"json_rows={rows_total}, parsed_time={parsed_time_count}, parse_failed={parse_failed_count}, "
            f"window_hits={window_hit_count}, unrecovered_hits={unrecovered_hit_count}, "
            f"first_event={(first_event_dt.strftime('%Y-%m-%d %H:%M:%S') if isinstance(first_event_dt, datetime) else '-')}, "
            f"last_event={(last_event_dt.strftime('%Y-%m-%d %H:%M:%S') if isinstance(last_event_dt, datetime) else '-')}"
        )
        return AlarmSummary(
            total_count=total_count,
            unrecovered_count=unrecovered_count,
            accept_description=accept_description,
            used_host=str(file_path),
            used_mode=selection_scope or source_kind,
            queried_tables=[str(selected.get("entry_id", "") or "").strip()],
            source="alarm_json",
            building=building_text,
            source_kind=source_kind,
            selection_scope=selection_scope,
            selected_downloaded_at=selected_downloaded_at,
            query_start=str(payload.get("query_start", "") or "").strip(),
            query_end=str(payload.get("query_end", "") or "").strip(),
            coverage_ok=bool(coverage.get("full", False)),
            fallback_used=False,
            error="",
        )
