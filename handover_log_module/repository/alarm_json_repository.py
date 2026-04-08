from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.modules.shared_bridge.service.alarm_event_page_export_service import load_alarm_event_json
from app.modules.shared_bridge.service.alarm_external_selection import build_alarm_external_selection
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from handover_log_module.repository.alarm_summary import AlarmSummary


FAMILY_ALARM_EVENT = "alarm_event_family"


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


class AlarmJsonRepository:
    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg if isinstance(handover_cfg, dict) else {}

    def _shared_root(self) -> Path:
        shared_bridge = self.handover_cfg.get("_shared_bridge", {})
        root_text = str(shared_bridge.get("root_dir", "") or "").strip() if isinstance(shared_bridge, dict) else ""
        if not root_text:
            raise RuntimeError("shared_bridge.root_dir 未配置，无法读取告警 JSON")
        return Path(root_text)

    def _build_store(self) -> SharedBridgeStore:
        root = self._shared_root()
        store = SharedBridgeStore(root)
        store.ensure_ready()
        if not store.db_path.exists():
            raise RuntimeError(f"共享桥接索引不存在: {store.db_path}")
        return store

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
        store = self._build_store()
        return build_alarm_external_selection(
            store=store,
            shared_root=self._shared_root(),
            reference_date=reference_date,
            enabled_buildings=self._target_buildings(buildings),
        )

    @staticmethod
    def _covers_window(payload: Dict[str, Any], *, start_dt: datetime, end_dt: datetime) -> bool:
        query_start_dt = _parse_datetime_text(payload.get("query_start"))
        query_end_dt = _parse_datetime_text(payload.get("query_end"))
        if query_start_dt is None or query_end_dt is None:
            return False
        return query_start_dt <= start_dt and query_end_dt >= end_dt

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

        snapshot = selection_snapshot if isinstance(selection_snapshot, dict) else self.build_selection_snapshot(buildings=[building_text])
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
        if not self._covers_window(payload, start_dt=start_dt, end_dt=end_dt):
            raise RuntimeError(
                "告警 JSON coverage 不足: "
                f"window={start_time}~{end_time}, "
                f"query={payload.get('query_start', '')}~{payload.get('query_end', '')}"
            )

        total_count = 0
        unrecovered_count = 0
        accept_description = ""
        latest_unrecovered_dt = datetime.min
        rows = payload.get("rows", [])
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            event_dt = _parse_datetime_text(row.get("event_time"))
            if not self._event_in_window(event_dt=event_dt, start_dt=start_dt, end_dt=end_dt):
                continue
            total_count += 1
            is_recover_text = str(row.get("is_recover", "") or "").strip()
            if is_recover_text == "已恢复":
                continue
            unrecovered_count += 1
            desc_text = str(row.get("accept_content") or row.get("accept_description") or "").strip()
            if desc_text and event_dt is not None and event_dt >= latest_unrecovered_dt:
                latest_unrecovered_dt = event_dt
                accept_description = desc_text

        selection_scope = str(selected.get("selection_scope", "") or "").strip()
        source_kind = str(selected.get("source_kind", "") or selected.get("bucket_kind", "") or "").strip().lower()
        selected_downloaded_at = str(selected.get("downloaded_at", "") or "").strip()
        emit_log(
            "[交接班][告警JSON] "
            f"building={building_text}, selected={selection_scope}/{source_kind}, downloaded_at={selected_downloaded_at}, "
            f"total={total_count}, unrecovered={unrecovered_count}, accept_desc={'有' if bool(accept_description) else '无'}"
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
            coverage_ok=True,
            fallback_used=False,
            error="",
        )
