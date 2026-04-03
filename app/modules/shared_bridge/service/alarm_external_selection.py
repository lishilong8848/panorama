from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore


FAMILY_ALARM_EVENT = "alarm_event_family"


def _parse_datetime_text(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace("/", "-")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _resolve_entry_file_path(shared_root: Path, entry: Dict[str, Any]) -> Path | None:
    relative_path = str(entry.get("relative_path", "") or "").replace("\\", "/").strip()
    if not relative_path:
        return None
    file_path = shared_root / relative_path
    if not file_path.exists() or not file_path.is_file():
        return None
    return file_path


def build_alarm_external_selection(
    *,
    store: SharedBridgeStore,
    shared_root: Path,
    reference_date: date | None = None,
    enabled_buildings: List[str] | None = None,
    building: str = "",
) -> Dict[str, Any]:
    reference_day = reference_date or datetime.now().date()
    previous_day = reference_day - timedelta(days=1)
    building_filter = str(building or "").strip()
    requested_buildings = [
        str(item or "").strip()
        for item in ([building_filter] if building_filter else (enabled_buildings or []))
        if str(item or "").strip()
    ]
    target_buildings = list(dict.fromkeys(requested_buildings))
    rows = [
        item
        for item in store.list_source_cache_entries(
            source_family=FAMILY_ALARM_EVENT,
            building=building_filter,
            status="ready",
            limit=20000,
        )
        if isinstance(item, dict)
    ]

    grouped: Dict[str, List[Dict[str, Any]]] = {name: [] for name in target_buildings}
    for row in rows:
        row_building = str(row.get("building", "") or "").strip()
        if row_building not in grouped:
            continue
        bucket_kind = str(row.get("bucket_kind", "") or "").strip().lower()
        if bucket_kind not in {"latest", "manual"}:
            continue
        downloaded_at_text = str(row.get("downloaded_at", "") or "").strip()
        downloaded_at_dt = _parse_datetime_text(downloaded_at_text)
        file_path = _resolve_entry_file_path(shared_root, row)
        if downloaded_at_dt is None or file_path is None:
            continue
        grouped[row_building].append(
            {
                **row,
                "bucket_kind": bucket_kind,
                "file_path": str(file_path),
                "_downloaded_at_dt": downloaded_at_dt,
            }
        )

    for name in target_buildings:
        grouped[name].sort(
            key=lambda item: (
                item.get("_downloaded_at_dt") or datetime.min,
                str(item.get("updated_at", "") or "").strip(),
                str(item.get("entry_id", "") or "").strip(),
            ),
            reverse=True,
        )

    selected_entries: List[Dict[str, Any]] = []
    selected_by_building: Dict[str, Dict[str, Any]] = {}
    building_rows: List[Dict[str, Any]] = []
    used_previous_day_fallback: List[str] = []
    missing_today_buildings: List[str] = []
    missing_both_days_buildings: List[str] = []

    for name in target_buildings:
        candidates = grouped.get(name, [])
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
    }
