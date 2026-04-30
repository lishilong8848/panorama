from __future__ import annotations

import copy
import json
import os
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping


HANDOVER_SEGMENT_BUILDINGS: tuple[str, ...] = ("A楼", "B楼", "C楼", "D楼", "E楼")
HANDOVER_SEGMENT_BUILDING_TO_CODE: dict[str, str] = {
    "A楼": "A",
    "B楼": "B",
    "C楼": "C",
    "D楼": "D",
    "E楼": "E",
}
HANDOVER_SEGMENT_CODE_TO_BUILDING: dict[str, str] = {
    code: building for building, code in HANDOVER_SEGMENT_BUILDING_TO_CODE.items()
}
_FOOTER_DEFAULT_VISIBLE_COLUMNS: tuple[str, ...] = ("B", "C", "E", "F", "G")

_SEGMENT_ROOT_DIR = "config_segments"
_SEGMENT_FEATURE_DIR = "handover"
_SEGMENT_BUILDINGS_DIR = "buildings"
_SEGMENT_COMMON_FILE = "common.json"
_MIGRATION_BACKUP_RETENTION = 5
_handover_segment_write_lock = threading.RLock()
_handover_segment_target_locks_guard = threading.Lock()
_handover_segment_target_locks: dict[str, threading.RLock] = {}
_handover_segment_aggregate_locks_guard = threading.Lock()
_handover_segment_aggregate_locks: dict[str, threading.RLock] = {}


class HandoverSegmentRevisionConflict(ValueError):
    """Raised when a segment save request is based on a stale revision."""


def handover_segment_write_lock() -> threading.RLock:
    return _handover_segment_write_lock


def _normalized_segment_lock_key(config_path: str | Path, scope: str) -> str:
    target = Path(config_path).resolve(strict=False)
    return f"{str(target).casefold()}::{str(scope or '').strip().casefold()}"


def handover_segment_target_lock(config_path: str | Path, segment_key: str) -> threading.RLock:
    key = _normalized_segment_lock_key(config_path, f"segment:{segment_key}")
    with _handover_segment_target_locks_guard:
        lock = _handover_segment_target_locks.get(key)
        if lock is None:
            lock = threading.RLock()
            _handover_segment_target_locks[key] = lock
        return lock


def handover_segment_aggregate_lock(config_path: str | Path) -> threading.RLock:
    key = _normalized_segment_lock_key(config_path, "aggregate")
    with _handover_segment_aggregate_locks_guard:
        lock = _handover_segment_aggregate_locks.get(key)
        if lock is None:
            lock = threading.RLock()
            _handover_segment_aggregate_locks[key] = lock
        return lock


def normalize_handover_building_code(code: str | None) -> str:
    text = str(code or "").strip().upper()
    if text not in HANDOVER_SEGMENT_CODE_TO_BUILDING:
        raise ValueError("仅支持 A/B/C/D/E 五个楼栋配置")
    return text


def building_name_from_segment_code(code: str | None) -> str:
    return HANDOVER_SEGMENT_CODE_TO_BUILDING[normalize_handover_building_code(code)]


def building_code_from_name(building: str | None) -> str:
    text = str(building or "").strip()
    if text not in HANDOVER_SEGMENT_BUILDING_TO_CODE:
        raise ValueError(f"不支持的楼栋配置: {text or '-'}")
    return HANDOVER_SEGMENT_BUILDING_TO_CODE[text]


def handover_segment_root(config_path: str | Path) -> Path:
    target = Path(config_path)
    return target.parent / _SEGMENT_ROOT_DIR / _SEGMENT_FEATURE_DIR


def handover_common_segment_path(config_path: str | Path) -> Path:
    return handover_segment_root(config_path) / _SEGMENT_COMMON_FILE


def handover_building_segment_path(config_path: str | Path, building_or_code: str) -> Path:
    building = (
        building_name_from_segment_code(building_or_code)
        if str(building_or_code or "").strip().upper() in HANDOVER_SEGMENT_CODE_TO_BUILDING
        else str(building_or_code or "").strip()
    )
    code = building_code_from_name(building)
    return handover_segment_root(config_path) / _SEGMENT_BUILDINGS_DIR / f"{code}.json"


def expected_handover_segment_paths(config_path: str | Path) -> dict[str, Path]:
    mapping: dict[str, Path] = {"common": handover_common_segment_path(config_path)}
    for building in HANDOVER_SEGMENT_BUILDINGS:
        mapping[building] = handover_building_segment_path(config_path, building)
    return mapping


def _iso_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def build_segment_document(
    data: Mapping[str, Any] | None = None,
    *,
    revision: int = 1,
    updated_at: str | None = None,
) -> Dict[str, Any]:
    return {
        "revision": max(0, int(revision)),
        "updated_at": str(updated_at or _iso_now()),
        "data": copy.deepcopy(dict(data or {})),
    }


def read_segment_document(path: str | Path, *, default_data: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return build_segment_document(default_data, revision=0, updated_at="")
    try:
        payload = json.loads(target.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"交接班分段配置解析失败: {target} ({exc})") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"交接班分段配置格式错误: {target}")
    data = payload.get("data", {})
    if not isinstance(data, dict):
        raise ValueError(f"交接班分段配置 data 必须是对象: {target}")
    revision = payload.get("revision", 0)
    try:
        revision_value = max(0, int(revision))
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"交接班分段配置 revision 非法: {target}") from exc
    return {
        "revision": revision_value,
        "updated_at": str(payload.get("updated_at", "") or "").strip(),
        "data": copy.deepcopy(data),
    }


def write_segment_document(path: str | Path, payload: Mapping[str, Any]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_name(f".{target.name}.{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp_path.open("w", encoding="utf-8-sig", newline="\n") as handle:
        json.dump(dict(payload), handle, ensure_ascii=False, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, target)
    return target


def create_pre_handover_segment_backup(config_path: str | Path) -> Path | None:
    target = Path(config_path)
    if not target.exists():
        return None
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    backup_path = target.with_name(f"{target.stem}.pre_handover_segments.{timestamp}{target.suffix}")
    counter = 1
    while backup_path.exists():
        backup_path = target.with_name(f"{target.stem}.pre_handover_segments.{timestamp}.{counter}{target.suffix}")
        counter += 1
    shutil.copy2(target, backup_path)
    backups = sorted(
        target.parent.glob(f"{target.stem}.pre_handover_segments.*{target.suffix}"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    for extra in backups[max(0, int(_MIGRATION_BACKUP_RETENTION)) :]:
        try:
            extra.unlink()
        except OSError:
            pass
    return backup_path


def has_any_handover_segment_file(config_path: str | Path) -> bool:
    return any(path.exists() for path in expected_handover_segment_paths(config_path).values())


def has_all_handover_segment_files(config_path: str | Path) -> bool:
    return all(path.exists() for path in expected_handover_segment_paths(config_path).values())


def read_all_segment_documents(config_path: str | Path) -> tuple[Dict[str, Any], dict[str, Dict[str, Any]]]:
    common_doc = read_segment_document(handover_common_segment_path(config_path))
    building_docs = {
        building: read_segment_document(handover_building_segment_path(config_path, building))
        for building in HANDOVER_SEGMENT_BUILDINGS
    }
    return common_doc, building_docs


def _deep_merge_dict(base: Dict[str, Any], overlay: Mapping[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(overlay, Mapping):
        return base
    for key, value in overlay.items():
        if isinstance(value, Mapping) and isinstance(base.get(key), dict):
            base[key] = _deep_merge_dict(copy.deepcopy(base[key]), value)
        else:
            base[key] = copy.deepcopy(value)
    return base


def _normalize_footer_default_rows(rows: Any) -> list[Dict[str, Any]]:
    normalized: list[Dict[str, Any]] = []
    source_rows = rows if isinstance(rows, list) else []
    for raw_row in source_rows:
        cells = raw_row.get("cells", {}) if isinstance(raw_row, dict) else {}
        if not isinstance(cells, Mapping):
            cells = {}
        normalized_cells = {
            column: str(cells.get(column, "") or "").strip()
            for column in _FOOTER_DEFAULT_VISIBLE_COLUMNS
        }
        normalized.append({"cells": normalized_cells})
    if normalized:
        return normalized
    return [{"cells": {column: "" for column in _FOOTER_DEFAULT_VISIBLE_COLUMNS}}]


def _normalize_footer_defaults_by_building(payload: Any) -> dict[str, Any]:
    output: dict[str, Any] = {}
    source = payload if isinstance(payload, Mapping) else {}
    for raw_building, raw_payload in source.items():
        building = str(raw_building or "").strip()
        if not building:
            continue
        rows = raw_payload.get("rows", []) if isinstance(raw_payload, Mapping) else []
        output[building] = {"rows": _normalize_footer_default_rows(rows)}
    return output


def _normalize_review_link_recipients_by_building(payload: Any) -> dict[str, Any]:
    output: dict[str, Any] = {}
    source = payload if isinstance(payload, Mapping) else {}
    for raw_building, raw_items in source.items():
        building = str(raw_building or "").strip()
        if not building:
            continue
        rows = raw_items if isinstance(raw_items, list) else []
        normalized_rows = []
        for raw_row in rows:
            if not isinstance(raw_row, Mapping):
                continue
            normalized_rows.append(
                {
                    "note": str(raw_row.get("note", "") or "").strip(),
                    "open_id": str(raw_row.get("open_id", "") or "").strip(),
                    "enabled": False if raw_row.get("enabled", True) is False else True,
                }
            )
        output[building] = normalized_rows
    return output


def _ensure_child_dict(root: Dict[str, Any], key: str) -> Dict[str, Any]:
    child = root.get(key)
    if not isinstance(child, dict):
        child = {}
        root[key] = child
    return child


def _clear_segment_backed_handover_fields(handover: Dict[str, Any]) -> Dict[str, Any]:
    output = copy.deepcopy(handover)
    cell_rules = _ensure_child_dict(output, "cell_rules")
    building_rows = cell_rules.get("building_rows", {})
    if isinstance(building_rows, dict):
        cell_rules["building_rows"] = {
            key: copy.deepcopy(value)
            for key, value in building_rows.items()
            if key not in HANDOVER_SEGMENT_BUILDINGS
        }
    else:
        cell_rules["building_rows"] = {}

    cloud_sheet_sync = _ensure_child_dict(output, "cloud_sheet_sync")
    sheet_names = cloud_sheet_sync.get("sheet_names", {})
    if isinstance(sheet_names, dict):
        cloud_sheet_sync["sheet_names"] = {
            key: copy.deepcopy(value)
            for key, value in sheet_names.items()
            if key not in HANDOVER_SEGMENT_BUILDINGS
        }
    else:
        cloud_sheet_sync["sheet_names"] = {}

    review_ui = _ensure_child_dict(output, "review_ui")
    cabinet_defaults = review_ui.get("cabinet_power_defaults_by_building", {})
    if isinstance(cabinet_defaults, dict):
        review_ui["cabinet_power_defaults_by_building"] = {
            key: copy.deepcopy(value)
            for key, value in cabinet_defaults.items()
            if key not in HANDOVER_SEGMENT_BUILDINGS
        }
    else:
        review_ui["cabinet_power_defaults_by_building"] = {}
    footer_defaults = review_ui.get("footer_inventory_defaults_by_building", {})
    if isinstance(footer_defaults, dict):
        review_ui["footer_inventory_defaults_by_building"] = {
            key: copy.deepcopy(value)
            for key, value in footer_defaults.items()
            if key not in HANDOVER_SEGMENT_BUILDINGS
        }
    else:
        review_ui["footer_inventory_defaults_by_building"] = {}
    review_link_recipients = review_ui.get("review_link_recipients_by_building", {})
    if isinstance(review_link_recipients, dict):
        cleaned_review_link_recipients = _normalize_review_link_recipients_by_building(
            {
                key: copy.deepcopy(value)
                for key, value in review_link_recipients.items()
                if key not in HANDOVER_SEGMENT_BUILDINGS
            }
        )
        if cleaned_review_link_recipients:
            review_ui["review_link_recipients_by_building"] = cleaned_review_link_recipients
        else:
            review_ui.pop("review_link_recipients_by_building", None)
    else:
        review_ui.pop("review_link_recipients_by_building", None)
    return output


def extract_handover_common_data(cfg: Mapping[str, Any]) -> Dict[str, Any]:
    features = cfg.get("features", {}) if isinstance(cfg, Mapping) else {}
    handover = features.get("handover_log", {}) if isinstance(features, Mapping) else {}
    if not isinstance(handover, Mapping):
        return {}
    return _clear_segment_backed_handover_fields(dict(handover))


def extract_handover_building_data(cfg: Mapping[str, Any], building: str) -> Dict[str, Any]:
    building_name = str(building or "").strip()
    if building_name not in HANDOVER_SEGMENT_BUILDINGS:
        raise ValueError(f"不支持的楼栋配置: {building_name or '-'}")
    features = cfg.get("features", {}) if isinstance(cfg, Mapping) else {}
    handover = features.get("handover_log", {}) if isinstance(features, Mapping) else {}
    if not isinstance(handover, Mapping):
        handover = {}
    cell_rules = handover.get("cell_rules", {}) if isinstance(handover, Mapping) else {}
    cloud_sheet_sync = handover.get("cloud_sheet_sync", {}) if isinstance(handover, Mapping) else {}
    review_ui = handover.get("review_ui", {}) if isinstance(handover, Mapping) else {}
    building_rows = cell_rules.get("building_rows", {}) if isinstance(cell_rules, Mapping) else {}
    sheet_names = cloud_sheet_sync.get("sheet_names", {}) if isinstance(cloud_sheet_sync, Mapping) else {}
    cabinet_defaults = (
        review_ui.get("cabinet_power_defaults_by_building", {}) if isinstance(review_ui, Mapping) else {}
    )
    footer_defaults = (
        review_ui.get("footer_inventory_defaults_by_building", {}) if isinstance(review_ui, Mapping) else {}
    )
    review_link_recipients = (
        review_ui.get("review_link_recipients_by_building", {}) if isinstance(review_ui, Mapping) else {}
    )
    cabinet_payload = (
        {building_name: copy.deepcopy(cabinet_defaults.get(building_name))}
        if isinstance(cabinet_defaults, Mapping) and building_name in cabinet_defaults
        else {}
    )
    footer_payload = (
        _normalize_footer_defaults_by_building({building_name: copy.deepcopy(footer_defaults.get(building_name))})
        if isinstance(footer_defaults, Mapping) and building_name in footer_defaults
        else {}
    )
    review_link_recipients_payload = (
        _normalize_review_link_recipients_by_building(
            {building_name: copy.deepcopy(review_link_recipients.get(building_name))}
        )
        if isinstance(review_link_recipients, Mapping) and building_name in review_link_recipients
        else {}
    )
    return {
        "cell_rules": {
            "building_rows": {
                building_name: copy.deepcopy(building_rows.get(building_name, []))
                if isinstance(building_rows, Mapping)
                else []
            }
        },
        "cloud_sheet_sync": {
            "sheet_names": {
                building_name: copy.deepcopy(sheet_names.get(building_name, ""))
                if isinstance(sheet_names, Mapping)
                else ""
            }
        },
        "review_ui": {
            "cabinet_power_defaults_by_building": cabinet_payload,
            "footer_inventory_defaults_by_building": footer_payload,
            "review_link_recipients_by_building": review_link_recipients_payload,
        },
    }


def apply_handover_segment_data(
    cfg: Mapping[str, Any],
    *,
    common_data: Mapping[str, Any] | None = None,
    building_data_by_name: Mapping[str, Mapping[str, Any]] | None = None,
) -> Dict[str, Any]:
    output = copy.deepcopy(dict(cfg))
    features = output.get("features")
    if not isinstance(features, dict):
        features = {}
        output["features"] = features
    handover = features.get("handover_log")
    if not isinstance(handover, dict):
        handover = {}
    handover = _clear_segment_backed_handover_fields(handover)
    handover = _deep_merge_dict(handover, common_data or {})
    for building in HANDOVER_SEGMENT_BUILDINGS:
        building_payload = (building_data_by_name or {}).get(building)
        if isinstance(building_payload, Mapping):
            handover = _deep_merge_dict(handover, building_payload)
    features["handover_log"] = handover
    output["features"] = features
    return output


def build_segment_documents_from_config(cfg: Mapping[str, Any]) -> tuple[Dict[str, Any], dict[str, Dict[str, Any]]]:
    common_doc = build_segment_document(extract_handover_common_data(cfg))
    building_docs = {
        building: build_segment_document(extract_handover_building_data(cfg, building))
        for building in HANDOVER_SEGMENT_BUILDINGS
    }
    return common_doc, building_docs
