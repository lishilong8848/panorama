from __future__ import annotations

import copy
from typing import Any, Dict, List


INTERNAL_BUILDINGS = ["A楼", "B楼", "C楼", "D楼", "E楼"]


_BRIDGE_EVENT_TYPE_LABELS = {
    "await_external": "等待外网继续处理",
    "claimed": "已认领",
    "completed": "已完成",
    "log": "日志",
    "waiting_source_sync": "等待内网补采同步",
    "retired_feature": "停用功能已拦截",
}

_BRIDGE_ERROR_TEXTS = {
    "internal_download_failed": "共享文件准备失败",
    "internal_query_failed": "内网查询失败",
    "external_upload_failed": "外网上传失败",
    "external_continue_failed": "外网继续处理失败",
    "missing_source_file": "缺少共享文件",
    "await_external": "等待外网继续处理",
    "shared_bridge_disabled": "共享桥接未启用",
    "shared_bridge_service_unavailable": "共享桥接服务不可用",
    "disabled_or_switching": "当前未启用共享桥接",
    "disabled_or_unselected": "当前未启用共享桥接",
    "misconfigured": "共享桥接目录未配置",
    "database is locked": "共享桥接数据库正忙，请稍后重试",
    "unable to open database file": "无法打开共享桥接数据库文件",
    "cannot operate on a closed database": "共享桥接数据库连接已关闭",
    "cannot operate on a closed database.": "共享桥接数据库连接已关闭",
}


def bridge_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if "permissionerror" in lowered or "winerror 5" in lowered:
        return "共享桥接目录无写入权限"
    if "no such table" in lowered:
        return "共享桥接数据库结构未初始化"
    return _BRIDGE_ERROR_TEXTS.get(lowered, _BRIDGE_EVENT_TYPE_LABELS.get(lowered, text))


def _string(value: Any) -> str:
    return str(value or "").strip()


def _normalize_source_cache_building_row(payload: Any, *, building: str = "", fallback_bucket: str = "") -> Dict[str, Any]:
    row = payload if isinstance(payload, dict) else {}
    raw_status = _string(row.get("status", "")).lower() or _string(row.get("status_key", "")).lower()
    return {
        "building": _string(row.get("building", "")) or _string(building) or "-",
        "bucket_key": _string(row.get("bucket_key", "")) or _string(fallback_bucket) or "-",
        "status": raw_status if raw_status in {"ready", "failed", "downloading", "consumed"} else "waiting",
        "ready": bool(row.get("ready", False)),
        "downloaded_at": _string(row.get("downloaded_at", "")),
        "last_error": bridge_text(row.get("last_error", "")),
        "relative_path": _string(row.get("relative_path", "")),
        "resolved_file_path": _string(row.get("resolved_file_path", "")),
        "started_at": _string(row.get("started_at", "")),
        "blocked": bool(row.get("blocked", False)),
        "blocked_reason": bridge_text(row.get("blocked_reason", "")),
        "next_probe_at": _string(row.get("next_probe_at", "")),
    }


def present_source_cache_building_row(
    payload: Any,
    *,
    building: str = "",
    fallback_bucket: str = "",
    source_family: str = "",
    waiting_text: str = "等待共享文件就绪",
    blocked_text: str = "等待内网恢复",
) -> Dict[str, Any]:
    row = _normalize_source_cache_building_row(payload, building=building, fallback_bucket=fallback_bucket)
    status_key = row["status"]
    tone = "warning"
    status_text = "等待中"
    if status_key == "ready" and bool(row.get("ready", False)):
        tone = "success"
        status_text = "已就绪"
    elif status_key == "consumed":
        tone = "info"
        status_text = "已消费"
    elif status_key == "downloading":
        tone = "info"
        status_text = "下载中"
    elif status_key == "failed":
        tone = "danger"
        status_text = "失败"
    elif bool(row.get("blocked", False)):
        status_key = "blocked"
        tone = "warning"
        status_text = blocked_text
    refresh_pending = status_key == "downloading"
    refresh_allowed = not refresh_pending
    refresh_label = "拉取中..." if refresh_pending else "重新拉取"
    refresh_disabled_reason = "当前楼栋正在下载共享文件" if refresh_pending else ""
    next_probe_text = _string(row.get("next_probe_at", ""))
    blocked_reason_text = _string(row.get("blocked_reason", "")) or "楼栋页面异常，等待内网恢复"
    blocked_detail_suffix = f" / 下次自动检测：{next_probe_text}" if next_probe_text else ""
    detail_text = (
        (
            f"{blocked_reason_text}"
            f"{blocked_detail_suffix}"
        )
        if bool(row.get("blocked", False))
        else (
            row.get("last_error", "")
            or row.get("downloaded_at", "")
            or row.get("started_at", "")
            or (row.get("resolved_file_path", "") or waiting_text)
        )
    )
    meta_lines: List[str] = []
    bucket_text = _string(row.get("bucket_key", "")) or _string(fallback_bucket) or "-"
    _push_meta_line(meta_lines, f"时间桶：{bucket_text}")
    if bool(row.get("blocked", False)):
        _push_meta_line(
            meta_lines,
            f"最近错误：{blocked_reason_text or row.get('last_error', '') or '楼栋页面异常，等待内网恢复'}",
        )
    elif row.get("last_error", ""):
        _push_meta_line(meta_lines, f"最近错误：{row['last_error']}")
    elif row.get("downloaded_at", ""):
        _push_meta_line(meta_lines, f"最近成功：{row['downloaded_at']}")
    elif row.get("started_at", ""):
        _push_meta_line(meta_lines, f"开始时间：{row['started_at']}")
    if row.get("resolved_file_path", ""):
        _push_meta_line(meta_lines, f"共享路径：{row['resolved_file_path']}")
    elif row.get("relative_path", ""):
        _push_meta_line(meta_lines, f"缓存文件：{row['relative_path']}")
    elif status_key != "waiting":
        _push_meta_line(meta_lines, "共享文件未登记")
    return {
        **row,
        "source_family": _string(source_family),
        "status_key": status_key,
        "tone": tone,
        "status_text": status_text,
        "detail_text": _string(detail_text),
        "reason_code": status_key,
        "meta_lines": meta_lines,
        "actions": {
            "refresh": {
                "allowed": refresh_allowed,
                "pending": refresh_pending,
                "label": refresh_label,
                "disabled_reason": refresh_disabled_reason,
            }
        },
    }


def has_meaningful_source_cache_building_row(payload: Any, *, fallback_bucket: str = "") -> bool:
    row = _normalize_source_cache_building_row(payload, fallback_bucket=fallback_bucket)
    if row["status"] in {"ready", "failed", "downloading", "consumed"}:
        return True
    if bool(row.get("ready")) or bool(row.get("blocked")):
        return True
    if any(
        _string(row.get(key, ""))
        for key in ("downloaded_at", "started_at", "relative_path", "resolved_file_path", "last_error")
    ):
        return True
    return False


def present_source_cache_family(
    payload: Any,
    *,
    key: str = "",
    title: str = "",
    fallback_bucket: str = "",
    bucket_scope_text: str = "本小时",
) -> Dict[str, Any]:
    family = payload if isinstance(payload, dict) else {}
    family_key = _string(key) or _string(family.get("key", ""))
    family_title = _string(title) or _string(family.get("title", ""))
    manual_refresh = family.get("manual_refresh", {}) if isinstance(family.get("manual_refresh", {}), dict) else {}
    current_bucket = _string(family.get("current_bucket", "")) or _string(fallback_bucket)
    raw_rows = family.get("buildings", []) if isinstance(family.get("buildings", []), list) else []
    buildings = [
        present_source_cache_building_row(
            item,
            building=_string(item.get("building", "")),
            fallback_bucket=current_bucket,
            source_family=family_key,
        )
        for item in raw_rows
        if isinstance(item, dict)
    ]
    ready_count = int(family.get("ready_count", 0) or 0)
    if ready_count <= 0:
        ready_count = sum(1 for item in buildings if item.get("status_key") == "ready")
    failed_buildings = [
        _string(item)
        for item in (family.get("failed_buildings", []) if isinstance(family.get("failed_buildings", []), list) else [])
        if _string(item)
    ]
    if not failed_buildings:
        failed_buildings = [item["building"] for item in buildings if item.get("status_key") == "failed"]
    blocked_buildings = [
        _string(item)
        for item in (family.get("blocked_buildings", []) if isinstance(family.get("blocked_buildings", []), list) else [])
        if _string(item)
    ]
    if not blocked_buildings:
        blocked_buildings = [item["building"] for item in buildings if bool(item.get("blocked", False))]
    last_success_at = _string(family.get("last_success_at", ""))
    if not last_success_at:
        last_success_candidates = [_string(item.get("downloaded_at", "")) for item in buildings if _string(item.get("downloaded_at", ""))]
        last_success_at = max(last_success_candidates) if last_success_candidates else ""
    has_failures = any(item.get("status_key") == "failed" for item in buildings) or bool(failed_buildings)
    has_blocked = any(item.get("status_key") == "blocked" for item in buildings) or bool(blocked_buildings)
    has_downloading = any(item.get("status_key") == "downloading" for item in buildings)
    all_ready = bool(buildings) and all(item.get("status_key") == "ready" for item in buildings)
    downloading_count = sum(1 for item in buildings if item.get("status_key") == "downloading")
    problem_count = len(failed_buildings) + len(blocked_buildings)
    tone = "warning"
    status_text = f"{bucket_scope_text}仍有楼栋等待中"
    summary_text = ""
    reason_code = "waiting"
    if has_failures:
        tone = "danger"
        status_text = f"{bucket_scope_text}存在失败楼栋"
        summary_text = "最近一轮共享文件同步存在失败楼栋，请检查共享目录和内网页面状态。"
        reason_code = "failed"
    elif has_blocked:
        tone = "warning"
        status_text = f"{bucket_scope_text}存在等待恢复楼栋"
        summary_text = "部分楼栋正在等待内网恢复，恢复后会自动继续同步共享文件。"
        reason_code = "blocked"
    elif all_ready:
        tone = "success"
        status_text = f"{bucket_scope_text}全部就绪"
        summary_text = f"{family_title or '共享文件'}当前已全部就绪。"
        reason_code = "ready"
    elif has_downloading:
        tone = "info"
        status_text = f"{bucket_scope_text}同步中"
        summary_text = "部分楼栋当前正在下载或校验共享文件。"
        reason_code = "downloading"
    meta_lines: List[str] = []
    bucket_label = f"{bucket_scope_text}桶" if bucket_scope_text else "时间桶"
    _push_meta_line(meta_lines, f"{bucket_label}：{current_bucket or '-'}")
    if last_success_at:
        _push_meta_line(meta_lines, f"最近成功：{last_success_at}")
    if failed_buildings:
        _push_meta_line(meta_lines, f"失败楼栋：{' / '.join(failed_buildings)}")
    if blocked_buildings:
        _push_meta_line(meta_lines, f"等待恢复：{' / '.join(blocked_buildings)}")
    detail_text = summary_text or _string(family.get("last_error", "")) or (
        f"{family_title or '共享文件'}当前等待共享文件继续同步。"
    )
    items = [
        {
            "label": bucket_label,
            "value": current_bucket or "-",
            "tone": "info" if current_bucket else "neutral",
        },
        {
            "label": "已就绪楼栋",
            "value": f"{ready_count}/{len(buildings) or 5}",
            "tone": "success" if all_ready else ("info" if ready_count else "neutral"),
        },
        {
            "label": "下载中楼栋",
            "value": f"{downloading_count} 个",
            "tone": "info" if downloading_count else "neutral",
        },
        {
            "label": "异常楼栋",
            "value": f"{problem_count} 个",
            "tone": "danger" if failed_buildings else ("warning" if blocked_buildings else "neutral"),
        },
    ]
    return {
        **family,
        "key": family_key,
        "title": family_title,
        "ready_count": ready_count,
        "failed_buildings": failed_buildings,
        "blocked_buildings": blocked_buildings,
        "last_success_at": last_success_at,
        "current_bucket": current_bucket,
        "buildings": buildings,
        "has_failures": has_failures,
        "has_blocked": has_blocked,
        "has_downloading": has_downloading,
        "all_ready": all_ready,
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "detail_text": detail_text,
        "reason_code": reason_code,
        "meta_lines": meta_lines,
        "items": items,
        "actions": family.get("actions", {}) if isinstance(family.get("actions", {}), dict) else {},
        "manual_refresh": {
            "running": bool(manual_refresh.get("running", False)),
            "last_run_at": _string(manual_refresh.get("last_run_at", "")),
            "last_success_at": _string(manual_refresh.get("last_success_at", "")),
            "last_error": bridge_text(manual_refresh.get("last_error", "")),
            "bucket_key": _string(manual_refresh.get("bucket_key", "")),
            "successful_buildings": [
                _string(item)
                for item in (
                    manual_refresh.get("successful_buildings", [])
                    if isinstance(manual_refresh.get("successful_buildings", []), list)
                    else []
                )
                if _string(item)
            ],
            "failed_buildings": [
                _string(item)
                for item in (
                    manual_refresh.get("failed_buildings", [])
                    if isinstance(manual_refresh.get("failed_buildings", []), list)
                    else []
                )
                if _string(item)
            ],
            "blocked_buildings": [
                _string(item)
                for item in (
                    manual_refresh.get("blocked_buildings", [])
                    if isinstance(manual_refresh.get("blocked_buildings", []), list)
                    else []
                )
                if _string(item)
            ],
            "total_row_count": int(manual_refresh.get("total_row_count", 0) or 0),
            "building_row_counts": manual_refresh.get("building_row_counts", {})
            if isinstance(manual_refresh.get("building_row_counts", {}), dict)
            else {},
            "query_start": _string(manual_refresh.get("query_start", "")),
            "query_end": _string(manual_refresh.get("query_end", "")),
        },
    }


def _normalize_latest_selection_building(payload: Any, *, fallback_bucket: str = "") -> Dict[str, Any]:
    row = payload if isinstance(payload, dict) else {}
    raw_status = _string(row.get("status", "")).lower() or _string(row.get("status_key", "")).lower()
    version_gap = row.get("version_gap", None)
    try:
        version_gap_value = int(version_gap) if version_gap is not None and str(version_gap).strip() else None
    except Exception:
        version_gap_value = None
    return {
        "building": _string(row.get("building", "")) or "-",
        "bucket_key": _string(row.get("bucket_key", "")) or _string(fallback_bucket) or "-",
        "status": raw_status if raw_status in {"ready", "stale", "failed"} else "waiting",
        "using_fallback": bool(row.get("using_fallback", False)),
        "version_gap": version_gap_value,
        "downloaded_at": _string(row.get("downloaded_at", "")),
        "last_error": bridge_text(row.get("last_error", "")),
        "relative_path": _string(row.get("relative_path", "")),
        "resolved_file_path": _string(row.get("resolved_file_path", "")),
        "blocked": bool(row.get("blocked", False)),
        "blocked_reason": bridge_text(row.get("blocked_reason", "")),
        "next_probe_at": _string(row.get("next_probe_at", "")),
    }


def _push_meta_line(lines: List[str], value: Any) -> None:
    text = _string(value)
    if text:
        lines.append(text)


def present_latest_selection_building(payload: Any, *, fallback_bucket: str = "") -> Dict[str, Any]:
    row = _normalize_latest_selection_building(payload, fallback_bucket=fallback_bucket)
    tone = "warning"
    status_text = "等待共享文件就绪"
    if row["status"] == "ready" and bool(row.get("using_fallback", False)):
        tone = "warning"
        status_text = "使用上一版共享文件"
    elif row["status"] == "ready":
        tone = "success"
        status_text = "已就绪"
    elif row["status"] == "failed":
        tone = "danger"
        status_text = "失败"
    elif row["status"] == "stale":
        tone = "danger"
        status_text = "版本过旧，等待更新"
    meta_lines: List[str] = []
    reference_bucket = _string(row.get("bucket_key", "")) or _string(fallback_bucket) or "-"
    _push_meta_line(meta_lines, f"时间桶：{reference_bucket}")
    if bool(row.get("using_fallback", False)) and row.get("version_gap", None) is not None:
        _push_meta_line(meta_lines, f"较最新版本落后 {row['version_gap']} 桶")
    if row.get("last_error", ""):
        _push_meta_line(meta_lines, f"最近错误：{row['last_error']}")
    else:
        _push_meta_line(meta_lines, f"最近成功：{_string(row.get('downloaded_at', '')) or '-'}")
    if row.get("resolved_file_path", ""):
        _push_meta_line(meta_lines, f"共享路径：{row['resolved_file_path']}")
    elif row.get("relative_path", ""):
        _push_meta_line(meta_lines, f"缓存文件：{row['relative_path']}")
    elif _string(row.get("status", "")) != "waiting":
        _push_meta_line(meta_lines, "共享文件未登记")
    return {
        **row,
        "status_key": row["status"],
        "tone": tone,
        "status_text": status_text,
        "detail_text": row.get("last_error", "") or row.get("downloaded_at", "") or (row.get("resolved_file_path", "") or "等待共享文件就绪"),
        "meta_lines": meta_lines,
    }


def has_meaningful_latest_selection_building(payload: Any, *, fallback_bucket: str = "") -> bool:
    row = _normalize_latest_selection_building(payload, fallback_bucket=fallback_bucket)
    if row["status"] in {"ready", "stale", "failed"}:
        return True
    if bool(row.get("using_fallback", False)):
        return True
    if row.get("version_gap", None) is not None:
        return True
    if any(_string(row.get(key, "")) for key in ("downloaded_at", "relative_path", "resolved_file_path", "last_error")):
        return True
    return False


def present_latest_selection_overview(payload: Any, *, key: str = "", title: str = "") -> Dict[str, Any]:
    selection = payload if isinstance(payload, dict) else {}
    best_bucket_key = _string(selection.get("best_bucket_key", ""))
    best_bucket_age_hours = selection.get("best_bucket_age_hours", None)
    try:
        best_bucket_age_value = float(best_bucket_age_hours) if best_bucket_age_hours not in ("", None) else None
    except Exception:
        best_bucket_age_value = None
    best_bucket_age_text = ""
    if best_bucket_age_value is not None:
        rounded = round(max(0.0, best_bucket_age_value), 1)
        best_bucket_age_text = f"{int(rounded)} 小时" if float(rounded).is_integer() else f"{rounded:.1f} 小时"
    is_best_bucket_too_old = bool(selection.get("is_best_bucket_too_old", False))
    fallback_buildings = [_string(item) for item in (selection.get("fallback_buildings", []) if isinstance(selection.get("fallback_buildings", []), list) else []) if _string(item)]
    missing_buildings = [_string(item) for item in (selection.get("missing_buildings", []) if isinstance(selection.get("missing_buildings", []), list) else []) if _string(item)]
    failed_buildings = [_string(item) for item in (selection.get("failed_buildings", []) if isinstance(selection.get("failed_buildings", []), list) else []) if _string(item)]
    stale_buildings = [_string(item) for item in (selection.get("stale_buildings", []) if isinstance(selection.get("stale_buildings", []), list) else []) if _string(item)]
    buildings = [
        present_latest_selection_building(item, fallback_bucket=best_bucket_key)
        for item in (selection.get("buildings", []) if isinstance(selection.get("buildings", []), list) else [])
        if isinstance(item, dict)
    ]
    if not failed_buildings:
        failed_buildings = [item["building"] for item in buildings if item.get("status_key") == "failed" and _string(item.get("building", ""))]
    can_proceed = bool(selection.get("can_proceed", False)) and not failed_buildings and not missing_buildings and not stale_buildings and not is_best_bucket_too_old
    tone = "warning"
    status_text = "等待共享文件就绪"
    summary_text = "共享文件尚未齐套。"
    reason_code = "waiting"
    if failed_buildings:
        tone = "danger"
        status_text = "存在失败楼栋"
        summary_text = f"以下楼栋共享文件同步失败：{' / '.join(failed_buildings)}"
        reason_code = "failed"
    elif is_best_bucket_too_old:
        tone = "danger"
        status_text = "最新时间桶已过旧"
        summary_text = (
            f"当前最新时间桶 {best_bucket_key} 距现在约 {best_bucket_age_text}，已超过 3 小时。"
            if best_bucket_key and best_bucket_age_text
            else "当前最新共享文件已超过 3 小时，等待内网更新后会自动重试。"
        )
        reason_code = "stale_bucket"
    elif stale_buildings:
        tone = "danger"
        status_text = "存在过旧楼栋"
        summary_text = f"以下楼栋相对最新时间桶落后超过 3 桶：{' / '.join(stale_buildings)}"
        reason_code = "stale_buildings"
    elif missing_buildings:
        tone = "warning"
        status_text = "存在缺失楼栋"
        summary_text = f"以下楼栋尚未登记共享文件：{' / '.join(missing_buildings)}"
        reason_code = "missing"
    elif fallback_buildings:
        tone = "warning"
        status_text = "已允许回退"
        summary_text = f"以下楼栋正在使用上一版共享文件：{' / '.join(fallback_buildings)}"
        reason_code = "fallback"
    elif can_proceed and buildings:
        tone = "success"
        status_text = "最新桶已齐套"
        summary_text = f"{title or '共享文件'}当前都已命中最新共享文件，可直接继续处理。"
        reason_code = "ready"
    meta_lines: List[str] = []
    _push_meta_line(meta_lines, f"最新时间桶：{best_bucket_key or '-'}")
    if best_bucket_age_text:
        _push_meta_line(meta_lines, f"距当前约 {best_bucket_age_text}")
    if fallback_buildings:
        _push_meta_line(meta_lines, f"回退楼栋：{' / '.join(fallback_buildings)}")
    if failed_buildings:
        _push_meta_line(meta_lines, f"失败楼栋：{' / '.join(failed_buildings)}")
    if missing_buildings:
        _push_meta_line(meta_lines, f"缺失楼栋：{' / '.join(missing_buildings)}")
    if stale_buildings:
        _push_meta_line(meta_lines, f"过旧楼栋：{' / '.join(stale_buildings)}")
    items = [
        {
            "label": "最新时间桶",
            "value": best_bucket_key or "-",
            "tone": "info" if best_bucket_key else "neutral",
        },
        {
            "label": "回退楼栋",
            "value": f"{len(fallback_buildings)} 个",
            "tone": "warning" if fallback_buildings else "neutral",
        },
        {
            "label": "失败楼栋",
            "value": f"{len(failed_buildings)} 个",
            "tone": "danger" if failed_buildings else "neutral",
        },
        {
            "label": "缺失楼栋",
            "value": f"{len(missing_buildings)} 个",
            "tone": "danger" if missing_buildings else "neutral",
        },
        {
            "label": "过旧楼栋",
            "value": f"{len(stale_buildings)} 个",
            "tone": "danger" if stale_buildings or is_best_bucket_too_old else "neutral",
        },
    ]
    return {
        **selection,
        "key": key or _string(selection.get("key", "")),
        "title": title or _string(selection.get("title", "")),
        "best_bucket_key": best_bucket_key,
        "best_bucket_age_hours": best_bucket_age_value,
        "best_bucket_age_text": best_bucket_age_text,
        "is_best_bucket_too_old": is_best_bucket_too_old,
        "fallback_buildings": fallback_buildings,
        "missing_buildings": missing_buildings,
        "failed_buildings": failed_buildings,
        "stale_buildings": stale_buildings,
        "buildings": buildings,
        "can_proceed": can_proceed,
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "detail_text": summary_text,
        "reason_code": reason_code,
        "meta_lines": meta_lines,
        "items": items,
        "actions": {},
    }


def choose_external_shared_source_building_status(live_payload: Any, latest_payload: Any, *, fallback_bucket: str = "") -> Dict[str, Any]:
    live_meaningful = has_meaningful_source_cache_building_row(live_payload, fallback_bucket=fallback_bucket)
    latest_meaningful = has_meaningful_latest_selection_building(latest_payload, fallback_bucket=fallback_bucket)
    if live_meaningful:
        live = present_source_cache_building_row(live_payload, fallback_bucket=fallback_bucket)
        if latest_meaningful:
            latest = present_latest_selection_building(latest_payload, fallback_bucket=live.get("bucket_key", "") or fallback_bucket)
            merged = {
                **latest,
                **live,
                "using_fallback": bool(latest.get("using_fallback", False)),
                "version_gap": latest.get("version_gap", None),
            }
            meta_lines: List[str] = []
            _push_meta_line(meta_lines, f"时间桶：{_string(merged.get('bucket_key', '')) or _string(fallback_bucket) or '-'}")
            if bool(merged.get("using_fallback", False)) and merged.get("version_gap", None) is not None:
                _push_meta_line(meta_lines, f"较最新版本落后 {merged['version_gap']} 桶")
            if merged.get("last_error", ""):
                _push_meta_line(meta_lines, f"最近错误：{merged['last_error']}")
            else:
                _push_meta_line(meta_lines, f"最近成功：{_string(merged.get('downloaded_at', '')) or '-'}")
            if merged.get("resolved_file_path", ""):
                _push_meta_line(meta_lines, f"共享路径：{merged['resolved_file_path']}")
            elif merged.get("relative_path", ""):
                _push_meta_line(meta_lines, f"缓存文件：{merged['relative_path']}")
            elif _string(merged.get("status_key", "")) != "waiting":
                _push_meta_line(meta_lines, "共享文件未登记")
            merged["meta_lines"] = meta_lines
            return merged
        if not isinstance(live.get("meta_lines"), list) or not live.get("meta_lines"):
            live_meta_lines: List[str] = []
            _push_meta_line(live_meta_lines, f"时间桶：{_string(live.get('bucket_key', '')) or _string(fallback_bucket) or '-'}")
            if live.get("last_error", ""):
                _push_meta_line(live_meta_lines, f"最近错误：{live['last_error']}")
            else:
                _push_meta_line(live_meta_lines, f"最近成功：{_string(live.get('downloaded_at', '')) or '-'}")
            if live.get("resolved_file_path", ""):
                _push_meta_line(live_meta_lines, f"共享路径：{live['resolved_file_path']}")
            elif live.get("relative_path", ""):
                _push_meta_line(live_meta_lines, f"缓存文件：{live['relative_path']}")
            elif _string(live.get("status_key", "")) != "waiting":
                _push_meta_line(live_meta_lines, "共享文件未登记")
            live["meta_lines"] = live_meta_lines
        return live
    if latest_meaningful:
        return present_latest_selection_building(latest_payload, fallback_bucket=fallback_bucket)
    return present_latest_selection_building(
        {"building": _string((latest_payload or {}).get("building", "")) or _string((live_payload or {}).get("building", ""))},
        fallback_bucket=fallback_bucket,
    )


def present_external_source_cache_family(
    *,
    key: str,
    title: str,
    live_payload: Any,
    latest_payload: Any,
) -> Dict[str, Any]:
    live_family = present_source_cache_family(live_payload, title=title, bucket_scope_text="当前桶")
    latest_family = present_latest_selection_overview(latest_payload, key=key, title=title)
    ordered_buildings: List[str] = []
    for row in latest_family.get("buildings", []):
        building = _string(row.get("building", ""))
        if building and building not in ordered_buildings:
            ordered_buildings.append(building)
    for row in live_family.get("buildings", []):
        building = _string(row.get("building", ""))
        if building and building not in ordered_buildings:
            ordered_buildings.append(building)
    latest_map = {_string(item.get("building", "")): item for item in latest_family.get("buildings", []) if isinstance(item, dict)}
    live_map = {_string(item.get("building", "")): item for item in live_family.get("buildings", []) if isinstance(item, dict)}
    buildings = [
        choose_external_shared_source_building_status(
            live_map.get(building, {"building": building}),
            latest_map.get(building, {"building": building}),
            fallback_bucket=_string(latest_family.get("best_bucket_key", "")) or _string(live_family.get("current_bucket", "")),
        )
        for building in ordered_buildings
    ]
    has_failures = any(item.get("status_key") == "failed" for item in buildings)
    has_blocked = any(item.get("status_key") == "blocked" for item in buildings)
    has_downloading = any(item.get("status_key") == "downloading" for item in buildings)
    all_live_ready = bool(buildings) and all(item.get("status_key") == "ready" for item in buildings)
    live_ready_count = sum(1 for item in buildings if item.get("status_key") == "ready")
    live_downloading_count = sum(1 for item in buildings if item.get("status_key") == "downloading")
    live_failed_count = sum(1 for item in buildings if item.get("status_key") == "failed")
    live_blocked_count = sum(1 for item in buildings if item.get("status_key") == "blocked")
    live_problem_count = live_failed_count + live_blocked_count
    tone = _string(latest_family.get("tone", "")) or "warning"
    status_text = _string(latest_family.get("status_text", "")) or "等待共享文件就绪"
    summary_text = _string(latest_family.get("summary_text", "")) or "共享文件尚未齐套。"
    if has_failures:
        tone = "danger"
        status_text = "存在失败楼栋"
        summary_text = "部分楼栋共享文件同步失败，请检查共享目录和内网下载状态。"
    elif has_blocked:
        tone = "warning"
        status_text = "存在等待恢复楼栋"
        summary_text = "部分楼栋正在等待内网恢复，恢复后会自动继续刷新共享文件状态。"
    elif has_downloading:
        tone = "info"
        status_text = "共享文件同步中"
        summary_text = "部分楼栋正在下载或校验共享文件。"
    elif all_live_ready:
        tone = "success"
        status_text = "共享文件已就绪"
        summary_text = f"{title}当前已齐套。"
    is_date_semantic = key == "monthly_report_family"
    reference_label = "当前日期文件" if is_date_semantic else "最新时间桶"
    age_label = "距当前约"
    family_meta_lines: List[str] = []
    _push_meta_line(family_meta_lines, f"{reference_label}：{_string(latest_family.get('best_bucket_key', '')) or _string(live_family.get('current_bucket', '')) or '-'}")
    if _string(latest_family.get("best_bucket_age_text", "")):
        _push_meta_line(family_meta_lines, f"{age_label} {_string(latest_family.get('best_bucket_age_text', ''))}")
    for item in buildings:
        meta_lines = item.get("meta_lines", []) if isinstance(item.get("meta_lines", []), list) else []
        if is_date_semantic:
            rewritten: List[str] = []
            for line in meta_lines:
                text = _string(line)
                if text.startswith("时间桶："):
                    rewritten.append(text.replace("时间桶：", "日期文件：", 1))
                elif text.startswith("较最新版本落后 "):
                    rewritten.append(text.replace("较最新版本落后 ", "较当前日期文件落后 ", 1))
                else:
                    rewritten.append(text)
            item["meta_lines"] = rewritten
    family_items = [
        {
            "label": reference_label,
            "value": _string(latest_family.get("best_bucket_key", "")) or _string(live_family.get("current_bucket", "")) or "-",
            "tone": "info" if (_string(latest_family.get("best_bucket_key", "")) or _string(live_family.get("current_bucket", ""))) else "neutral",
        },
        {
            "label": "已就绪楼栋",
            "value": f"{live_ready_count}/{len(buildings) or 5}",
            "tone": "success" if live_ready_count and live_ready_count == (len(buildings) or 5) else ("info" if live_ready_count else "neutral"),
        },
        {
            "label": "下载中楼栋",
            "value": f"{live_downloading_count} 个",
            "tone": "info" if live_downloading_count else "neutral",
        },
        {
            "label": "异常楼栋",
            "value": f"{live_problem_count} 个",
            "tone": "danger" if live_failed_count else ("warning" if live_blocked_count else "neutral"),
        },
    ]
    return {
        **latest_family,
        "key": key,
        "title": title,
        "buildings": buildings,
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "detail_text": _string(latest_family.get("detail_text", "")) or summary_text,
        "live_ready_count": live_ready_count,
        "live_downloading_count": live_downloading_count,
        "live_failed_count": live_failed_count,
        "live_blocked_count": live_blocked_count,
        "reference_label": reference_label,
        "age_label": age_label,
        "building_reference_label": "日期文件" if is_date_semantic else "时间桶",
        "date_semantic": is_date_semantic,
        "meta_lines": family_meta_lines,
        "items": family_items,
    }


def present_internal_page_slot(payload: Any) -> Dict[str, Any]:
    slot = payload if isinstance(payload, dict) else {}
    page_ready = bool(slot.get("page_ready", False))
    browser_ready = bool(slot.get("browser_ready", False))
    in_use = bool(slot.get("in_use", False))
    suspended = bool(slot.get("suspended", False))
    last_result = _string(slot.get("last_result", "")).lower()
    login_state = _string(slot.get("login_state", "")).lower()
    last_error = bridge_text(slot.get("last_error", ""))
    login_error = bridge_text(slot.get("login_error", ""))
    suspend_reason = bridge_text(slot.get("suspend_reason", "") or slot.get("pending_issue_summary", ""))
    next_probe_at = _string(slot.get("next_probe_at", ""))

    tone = "neutral"
    status_key = "waiting"
    status_text = "未建页"
    detail_text = "页签尚未初始化"
    if (page_ready or login_state == "ready" or last_result == "ready") and not in_use:
        tone = "success"
        status_key = "ready"
        status_text = "待命"
        detail_text = "登录态已就绪，等待下载任务"
        if page_ready:
            detail_text = "页签已就绪，等待下载任务"
    if in_use or last_result == "running":
        tone = "warning"
        status_key = "in_use"
        status_text = "使用中"
        detail_text = "当前楼栋正在执行下载或查询任务"
    if suspended:
        tone = "danger"
        status_key = "suspended"
        status_text = "已暂停等待恢复"
        detail_text = suspend_reason or "该楼已暂停等待恢复"
        if next_probe_at:
            detail_text += f" / 下次自动检测：{next_probe_at}"
    elif last_result in {"failed", "error"}:
        tone = "danger"
        status_key = "failed"
        status_text = "最近失败"
        detail_text = last_error or login_error or "最近一次任务失败"
    elif last_result == "success":
        tone = "success"
        status_key = "ready"
        status_text = "最近成功"
        detail_text = _string(slot.get("last_used_at", "")) and f"最近使用：{_string(slot.get('last_used_at', ''))}" or "最近一次任务成功"
    login_tone = "warning"
    login_text = "待登录"
    if suspended:
        login_tone = "danger"
        login_text = "页面异常"
    elif login_state == "ready":
        login_tone = "success"
        login_text = "已登录"
    elif login_state == "logging_in":
        login_tone = "info"
        login_text = "登录中"
        if status_key not in {"in_use", "suspended"}:
            tone = "info"
            status_key = "prelogin"
            status_text = "预登录中"
            detail_text = "正在检查登录态并准备进入目标页面"
    elif login_state == "expired":
        login_tone = "warning"
        login_text = "登录已失效"
        if status_key not in {"in_use", "suspended"}:
            detail_text = "登录态已失效，任务开始前会自动重登"
    elif login_state == "failed":
        login_tone = "danger"
        login_text = "登录失败"
        if status_key not in {"in_use", "suspended"}:
            tone = "danger"
            status_key = "failed"
            status_text = "登录失败"
            detail_text = login_error or last_error or "登录失败，请检查楼栋地址、网络和登录页状态"
    return {
        **slot,
        "browser_ready": browser_ready,
        "page_ready": page_ready,
        "in_use": in_use,
        "last_result": last_result,
        "login_state": login_state,
        "last_error": last_error,
        "login_error": login_error,
        "suspend_reason": suspend_reason,
        "next_probe_at": next_probe_at,
        "status_key": status_key,
        "tone": tone,
        "status_text": status_text,
        "detail_text": detail_text,
        "login_tone": login_tone,
        "login_text": login_text,
    }


def present_internal_download_pool_overview(payload: Any) -> Dict[str, Any]:
    pool = payload if isinstance(payload, dict) else {}
    enabled = bool(pool.get("enabled", False))
    browser_ready = bool(pool.get("browser_ready", False))
    last_error = bridge_text(pool.get("last_error", ""))
    raw_slots = pool.get("page_slots", []) if isinstance(pool.get("page_slots", []), list) else []
    slot_map = {
        _string(item.get("building", "")): present_internal_page_slot(item)
        for item in raw_slots
        if isinstance(item, dict) and _string(item.get("building", ""))
    }
    slots = [slot_map.get(building, present_internal_page_slot({"building": building})) for building in INTERNAL_BUILDINGS]
    active_buildings = [
        _string(item) for item in (pool.get("active_buildings", []) if isinstance(pool.get("active_buildings", []), list) else [])
        if _string(item)
    ]
    ready_login_count = sum(1 for item in slots if _string(item.get("login_state", "")).lower() == "ready")

    tone = "warning"
    status_text = "启动中"
    summary_text = "内网下载页池正在准备浏览器和固定楼栋页签。"
    if not enabled:
        tone = "warning"
        status_text = "未启用"
        summary_text = "当前内网端尚未启用常驻下载页池。"
    elif browser_ready:
        if active_buildings:
            tone = "warning"
            status_text = "运行中 / 有页签占用"
            summary_text = f"当前占用楼栋：{' / '.join(active_buildings)}"
        elif ready_login_count == len(slots) and slots:
            tone = "success"
            status_text = "运行中 / 5个楼已登录"
            summary_text = "5个楼状态实时展示，下载前会先刷新，只有登录失效时才重新登录。"
        else:
            tone = "warning"
            status_text = "运行中 / 预登录进行中"
            summary_text = f"5个楼状态实时展示中，已登录 {ready_login_count}/{len(slots) or 5}，收到相关事件会即时刷新，并保留10秒兜底刷新。"
    elif last_error:
        tone = "danger"
        status_text = "页池异常"
        summary_text = "内网下载页池启动失败或最近一次重建异常，请检查 Playwright 环境和登录页。"

    return {
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "error_text": last_error,
        "slots": slots,
        "items": [
            {
                "label": "页池状态",
                "value": "浏览器已就绪" if browser_ready else ("浏览器未就绪" if enabled else "未启用"),
                "tone": "success" if browser_ready else ("warning" if enabled else "neutral"),
            },
            {
                "label": "当前占用",
                "value": " / ".join(active_buildings) if active_buildings else "无",
                "tone": "warning" if active_buildings else "neutral",
            },
            {
                "label": "已登录楼栋",
                "value": f"{ready_login_count}/{len(slots) or 5}",
                "tone": "success" if ready_login_count == len(slots) and slots else ("warning" if ready_login_count > 0 else "neutral"),
            },
        ],
    }


def present_internal_source_cache_overview(payload: Any) -> Dict[str, Any]:
    source_cache = payload if isinstance(payload, dict) else {}
    enabled = bool(source_cache.get("enabled", False))
    scheduler_running = bool(source_cache.get("scheduler_running", False))
    current_hour_bucket = _string(source_cache.get("current_hour_bucket", ""))
    last_run_at = _string(source_cache.get("last_run_at", ""))
    last_success_at = _string(source_cache.get("last_success_at", ""))
    error_text = bridge_text(source_cache.get("last_error", ""))
    cache_root = _string(source_cache.get("cache_root", ""))

    handover_family = present_source_cache_family(
        source_cache.get("handover_log_family", {}),
        key="handover_log_family",
        title="交接班日志源文件",
        fallback_bucket=current_hour_bucket,
        bucket_scope_text="本小时",
    )
    handover_capacity_family = present_source_cache_family(
        source_cache.get("handover_capacity_report_family", {}),
        key="handover_capacity_report_family",
        title="交接班容量报表源文件",
        fallback_bucket=current_hour_bucket,
        bucket_scope_text="本小时",
    )
    monthly_family = present_source_cache_family(
        source_cache.get("monthly_report_family", {}),
        key="monthly_report_family",
        title="全景平台月报源文件",
        fallback_bucket=current_hour_bucket,
        bucket_scope_text="本小时",
    )
    alarm_bucket = _string(source_cache.get("alarm_event_family", {}).get("current_bucket", "")) or current_hour_bucket
    alarm_family = present_source_cache_family(
        source_cache.get("alarm_event_family", {}),
        key="alarm_event_family",
        title="告警信息源文件",
        fallback_bucket=alarm_bucket,
        bucket_scope_text="本次定时",
    )
    branch_power_bucket = _string(source_cache.get("branch_power_family", {}).get("current_bucket", "")) or current_hour_bucket
    branch_power_family = present_source_cache_family(
        source_cache.get("branch_power_family", {}),
        key="branch_power_family",
        title="支路功率源文件",
        fallback_bucket=branch_power_bucket,
        bucket_scope_text="本小时",
    )
    branch_current_bucket = _string(source_cache.get("branch_current_family", {}).get("current_bucket", "")) or current_hour_bucket
    branch_current_family = present_source_cache_family(
        source_cache.get("branch_current_family", {}),
        key="branch_current_family",
        title="支路电流源文件",
        fallback_bucket=branch_current_bucket,
        bucket_scope_text="本小时",
    )
    branch_switch_bucket = _string(source_cache.get("branch_switch_family", {}).get("current_bucket", "")) or branch_power_bucket
    branch_switch_family = present_source_cache_family(
        source_cache.get("branch_switch_family", {}),
        key="branch_switch_family",
        title="支路开关源文件",
        fallback_bucket=branch_switch_bucket,
        bucket_scope_text="本小时",
    )
    families = [
        handover_family,
        handover_capacity_family,
        monthly_family,
        branch_power_family,
        branch_current_family,
        branch_switch_family,
        alarm_family,
    ]

    tone = "warning"
    status_text = "准备中"
    summary_text = "内网端会维护七组共享源文件：交接班日志源文件、交接班容量报表源文件、支路功率源文件、支路电流源文件、支路开关源文件、全景平台月报源文件，以及按策略拉取的告警信息源文件。"
    if not enabled:
        tone = "warning"
        status_text = "未启用"
        summary_text = "当前未启用共享缓存仓。"
    elif any(bool(item.get("has_failures", False)) or bool(item.get("has_blocked", False)) for item in families) or (not last_success_at and error_text):
        tone = "danger"
        status_text = "最近一轮存在失败"
        summary_text = "最近一轮共享文件同步存在失败楼栋，请检查共享目录权限和内网页面登录状态。"
    elif families and all(bool(item.get("all_ready", False)) and bool(item.get("buildings")) for item in families):
        tone = "success"
        status_text = "本轮共享文件已全部就绪"
        summary_text = "交接班、容量报表、支路功率、支路电流、支路开关、月报和告警信息七组共享文件都已就绪。"
    elif scheduler_running:
        tone = "warning"
        status_text = "运行中"
        summary_text = "共享缓存仓正在维护交接班、容量报表、支路功率、支路电流、支路开关、月报和最近应执行的告警信息文件。"
    reason_code = "waiting"
    if not enabled:
        reason_code = "disabled"
    elif any(bool(item.get("has_failures", False)) or bool(item.get("has_blocked", False)) for item in families) or (not last_success_at and error_text):
        reason_code = "failed"
    elif families and all(bool(item.get("all_ready", False)) and bool(item.get("buildings")) for item in families):
        reason_code = "ready"
    elif scheduler_running:
        reason_code = "running"

    return {
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "detail_text": error_text or summary_text,
        "reason_code": reason_code,
        "current_hour_bucket": current_hour_bucket or "-",
        "last_run_at": last_run_at,
        "last_success_at": last_success_at,
        "error_text": error_text,
        "cache_root": cache_root,
        "families": families,
        "items": [
            {
                "label": "当前小时桶",
                "value": current_hour_bucket or "-",
                "tone": "info" if current_hour_bucket else "neutral",
            },
            {
                "label": "最近成功时间",
                "value": last_success_at or "-",
                "tone": "success" if last_success_at else "neutral",
            },
            {
                "label": "最近调度时间",
                "value": last_run_at or "-",
                "tone": "info" if last_run_at else "neutral",
            },
        ],
    }


def present_current_hour_refresh_overview(payload: Any) -> Dict[str, Any]:
    refresh = payload if isinstance(payload, dict) else {}
    running = bool(refresh.get("running", False))
    last_run_at = _string(refresh.get("last_run_at", ""))
    last_success_at = _string(refresh.get("last_success_at", ""))
    last_error = bridge_text(refresh.get("last_error", ""))
    failed_buildings = [_string(item) for item in (refresh.get("failed_buildings", []) if isinstance(refresh.get("failed_buildings", []), list) else []) if _string(item)]
    blocked_buildings = [_string(item) for item in (refresh.get("blocked_buildings", []) if isinstance(refresh.get("blocked_buildings", []), list) else []) if _string(item)]
    running_buildings = [_string(item) for item in (refresh.get("running_buildings", []) if isinstance(refresh.get("running_buildings", []), list) else []) if _string(item)]
    completed_buildings = [_string(item) for item in (refresh.get("completed_buildings", []) if isinstance(refresh.get("completed_buildings", []), list) else []) if _string(item)]

    tone = "neutral"
    status_text = "尚未触发"
    summary_text = ""
    reason_code = "idle"
    if running:
        tone = "info"
        status_text = "下载中"
        summary_text = "当前小时共享文件下载正在执行。"
        reason_code = "running"
    elif last_error or failed_buildings:
        tone = "danger"
        status_text = "最近存在失败"
        summary_text = "最近一次当前小时下载存在失败楼栋。"
        reason_code = "failed"
    elif last_success_at:
        tone = "success"
        status_text = "最近下载成功"
        summary_text = "最近一次当前小时下载已完成。"
        reason_code = "ready"
    elif last_run_at:
        tone = "warning"
        status_text = "最近已触发"
        summary_text = "当前小时下载最近已触发，等待结果。"
        reason_code = "pending"

    return {
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "detail_text": last_error or summary_text,
        "reason_code": reason_code,
        "last_run_at": last_run_at,
        "last_success_at": last_success_at,
        "last_error": last_error,
        "failed_buildings": failed_buildings,
        "blocked_buildings": blocked_buildings,
        "running_buildings": running_buildings,
        "completed_buildings": completed_buildings,
        "items": [
            {
                "label": "最近触发",
                "value": last_run_at or "-",
                "tone": "info" if last_run_at else "neutral",
            },
            {
                "label": "最近成功",
                "value": last_success_at or "-",
                "tone": "success" if last_success_at else "neutral",
            },
            {
                "label": "进行中楼栋",
                "value": " / ".join(running_buildings) if running_buildings else "无",
                "tone": "info" if running_buildings else "neutral",
            },
            {
                "label": "失败楼栋",
                "value": " / ".join(failed_buildings) if failed_buildings else "无",
                "tone": "danger" if failed_buildings else "neutral",
            },
        ],
        "actions": {
            "refresh_current_hour": {
                "allowed": not running,
                "pending": running,
                "label": "刷新当前小时共享文件",
                "disabled_reason": "当前小时共享文件正在下载" if running else "",
            }
        },
    }


def present_external_source_cache_overview(payload: Any) -> Dict[str, Any]:
    source_cache = payload if isinstance(payload, dict) else {}
    error_text = bridge_text(source_cache.get("last_error", ""))
    families = []
    gating_families = []
    for key, title in (
        ("handover_log_family", "交接班日志源文件"),
        ("handover_capacity_report_family", "交接班容量报表源文件"),
        ("branch_power_family", "支路功率源文件"),
        ("branch_current_family", "支路电流源文件"),
        ("branch_switch_family", "支路开关源文件"),
        ("monthly_report_family", "全景平台月报源文件"),
    ):
        family_payload = source_cache.get(key, {}) if isinstance(source_cache.get(key, {}), dict) else {}
        display_family = family_payload.get("display_overview", {})
        if not isinstance(display_family, dict) or not display_family:
            latest_payload = family_payload.get("latest_selection", {})
            if not isinstance(latest_payload, dict):
                latest_payload = {}
            display_family = present_external_source_cache_family(
                key=key,
                title=title,
                live_payload=family_payload,
                latest_payload=latest_payload,
            )
        if key in {"handover_log_family", "monthly_report_family"}:
            latest_payload = family_payload.get("latest_selection", {})
            if not isinstance(latest_payload, dict):
                latest_payload = {}
            gating_families.append(
                present_latest_selection_overview(
                    latest_payload,
                    key=key,
                    title=title,
                )
            )
        families.append(display_family)
    alarm_payload = source_cache.get("alarm_event_family", {}) if isinstance(source_cache.get("alarm_event_family", {}), dict) else {}
    alarm_display = alarm_payload.get("display_overview", {})
    if not isinstance(alarm_display, dict) or not alarm_display:
        alarm_display = present_alarm_event_family(
            alarm_payload,
            key="alarm_event_family",
            title="告警信息源文件",
        )
    families.append(alarm_display)

    has_stale = any(bool(item.get("stale_buildings", [])) for item in gating_families)
    has_too_old = any(bool(item.get("is_best_bucket_too_old", False)) for item in gating_families)
    has_failed = any(bool(item.get("failed_buildings", [])) for item in gating_families)
    has_missing = any(bool(item.get("missing_buildings", [])) for item in gating_families)
    has_fallback = any(bool(item.get("fallback_buildings", [])) for item in gating_families)
    all_ready = bool(gating_families) and all(bool(item.get("can_proceed", False)) and bool(item.get("buildings")) for item in gating_families)
    reference_bucket_key = max((_string(item.get("best_bucket_key", "")) for item in gating_families if _string(item.get("best_bucket_key", ""))), default="-")

    family_can_proceed = {
        _string(item.get("key", "")): bool(item.get("can_proceed", False))
        for item in gating_families
        if _string(item.get("key", ""))
    }
    family_retry_signatures = {
        _string(item.get("key", "")): "=".join(
            [
                _string(item.get("best_bucket_key", "")),
                "|".join(
                    ":".join(
                        [
                            _string(building.get("building", "")),
                            _string(building.get("status_key", "")),
                            _string(building.get("bucket_key", "")),
                            _string(building.get("version_gap", "")),
                            "1" if bool(building.get("using_fallback", False)) else "0",
                        ]
                    )
                    for building in (item.get("buildings", []) if isinstance(item.get("buildings", []), list) else [])
                    if isinstance(building, dict)
                ),
                "1" if bool(item.get("is_best_bucket_too_old", False)) else "0",
                _string(item.get("best_bucket_age_text", "")),
            ]
        )
        for item in gating_families
        if _string(item.get("key", ""))
    }
    auto_retry_signature = "||".join(
        f"{key}={value}" for key, value in family_retry_signatures.items()
    )

    tone = "warning"
    status_text = "等待共享文件就绪"
    summary_text = "外网默认入口继续只依赖交接班日志源文件与全景平台月报源文件；支路功率源文件在专项任务中使用。"
    if has_failed:
        tone = "danger"
        status_text = "共享文件同步失败"
        summary_text = "部分楼栋共享文件同步失败，请先处理内网下载状态。"
    elif has_too_old:
        tone = "danger"
        status_text = "等待共享文件更新"
        summary_text = "当前共享参考文件整体已超过 3 小时，等待内网更新后会自动重试默认入口。"
    elif has_stale:
        tone = "danger"
        status_text = "等待共享文件就绪"
        summary_text = "部分楼栋共享文件版本过旧，等待更新后会自动重试默认入口。"
    elif has_missing:
        tone = "warning"
        status_text = "等待共享文件就绪"
        summary_text = "部分楼栋共享文件缺失，等待补齐后会自动重试默认入口。"
    elif has_fallback:
        tone = "warning"
        status_text = "等待共享文件就绪"
        summary_text = "当前允许部分楼栋回退到不超过 3 桶的上一版共享文件。"
    elif all_ready:
        tone = "success"
        status_text = "共享文件已就绪"
        summary_text = "外网默认入口继续只依赖交接班日志源文件与全景平台月报源文件；支路功率源文件在专项任务中使用。"
    reason_code = "waiting"
    if has_failed:
        reason_code = "failed"
    elif has_too_old:
        reason_code = "too_old"
    elif has_stale:
        reason_code = "stale"
    elif has_missing:
        reason_code = "missing"
    elif has_fallback:
        reason_code = "fallback"
    elif all_ready:
        reason_code = "ready"
    overview_items = [
        {
            "label": "主流程判断",
            "value": "可继续" if all_ready else "需等待",
            "tone": "success" if all_ready else tone,
        },
        {
            "label": "显示文件类型",
            "value": str(len(families)),
            "tone": "neutral",
        },
        {
            "label": "共享参考标识",
            "value": reference_bucket_key or "-",
            "tone": "info" if reference_bucket_key and reference_bucket_key != "-" else "neutral",
        },
    ]

    return {
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "detail_text": error_text or summary_text,
        "reason_code": reason_code,
            "display_note_text": "交接班容量报表源文件、支路功率源文件、支路电流源文件和支路开关源文件仅在状态页同步展示，不单独阻断外网默认流程。",
        "reference_bucket_key": reference_bucket_key,
        "error_text": error_text,
        "families": families,
        "items": overview_items,
        "can_proceed_latest": all_ready,
        "auto_retry_signature": auto_retry_signature,
        "family_can_proceed": family_can_proceed,
        "family_retry_signatures": family_retry_signatures,
    }


def _is_bridge_terminal_status(status: Any) -> bool:
    normalized = _string(status).lower()
    return normalized in {"success", "failed", "partial_failed", "cancelled", "stale"}


def _latest_bridge_task_event(task: Any) -> Dict[str, Any]:
    events = task.get("events", []) if isinstance(task, dict) and isinstance(task.get("events", []), list) else []
    for item in events:
        if isinstance(item, dict):
            return item
    return {}


def _is_shared_source_cache_backfill_waiting_sync(task: Any) -> bool:
    if not isinstance(task, dict):
        return False
    normalized_status = _string(task.get("status", "")).lower()
    if normalized_status not in {"ready_for_external", "waiting_next_side"}:
        return False
    latest_event = _latest_bridge_task_event(task)
    latest_event_type = _string(latest_event.get("event_type", "")).lower()
    if latest_event_type == "waiting_source_sync":
        return True
    latest_event_text = _string(latest_event.get("event_text", "") or ((latest_event.get("payload", {}) if isinstance(latest_event.get("payload", {}), dict) else {}).get("message", "")))
    return "等待内网补采同步" in latest_event_text


def _bridge_progress_status_text(status_or_task: Any, task_like: Any = None) -> str:
    task = status_or_task if isinstance(status_or_task, dict) else (task_like if isinstance(task_like, dict) else None)
    normalized = _string(task.get("status", "")) if isinstance(task, dict) else _string(status_or_task)
    normalized = normalized.lower()
    if not normalized:
        return "执行中"
    if normalized in {"internal_running", "external_running", "running", "claimed", "internal_claimed", "external_claimed"}:
        return "执行中"
    if normalized in {"queued_for_internal", "pending"}:
        return "等待执行"
    if normalized in {"ready_for_external", "waiting_next_side"}:
        return "等待内网补采同步" if _is_shared_source_cache_backfill_waiting_sync(task) else "等待接续"
    if normalized == "success":
        return "已完成"
    if normalized == "failed":
        return "失败"
    if normalized == "partial_failed":
        return "部分失败"
    return _string(task.get("status", "")) if isinstance(task, dict) else _string(status_or_task) or "执行中"


def _shared_source_cache_backfill_stage_text(task: Any) -> str:
    stage_name = _string((task or {}).get("current_stage_name", "")) if isinstance(task, dict) else ""
    feature_label = _string((task or {}).get("feature_label", "")) if isinstance(task, dict) else ""
    if not feature_label:
        feature = _string((task or {}).get("feature", "")) if isinstance(task, dict) else ""
        feature_label = {
            "handover_cache_fill": "交接班历史共享文件补采",
            "monthly_cache_fill": "月报历史共享文件补采",
        }.get(feature.lower(), "内外网同步任务")
    status_text = _bridge_progress_status_text(
        (task or {}).get("current_stage_status", "") if isinstance(task, dict) else "",
        task,
    )
    return f"{stage_name} / {status_text}" if stage_name else f"{feature_label} / {status_text}"


def _shared_source_cache_shift_text(value: Any) -> str:
    normalized = _string(value).lower()
    if normalized == "day":
        return "白班"
    if normalized == "night":
        return "夜班"
    return "-"


def _normalize_shared_source_cache_task_buildings(request_payload: Any) -> List[str]:
    request = request_payload if isinstance(request_payload, dict) else {}
    single_building = _string(request.get("building", ""))
    if single_building:
        return [single_building]
    return [
        _string(item)
        for item in (request.get("buildings", []) if isinstance(request.get("buildings", []), list) else [])
        if _string(item)
    ]


def _shared_source_cache_backfill_scope_text(task: Any) -> str:
    feature = _string((task or {}).get("feature", "")).lower() if isinstance(task, dict) else ""
    request = (task or {}).get("request", {}) if isinstance(task, dict) and isinstance((task or {}).get("request", {}), dict) else {}
    if feature == "handover_cache_fill":
        duty_date = _string(request.get("duty_date", ""))
        duty_shift_text = _shared_source_cache_shift_text(request.get("duty_shift", ""))
        if duty_date and duty_shift_text != "-":
            return f"{duty_date} / {duty_shift_text}"
        selected_dates = [
            _string(item)
            for item in (request.get("selected_dates", []) if isinstance(request.get("selected_dates", []), list) else [])
            if _string(item)
        ]
        if selected_dates:
            return f"日期 {' / '.join(selected_dates)}"
        return ""
    if feature == "monthly_cache_fill":
        selected_dates = [
            _string(item)
            for item in (request.get("selected_dates", []) if isinstance(request.get("selected_dates", []), list) else [])
            if _string(item)
        ]
        return " / ".join(selected_dates)
    return ""


def _build_shared_source_cache_backfill_overlays(tasks: Any) -> List[Dict[str, Any]]:
    normalized_tasks = tasks if isinstance(tasks, list) else []
    overlays: List[Dict[str, Any]] = []
    for task in normalized_tasks:
        if not isinstance(task, dict) or _is_bridge_terminal_status(task.get("status", "")):
            continue
        feature = _string(task.get("feature", "")).lower()
        request = task.get("request", {}) if isinstance(task.get("request", {}), dict) else {}
        requested_buildings = _normalize_shared_source_cache_task_buildings(request)
        base_overlay = {
            "task_id": _string(task.get("task_id", "")),
            "requested_buildings": requested_buildings,
            "stage_text": _shared_source_cache_backfill_stage_text(task),
            "scope_text": _shared_source_cache_backfill_scope_text(task),
        }
        if feature == "handover_cache_fill" and _string(request.get("continuation_kind", "")).lower() == "handover":
            overlays.append({**base_overlay, "family_key": "handover_log_family"})
            overlays.append({**base_overlay, "family_key": "handover_capacity_report_family"})
        elif feature == "monthly_cache_fill":
            overlays.append({**base_overlay, "family_key": "monthly_report_family"})
    return overlays


def apply_external_source_cache_backfill_overlays(overview: Any, tasks: Any) -> Dict[str, Any]:
    payload = copy.deepcopy(overview if isinstance(overview, dict) else {})
    families = payload.get("families", []) if isinstance(payload.get("families", []), list) else []
    overlays = _build_shared_source_cache_backfill_overlays(tasks)
    if not families or not overlays:
        return payload
    updated_families: List[Dict[str, Any]] = []
    for family in families:
        family_payload = copy.deepcopy(family if isinstance(family, dict) else {})
        family_key = _string(family_payload.get("key", "")).lower()
        if not family_key or family_key == "alarm_event_family":
            updated_families.append(family_payload)
            continue
        family_overlays = [item for item in overlays if _string(item.get("family_key", "")).lower() == family_key]
        if not family_overlays:
            family_payload["backfill_running"] = False
            family_payload["backfill_text"] = ""
            family_payload["backfill_scope_text"] = ""
            family_payload["backfill_task_id"] = ""
            updated_families.append(family_payload)
            continue
        is_monthly = family_key == "monthly_report_family"
        backfill_label = "当前同步" if is_monthly else "当前补采"
        backfill_scope_label = "同步日期" if is_monthly else "补采范围"
        family_payload["backfill_label"] = backfill_label
        family_payload["backfill_scope_label"] = backfill_scope_label
        family_payload["backfill_running"] = True
        family_payload["backfill_text"] = _string(family_overlays[0].get("stage_text", ""))
        family_payload["backfill_scope_text"] = _string(family_overlays[0].get("scope_text", ""))
        family_payload["backfill_task_id"] = _string(family_overlays[0].get("task_id", ""))
        family_payload["tone"] = "warning"
        family_payload["status_text"] = "同步中" if is_monthly else "补采中"
        family_payload["summary_text"] = (
            "月报日期文件同步中；如外网任务显示“等待内网补采同步”，文件到位后会自动继续并回切为已就绪。"
            if is_monthly
            else "历史共享文件补采中；如外网任务显示“等待内网补采同步”，文件到位后会自动继续并回切为已就绪。"
        )
        family_meta_lines = family_payload.get("meta_lines", []) if isinstance(family_payload.get("meta_lines", []), list) else []
        _push_meta_line(family_meta_lines, f"{backfill_label}：{family_payload['backfill_text']}")
        _push_meta_line(family_meta_lines, f"{backfill_scope_label}：{family_payload['backfill_scope_text']}")
        family_payload["meta_lines"] = family_meta_lines
        buildings = family_payload.get("buildings", []) if isinstance(family_payload.get("buildings", []), list) else []
        running_building_count = 0
        updated_buildings: List[Dict[str, Any]] = []
        for building in buildings:
            row = copy.deepcopy(building if isinstance(building, dict) else {})
            building_name = _string(row.get("building", ""))
            if not building_name or _string(row.get("status_key", row.get("status", ""))).lower() == "ready":
                row["backfill_running"] = False
                row["backfill_text"] = ""
                row["backfill_scope_text"] = ""
                row["backfill_task_id"] = ""
                updated_buildings.append(row)
                continue
            overlay = next(
                (
                    item for item in family_overlays
                    if not item.get("requested_buildings") or building_name in item.get("requested_buildings", [])
                ),
                None,
            )
            if not overlay:
                row["backfill_running"] = False
                row["backfill_text"] = ""
                row["backfill_scope_text"] = ""
                row["backfill_task_id"] = ""
                updated_buildings.append(row)
                continue
            running_building_count += 1
            row["tone"] = "warning"
            row["status_text"] = "补采中" if not is_monthly else "同步中"
            row["backfill_running"] = True
            row["backfill_text"] = _string(overlay.get("stage_text", ""))
            row["backfill_scope_text"] = _string(overlay.get("scope_text", ""))
            row["backfill_task_id"] = _string(overlay.get("task_id", ""))
            meta_lines = row.get("meta_lines", []) if isinstance(row.get("meta_lines", []), list) else []
            _push_meta_line(meta_lines, f"{backfill_label}：{row['backfill_text']}")
            _push_meta_line(meta_lines, f"{backfill_scope_label}：{row['backfill_scope_text']}")
            row["meta_lines"] = meta_lines
            updated_buildings.append(row)
        family_payload["buildings"] = updated_buildings
        if buildings and running_building_count <= 0:
            family_payload["backfill_running"] = False
            family_payload["backfill_text"] = ""
            family_payload["backfill_scope_text"] = ""
            family_payload["backfill_task_id"] = ""
        updated_families.append(family_payload)
    payload["families"] = updated_families
    return payload


def present_external_internal_alert_overview(payload: Any) -> Dict[str, Any]:
    status = payload if isinstance(payload, dict) else {}
    raw_buildings = status.get("buildings", []) if isinstance(status.get("buildings", []), list) else []
    building_map = {
        _string(item.get("building", "")): item
        for item in raw_buildings
        if isinstance(item, dict) and _string(item.get("building", ""))
    }
    buildings = []
    active_building_count = 0
    for building in INTERNAL_BUILDINGS:
        raw = building_map.get(building, {})
        raw_status = _string(raw.get("status", "")).lower()
        last_problem_at = _string(raw.get("last_problem_at", ""))
        last_recovered_at = _string(raw.get("last_recovered_at", ""))
        active_count = int(raw.get("active_count", 0) or 0)
        if raw_status == "problem" or active_count > 0:
            active_building_count += 1
            buildings.append(
                {
                    "building": building,
                    "tone": "danger",
                    "status_text": "异常",
                    "summary_text": _string(raw.get("summary", "")) or "存在内网异常告警",
                    "detail_text": _string(raw.get("detail", "")),
                    "time_text": f"最近告警：{last_problem_at}" if last_problem_at else "",
                    "active_count": active_count,
                }
            )
            continue
        buildings.append(
            {
                "building": building,
                "tone": "success",
                "status_text": "正常",
                "summary_text": "已恢复正常" if last_recovered_at else "正常",
                "detail_text": "",
                "time_text": f"最近恢复：{last_recovered_at}" if last_recovered_at else "",
                "active_count": 0,
            }
        )
    last_notified_at = _string(status.get("last_notified_at", ""))
    tone = "danger" if active_building_count > 0 else "success"
    status_text = "存在异常楼栋" if active_building_count > 0 else "5楼均正常"
    summary_text = (
        f"当前有 {active_building_count} 个楼栋存在未恢复的内网告警。"
        if active_building_count > 0
        else "当前未收到内网异常告警，5 个楼均显示正常。"
    )
    return {
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "items": [
            {
                "label": "异常楼栋",
                "value": f"{active_building_count}/5",
                "tone": tone,
            },
            {
                "label": "最近告警同步",
                "value": last_notified_at or "-",
                "tone": "info" if last_notified_at else "neutral",
            },
        ],
        "buildings": buildings,
    }


def present_alarm_event_building(payload: Any, *, fallback_bucket: str = "") -> Dict[str, Any]:
    row = payload if isinstance(payload, dict) else {}
    blocked = bool(row.get("blocked", False))
    blocked_reason = bridge_text(row.get("blocked_reason", "") or row.get("last_error", ""))
    raw_status = _string(row.get("status", "")).lower() or _string(row.get("status_key", "")).lower()
    status_key = raw_status if raw_status in {"ready", "failed"} else ("blocked" if blocked else "waiting")
    tone = "warning"
    status_text = "今天和昨天都缺文件"
    if status_key == "ready":
        tone = "success"
        status_text = "已就绪"
    elif status_key == "failed":
        tone = "danger"
        status_text = "失败"
    elif status_key == "blocked":
        tone = "warning"
        status_text = "等待内网恢复"
    source_kind = _string(row.get("source_kind", "")).lower()
    selection_scope = _string(row.get("selection_scope", "")).lower()
    downloaded_at = _string(row.get("downloaded_at", ""))
    selected_downloaded_at = _string(row.get("selected_downloaded_at", "")) or downloaded_at
    meta_lines: List[str] = []
    _push_meta_line(meta_lines, f"来源：{('手动' if source_kind == 'manual' else ('定时' if source_kind == 'latest' else '-'))}")
    _push_meta_line(meta_lines, f"选择：{('今天最新' if selection_scope == 'today' else ('昨天回退' if selection_scope == 'yesterday_fallback' else ('今天和昨天都缺文件' if selection_scope == 'missing' else '-')))}")
    _push_meta_line(meta_lines, f"选中文件时间：{selected_downloaded_at or '-'}")
    if blocked and blocked_reason:
        _push_meta_line(meta_lines, f"最近错误：{blocked_reason}")
    elif bridge_text(row.get("last_error", "")):
        _push_meta_line(meta_lines, f"最近错误：{bridge_text(row.get('last_error', ''))}")
    elif selected_downloaded_at:
        _push_meta_line(meta_lines, f"最近成功：{selected_downloaded_at}")
    if _string(row.get("resolved_file_path", "")):
        _push_meta_line(meta_lines, f"共享路径：{_string(row.get('resolved_file_path', ''))}")
    elif _string(row.get("relative_path", "")):
        _push_meta_line(meta_lines, f"缓存文件：{_string(row.get('relative_path', ''))}")
    return {
        **row,
        "building": _string(row.get("building", "")) or "-",
        "bucket_key": _string(row.get("bucket_key", "")) or _string(fallback_bucket) or "-",
        "status_key": status_key,
        "tone": tone,
        "status_text": status_text,
        "downloaded_at": downloaded_at,
        "selected_downloaded_at": selected_downloaded_at,
        "last_error": bridge_text(row.get("last_error", "")),
        "relative_path": _string(row.get("relative_path", "")),
        "resolved_file_path": _string(row.get("resolved_file_path", "")),
        "blocked": blocked,
        "blocked_reason": blocked_reason,
        "source_kind": source_kind,
        "source_kind_text": "手动" if source_kind == "manual" else ("定时" if source_kind == "latest" else ""),
        "selection_scope": selection_scope,
        "selection_scope_text": "今天最新" if selection_scope == "today" else ("昨天回退" if selection_scope == "yesterday_fallback" else ("今天和昨天都缺文件" if selection_scope == "missing" else "")),
        "detail_text": blocked_reason if blocked else (bridge_text(row.get("last_error", "")) or selected_downloaded_at or (_string(row.get("resolved_file_path", "")) or "今天和昨天都没有可用告警文件")),
        "meta_lines": meta_lines,
    }


def present_alarm_event_family(payload: Any, *, key: str = "", title: str = "") -> Dict[str, Any]:
    family = payload if isinstance(payload, dict) else {}
    current_bucket = _string(family.get("current_bucket", ""))
    buildings = [
        present_alarm_event_building(item, fallback_bucket=current_bucket)
        for item in (family.get("buildings", []) if isinstance(family.get("buildings", []), list) else [])
        if isinstance(item, dict)
    ]
    ready_count = sum(1 for item in buildings if item.get("status_key") == "ready")
    today_selected_count = sum(1 for item in buildings if item.get("selection_scope") == "today" and item.get("status_key") == "ready")
    failed_buildings = [item["building"] for item in buildings if item.get("status_key") == "failed"]
    blocked_buildings = [item["building"] for item in buildings if item.get("status_key") == "blocked"]
    used_previous_day_fallback = [_string(item) for item in (family.get("used_previous_day_fallback", []) if isinstance(family.get("used_previous_day_fallback", []), list) else []) if _string(item)]
    missing_today_buildings = [_string(item) for item in (family.get("missing_today_buildings", []) if isinstance(family.get("missing_today_buildings", []), list) else []) if _string(item)]
    missing_both_days_buildings = [_string(item) for item in (family.get("missing_both_days_buildings", []) if isinstance(family.get("missing_both_days_buildings", []), list) else []) if _string(item)]
    upload_state = family.get("external_upload", {}) if isinstance(family.get("external_upload", {}), dict) else {}
    upload_running = bool(upload_state.get("running", False))
    upload_started_at = _string(upload_state.get("started_at", ""))
    upload_current_mode = _string(upload_state.get("current_mode", ""))
    upload_current_scope = _string(upload_state.get("current_scope", ""))
    upload_last_run_at = _string(upload_state.get("last_run_at", ""))
    upload_last_success_at = _string(upload_state.get("last_success_at", ""))
    upload_last_error = bridge_text(upload_state.get("last_error", ""))
    upload_record_count = int(upload_state.get("uploaded_record_count", 0) or 0)
    upload_file_count = int(upload_state.get("uploaded_file_count", 0) or 0)
    upload_scope_text = f"（{upload_current_scope or '单楼'}）" if upload_current_mode == "single_building" else "（全量）"
    upload_started_text = f"，开始于 {upload_started_at}" if upload_started_at else ""
    upload_running_text = f"正在上传{upload_scope_text}{upload_started_text}" if upload_running else ""
    tone = "warning"
    status_text = "等待当天最新文件"
    summary_text = "当前策略：当天最新一份，缺失则回退昨天最新。"
    if failed_buildings:
        tone = "danger"
        status_text = "存在失败楼栋"
        summary_text = f"以下楼栋告警信息文件处理失败：{' / '.join(failed_buildings)}"
    elif blocked_buildings:
        tone = "warning"
        status_text = "等待内网恢复"
        summary_text = f"以下楼栋正在等待内网恢复：{' / '.join(blocked_buildings)}"
    elif missing_both_days_buildings:
        tone = "warning" if ready_count > 0 else "danger"
        status_text = "存在缺失楼栋"
        summary_text = (
            f"当前策略：当天最新一份，缺失则回退昨天最新。今天最新 {today_selected_count}/5 楼；"
            f"昨天回退 {len(used_previous_day_fallback)}/5 楼；今天和昨天都缺文件 {len(missing_both_days_buildings)}/5 楼。"
        )
    elif used_previous_day_fallback:
        tone = "warning"
        status_text = "存在昨天回退"
        summary_text = f"当前策略：当天最新 {today_selected_count}/5 楼；昨天回退 {len(used_previous_day_fallback)}/5 楼。"
    elif ready_count > 0:
        tone = "success"
        status_text = "当天最新已就绪"
        summary_text = f"当前策略：当天最新一份，缺失则回退昨天最新。今天已有 {today_selected_count or ready_count}/5 个楼栋告警文件可供外网消费。"
    if upload_last_run_at:
        summary_text += f" 最近上传：{upload_last_run_at}（记录 {upload_record_count} 条，文件 {upload_file_count} 份，源文件保留）。"
    upload_status_tone = tone
    upload_status_text = "尚未上传"
    upload_status_summary = "尚未执行告警信息上传。"
    if upload_running:
        upload_status_tone = "info"
        upload_status_text = "上传进行中"
        upload_status_summary = upload_running_text or "外网正在上传告警信息文件。"
    elif upload_last_error:
        upload_status_tone = "danger"
        upload_status_text = "最近上传失败"
        upload_status_summary = f"最近上传：{upload_last_run_at or '-'}。{upload_last_error}"
    elif upload_last_success_at:
        upload_status_tone = "success"
        upload_status_text = "最近上传成功"
        upload_status_summary = f"最近上传：{upload_last_run_at or upload_last_success_at}（记录 {upload_record_count} 条，文件 {upload_file_count} 份，源文件保留）。"
    family_meta_lines: List[str] = [
        "选择策略：当天最新一份，缺失则回退昨天最新",
    ]
    if _string(family.get("selection_reference_date", "")):
        _push_meta_line(family_meta_lines, f"参考日期：{_string(family.get('selection_reference_date', ''))}")
    if upload_last_run_at:
        _push_meta_line(family_meta_lines, f"最近上传：{upload_last_run_at} / 记录 {upload_record_count} 条 / 文件 {upload_file_count} 份 / 源文件保留")
    if upload_running_text:
        _push_meta_line(family_meta_lines, upload_running_text)
    if upload_last_error:
        _push_meta_line(family_meta_lines, f"最近上传异常：{upload_last_error}")
    family_items = [
        {
            "label": "当天最新",
            "value": f"{today_selected_count or ready_count}/5 楼",
            "tone": "success" if today_selected_count or ready_count else "neutral",
        },
        {
            "label": "昨天回退",
            "value": f"{len(used_previous_day_fallback)}/5 楼",
            "tone": "warning" if used_previous_day_fallback else "neutral",
        },
        {
            "label": "今天和昨天都缺",
            "value": f"{len(missing_both_days_buildings)}/5 楼",
            "tone": "danger" if missing_both_days_buildings else "neutral",
        },
        {
            "label": "最近上传",
            "value": upload_last_run_at or "-",
            "tone": upload_status_tone if upload_last_run_at else "neutral",
        },
    ]
    return {
        **family,
        "key": key or _string(family.get("key", "")),
        "title": title or _string(family.get("title", "")),
        "current_bucket": current_bucket,
        "buildings": buildings,
        "ready_count": ready_count,
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "can_proceed": ready_count > 0,
        "used_previous_day_fallback": used_previous_day_fallback,
        "missing_today_buildings": missing_today_buildings,
        "missing_both_days_buildings": missing_both_days_buildings,
        "today_selected_count": today_selected_count,
        "upload_last_run_at": upload_last_run_at,
        "upload_last_success_at": upload_last_success_at,
        "upload_last_error": upload_last_error,
        "upload_record_count": upload_record_count,
        "upload_file_count": upload_file_count,
        "upload_running": upload_running,
        "upload_started_at": upload_started_at,
        "upload_current_mode": upload_current_mode,
        "upload_current_scope": upload_current_scope,
        "upload_running_text": upload_running_text,
        "meta_lines": family_meta_lines,
        "detail_text": upload_last_error or summary_text,
        "reason_code": (
            "upload_running"
            if upload_running
            else ("failed" if failed_buildings else ("blocked" if blocked_buildings else ("missing" if missing_both_days_buildings else ("fallback" if used_previous_day_fallback else ("ready" if ready_count > 0 else "waiting")))))
        ),
        "items": family_items,
        "upload_status": {
            "tone": upload_status_tone,
            "status_text": upload_status_text,
            "summary_text": upload_status_summary,
        },
    }
