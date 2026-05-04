from __future__ import annotations

from typing import Any, Dict, List


def _string(value: Any) -> str:
    return str(value or "").strip()


def _list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _int(value: Any, fallback: int = 0) -> int:
    try:
        return int(str(value or "").strip())
    except Exception:
        return fallback


def _short_commit(value: Any) -> str:
    text = _string(value)
    return text[:7] if text else ""


def _action(
    action_id: str,
    *,
    label: str,
    desc: str = "",
    allowed: bool = True,
    pending: bool = False,
    disabled_reason: str = "",
    reason_code: str = "",
) -> Dict[str, Any]:
    return {
        "id": _string(action_id),
        "label": _string(label),
        "desc": _string(desc),
        "allowed": bool(allowed),
        "pending": bool(pending),
        "disabled_reason": _string(disabled_reason),
        "reason_code": _string(reason_code),
    }


def _normalize_role_mode(value: Any) -> str:
    text = _string(value).lower()
    return text if text in {"internal", "external"} else ""


def _format_role_label(value: Any) -> str:
    role_mode = _normalize_role_mode(value)
    if role_mode == "internal":
        return "内网端"
    if role_mode == "external":
        return "外网端"
    return "未选择"


def _resolve_shared_bridge_role_root(runtime_config: Any, role_mode: str) -> str:
    config = runtime_config if isinstance(runtime_config, dict) else {}
    shared_bridge = (
        config.get("shared_bridge", {})
        if isinstance(config.get("shared_bridge", {}), dict)
        else {}
    )
    root_dir = _string(shared_bridge.get("root_dir", ""))
    internal_root_dir = _string(shared_bridge.get("internal_root_dir", ""))
    external_root_dir = _string(shared_bridge.get("external_root_dir", ""))
    normalized_role = _normalize_role_mode(role_mode)
    if normalized_role == "internal":
        return internal_root_dir or root_dir
    if normalized_role == "external":
        return external_root_dir or root_dir
    return root_dir or internal_root_dir or external_root_dir


def _target_hint_text(
    *,
    preview: Dict[str, Any],
    target_kind: str,
    has_token_pair: bool,
    unresolved_hint: str,
    token_pair_hint: str,
) -> str:
    message = _string(preview.get("message", ""))
    if message:
        return message
    if target_kind in {"wiki_token_pair", "wiki_url"}:
        return "当前自动识别为 Wiki 多维表链接。"
    if target_kind in {"base_token_pair", "base_url"}:
        return "当前自动识别为 Base 多维表链接。"
    if has_token_pair:
        return token_pair_hint
    return unresolved_hint


def _present_day_metric_target(runtime_config: Any, preview_payload: Any) -> Dict[str, Any]:
    config = runtime_config if isinstance(runtime_config, dict) else {}
    preview = preview_payload if isinstance(preview_payload, dict) else {}
    target_cfg = (
        config.get("day_metric_upload", {}).get("target", {})
        if isinstance(config.get("day_metric_upload", {}), dict)
        else {}
    )
    source = target_cfg.get("source", {}) if isinstance(target_cfg.get("source", {}), dict) else {}
    app_token = _string(source.get("app_token", ""))
    table_id = _string(source.get("table_id", ""))
    base_url = _string(source.get("base_url", ""))
    wiki_url = _string(source.get("wiki_url", ""))
    display_url = (
        _string(preview.get("display_url", ""))
        or _string(preview.get("bitable_url", ""))
        or wiki_url
        or base_url
        or (f"https://vnet.feishu.cn/base/{app_token}?table={table_id}" if app_token and table_id else "")
    )
    target_kind = (
        _string(preview.get("target_kind", ""))
        or ("wiki_url" if wiki_url else ("base_url" if base_url else ("token_pair" if app_token and table_id else "")))
    )
    configured = bool(display_url or (app_token and table_id))
    return {
        "configured_app_token": _string(preview.get("configured_app_token", "")) or app_token,
        "operation_app_token": _string(preview.get("operation_app_token", "")),
        "table_id": _string(preview.get("table_id", "")) or table_id,
        "base_url": base_url,
        "wiki_url": wiki_url,
        "display_url": display_url,
        "bitable_url": display_url,
        "target_kind": target_kind,
        "configured": configured,
        "status_text": "已配置" if configured else "未配置",
        "hint_text": _target_hint_text(
            preview=preview,
            target_kind=target_kind,
            has_token_pair=bool(app_token and table_id),
            unresolved_hint="请先在配置中心补齐 12 项独立上传目标多维表配置。",
            token_pair_hint="当前按 App Token 和 Table ID 生成目标多维表链接。",
        ),
    }


def _present_alarm_target(runtime_config: Any, preview_payload: Any) -> Dict[str, Any]:
    config = runtime_config if isinstance(runtime_config, dict) else {}
    preview = preview_payload if isinstance(preview_payload, dict) else {}
    alarm_export = config.get("alarm_export", {}) if isinstance(config.get("alarm_export", {}), dict) else {}
    legacy_target = alarm_export.get("feishu", {}) if isinstance(alarm_export.get("feishu", {}), dict) else {}
    shared_upload_cfg = (
        alarm_export.get("shared_source_upload", {})
        if isinstance(alarm_export.get("shared_source_upload", {}), dict)
        else {}
    )
    override_target = (
        shared_upload_cfg.get("target", {})
        if isinstance(shared_upload_cfg.get("target", {}), dict)
        else {}
    )
    merged_target = {**legacy_target, **override_target}
    app_token = _string(merged_target.get("app_token", ""))
    table_id = _string(merged_target.get("table_id", ""))
    base_url = _string(merged_target.get("base_url", ""))
    wiki_url = _string(merged_target.get("wiki_url", ""))
    display_url = (
        _string(preview.get("display_url", ""))
        or _string(preview.get("bitable_url", ""))
        or wiki_url
        or base_url
        or (f"https://vnet.feishu.cn/base/{app_token}?table={table_id}" if app_token and table_id else "")
    )
    target_kind = (
        _string(preview.get("target_kind", ""))
        or ("wiki_url" if wiki_url else ("base_url" if base_url else ("token_pair" if app_token and table_id else "")))
    )
    configured = bool(display_url or (app_token and table_id))
    return {
        "configured_app_token": _string(preview.get("configured_app_token", "")) or app_token,
        "operation_app_token": _string(preview.get("operation_app_token", "")),
        "table_id": _string(preview.get("table_id", "")) or table_id,
        "base_url": base_url,
        "wiki_url": wiki_url,
        "display_url": display_url,
        "bitable_url": display_url,
        "target_kind": target_kind,
        "replace_existing_on_full": bool(shared_upload_cfg.get("replace_existing_on_full", True)),
        "configured": configured,
        "status_text": "已配置" if configured else "未配置",
        "hint_text": _target_hint_text(
            preview=preview,
            target_kind=target_kind,
            has_token_pair=bool(app_token and table_id),
            unresolved_hint="请先在配置中心的功能配置里补齐告警信息上传目标多维表。",
            token_pair_hint="当前按 App Token 和 Table ID 生成目标多维表链接。",
        ),
    }


def _present_engineer_directory_target(runtime_config: Any, preview_payload: Any) -> Dict[str, Any]:
    config = runtime_config if isinstance(runtime_config, dict) else {}
    preview = preview_payload if isinstance(preview_payload, dict) else {}
    handover_cfg = config.get("handover_log", {}) if isinstance(config.get("handover_log", {}), dict) else {}
    shift_roster_cfg = (
        handover_cfg.get("shift_roster", {})
        if isinstance(handover_cfg.get("shift_roster", {}), dict)
        else {}
    )
    source = (
        shift_roster_cfg.get("engineer_directory", {}).get("source", {})
        if isinstance(shift_roster_cfg.get("engineer_directory", {}), dict)
        and isinstance(shift_roster_cfg.get("engineer_directory", {}).get("source", {}), dict)
        else {}
    )
    fallback_source = (
        shift_roster_cfg.get("source", {})
        if isinstance(shift_roster_cfg.get("source", {}), dict)
        else {}
    )
    app_token = _string(source.get("app_token", "")) or _string(fallback_source.get("app_token", ""))
    table_id = _string(source.get("table_id", ""))
    display_url = _string(preview.get("display_url", "")) or _string(preview.get("bitable_url", ""))
    target_kind = _string(preview.get("target_kind", ""))
    has_configured_token_pair = bool(app_token and table_id)
    return {
        "configured_app_token": _string(preview.get("configured_app_token", "")) or app_token,
        "operation_app_token": _string(preview.get("operation_app_token", "")),
        "table_id": _string(preview.get("table_id", "")) or table_id,
        "display_url": display_url,
        "bitable_url": display_url,
        "target_kind": target_kind,
        "configured": has_configured_token_pair,
        "status_text": "已解析" if display_url else ("待解析" if has_configured_token_pair else "未配置"),
        "hint_text": _target_hint_text(
            preview=preview,
            target_kind=target_kind,
            has_token_pair=has_configured_token_pair,
            unresolved_hint="请先填写工程师目录多维 App Token 和 Table ID。",
            token_pair_hint="保存配置后会自动解析工程师目录多维表链接。",
        ),
    }


def _present_wet_bulb_target(runtime_config: Any, preview_payload: Any) -> Dict[str, Any]:
    config = runtime_config if isinstance(runtime_config, dict) else {}
    preview = preview_payload if isinstance(preview_payload, dict) else {}
    wet_bulb_cfg = (
        config.get("wet_bulb_collection", {})
        if isinstance(config.get("wet_bulb_collection", {}), dict)
        else {}
    )
    target_cfg = (
        wet_bulb_cfg.get("target", {})
        if isinstance(wet_bulb_cfg.get("target", {}), dict)
        else {}
    )
    configured_app_token = _string(preview.get("configured_app_token", "")) or _string(target_cfg.get("app_token", ""))
    operation_app_token = _string(preview.get("operation_app_token", ""))
    table_id = _string(preview.get("table_id", "")) or _string(target_cfg.get("table_id", ""))
    display_url = _string(preview.get("display_url", "")) or _string(preview.get("bitable_url", ""))
    target_kind = _string(preview.get("target_kind", ""))
    resolved_at = _string(preview.get("resolved_at", ""))
    message = _string(preview.get("message", ""))
    configured = bool(configured_app_token and table_id)
    status_text = "已解析" if display_url else ("待解析" if configured else "未配置")
    return {
        "configured_app_token": configured_app_token,
        "operation_app_token": operation_app_token,
        "table_id": table_id,
        "display_url": display_url,
        "bitable_url": display_url,
        "target_kind": target_kind,
        "resolved_at": resolved_at,
        "message": message,
        "configured": configured,
        "status_text": status_text,
        "hint_text": message or ("保存配置后会自动解析湿球温度目标多维表。" if configured else "请先补齐湿球温度采集目标多维表配置。"),
    }


def present_feature_target_displays(
    runtime_config: Any,
    *,
    engineer_directory_target_preview: Any = None,
    wet_bulb_target_preview: Any = None,
    day_metric_target_preview: Any = None,
    alarm_event_target_preview: Any = None,
) -> Dict[str, Any]:
    return {
        "engineer_directory": _present_engineer_directory_target(runtime_config, engineer_directory_target_preview),
        "wet_bulb_collection": _present_wet_bulb_target(runtime_config, wet_bulb_target_preview),
        "day_metric_upload": _present_day_metric_target(runtime_config, day_metric_target_preview),
        "alarm_event_upload": _present_alarm_target(runtime_config, alarm_event_target_preview),
    }


def present_config_guidance_overview(
    runtime_config: Any,
    *,
    configured_role_mode: str = "",
    running_role_mode: str = "",
    day_metric_target_preview: Any = None,
    alarm_event_target_preview: Any = None,
) -> Dict[str, Any]:
    config = runtime_config if isinstance(runtime_config, dict) else {}
    config_role = _normalize_role_mode(configured_role_mode) or _normalize_role_mode(
        config.get("deployment", {}).get("role_mode", "") if isinstance(config.get("deployment", {}), dict) else ""
    )
    runtime_role = _normalize_role_mode(running_role_mode)
    effective_role = config_role or runtime_role
    shared_root = _resolve_shared_bridge_role_root(config, effective_role)
    feishu_cfg = config.get("feishu", {}) if isinstance(config.get("feishu", {}), dict) else {}
    feishu_app_id = _string(feishu_cfg.get("app_id", ""))
    feishu_app_secret = _string(feishu_cfg.get("app_secret", ""))
    handover_cfg = config.get("handover_log", {}) if isinstance(config.get("handover_log", {}), dict) else {}
    handover_template_path = _string(
        handover_cfg.get("template", {}).get("source_path", "")
        if isinstance(handover_cfg.get("template", {}), dict)
        else ""
    )
    handover_cloud_root = _string(
        handover_cfg.get("cloud_sheet_sync", {}).get("root_wiki_url", "")
        if isinstance(handover_cfg.get("cloud_sheet_sync", {}), dict)
        else ""
    )
    day_metric_target = _present_day_metric_target(config, day_metric_target_preview)
    alarm_target = _present_alarm_target(config, alarm_event_target_preview)

    sections = [
        {
            "id": "common_deployment",
            "label": "角色与监听",
            "ready": bool(effective_role),
            "value": _format_role_label(effective_role),
            "tone": "success" if effective_role else "warning",
            "hint": (
                f"当前配置角色：{_format_role_label(effective_role)}"
                if effective_role
                else "需要先选择有效角色，否则无法确定本机监听模式。"
            ),
        },
        {
            "id": "common_deployment",
            "label": "共享目录",
            "ready": bool(shared_root),
            "value": shared_root or "未配置",
            "tone": "success" if shared_root else "warning",
            "hint": (
                "共享桥接、源文件和批准版本都会依赖该目录。"
                if shared_root
                else "未配置共享目录时，内外网主链无法通过共享缓存协同。"
            ),
        },
        {
            "id": "common_feishu_auth",
            "label": "飞书鉴权",
            "ready": bool(feishu_app_id and feishu_app_secret),
            "value": "已配置" if feishu_app_id and feishu_app_secret else "未配置",
            "tone": "success" if feishu_app_id and feishu_app_secret else "warning",
            "hint": (
                "飞书应用鉴权已具备。"
                if feishu_app_id and feishu_app_secret
                else "缺少 app_id 或 app_secret 时，涉及多维表的模块无法稳定运行。"
            ),
        },
    ]
    if effective_role != "internal":
        sections.extend(
            [
                {
                    "id": "feature_handover",
                    "label": "交接班模板",
                    "ready": bool(handover_template_path),
                    "value": "已配置" if handover_template_path else "未配置",
                    "tone": "success" if handover_template_path else "warning",
                    "hint": handover_template_path or "交接班日志没有模板路径时无法生成文件。",
                },
                {
                    "id": "feature_handover",
                    "label": "交接班云表",
                    "ready": bool(handover_cloud_root),
                    "value": "已配置" if handover_cloud_root else "未配置",
                    "tone": "success" if handover_cloud_root else "warning",
                    "hint": handover_cloud_root or "未配置根 Wiki 地址时，交接班后续云表链路无法完整执行。",
                },
                {
                    "id": "feature_day_metric_upload",
                    "label": "12项目标",
                    "ready": bool(day_metric_target.get("configured", False)),
                    "value": _string(day_metric_target.get("status_text", "")) or "未配置",
                    "tone": "success" if day_metric_target.get("configured", False) else "warning",
                    "hint": _string(day_metric_target.get("hint_text", "")),
                },
                {
                    "id": "feature_alarm_export",
                    "label": "告警目标",
                    "ready": bool(alarm_target.get("configured", False)),
                    "value": _string(alarm_target.get("status_text", "")) or "未配置",
                    "tone": "success" if alarm_target.get("configured", False) else "warning",
                    "hint": _string(alarm_target.get("hint_text", "")),
                },
            ]
        )
    ready_count = len([item for item in sections if bool(item.get("ready", False))])
    total_count = len(sections)
    missing_labels = [_string(item.get("label", "")) for item in sections if not bool(item.get("ready", False))]
    tone = "warning"
    status_text = "仍有关键配置待补齐"
    summary_text = f"当前已完成 {ready_count}/{total_count} 项关键配置。"
    if total_count > 0 and ready_count == total_count:
        tone = "success"
        status_text = "关键配置已齐套"
        summary_text = "当前高频主链所需配置已经齐套，后续再按模块补高级参数即可。"
    elif ready_count == 0:
        tone = "danger"
        status_text = "当前还没有完成关键配置"
        summary_text = "建议先从角色、共享目录和飞书鉴权开始，不要直接填全部细项。"
    elif missing_labels:
        summary_text = f"当前已完成 {ready_count}/{total_count} 项关键配置，仍缺：{' / '.join(missing_labels)}。"
    restart_required = bool(config_role and runtime_role and config_role != runtime_role)
    quick_tabs = [
        {"id": "common_deployment", "label": "角色与共享目录"},
        *([] if effective_role == "internal" else [{"id": "common_feishu_auth", "label": "飞书鉴权"}]),
        *([] if effective_role == "internal" else [{"id": "feature_handover", "label": "交接班"}]),
        *([] if effective_role == "internal" else [{"id": "feature_day_metric_upload", "label": "12项独立上传"}]),
        *([] if effective_role == "internal" else [{"id": "feature_alarm_export", "label": "告警上传"}]),
    ]
    reason_code = "partial"
    if total_count > 0 and ready_count == total_count:
        reason_code = "ready"
    elif ready_count == 0:
        reason_code = "missing_all"
    elif restart_required:
        reason_code = "restart_required"
    return {
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "detail_text": summary_text,
        "reason_code": reason_code,
        "restart_impact_text": (
            f"当前配置角色与正在运行角色不同，保存后会自动重启并切换到{_format_role_label(config_role)}。"
            if restart_required
            else "大多数配置保存后可直接生效；只有角色监听模式变化时才需要自动重启。"
        ),
        "sections": sections,
        "quick_tabs": quick_tabs,
        "ready_count": ready_count,
        "total_count": total_count,
        "missing_labels": [label for label in missing_labels if label],
        "configured_role_mode": config_role,
        "running_role_mode": runtime_role,
        "effective_role_mode": effective_role,
    }


def present_handover_review_overview(
    payload: Any,
    *,
    review_links: Any = None,
    recipient_status_by_building: Any = None,
) -> Dict[str, Any]:
    review = payload if isinstance(payload, dict) else {}
    review_link_rows = review_links if isinstance(review_links, list) else []
    recipient_rows = recipient_status_by_building if isinstance(recipient_status_by_building, list) else []
    review_link_map = {
        _string(item.get("building", "")): item
        for item in review_link_rows
        if isinstance(item, dict) and _string(item.get("building", ""))
    }
    recipient_status_map = {
        _string(item.get("building", "")): item
        for item in recipient_rows
        if isinstance(item, dict) and _string(item.get("building", ""))
    }
    required = int(review.get("required_count", 0) or 0)
    confirmed = int(review.get("confirmed_count", 0) or 0)
    pending = max(0, required - confirmed)
    duty_date = _string(review.get("duty_date", ""))
    duty_shift = _string(review.get("duty_shift", "")).lower()
    duty_shift_text = "白班" if duty_shift == "day" else ("夜班" if duty_shift == "night" else "")
    if not bool(review.get("has_any_session", False)):
        tone = "neutral"
        status_text = "当前批次未生成"
    elif bool(review.get("all_confirmed", False)):
        tone = "success"
        status_text = "5楼已全部确认"
    elif pending > 0:
        tone = "warning"
        status_text = f"还有 {pending} 个楼待确认"
    else:
        tone = "neutral"
        status_text = "等待确认状态更新"
    batch_key = _string(review.get("batch_key", ""))
    buildings = _list(review.get("buildings", []))
    cloud_retry_failure_count = len(
        [
            row
            for row in buildings
            if isinstance(row, dict)
            and _string(
                row.get("cloud_sheet_sync", {}).get("status", "")
                if isinstance(row.get("cloud_sheet_sync", {}), dict)
                else ""
            ).lower()
            in {"failed", "prepare_failed"}
        ]
    )
    followup_progress = (
        review.get("followup_progress", {})
        if isinstance(review.get("followup_progress", {}), dict)
        else {}
    )
    followup_failed = int(followup_progress.get("failed_count", 0) or 0)
    followup_pending = int(followup_progress.get("pending_count", 0) or 0)
    followup_attachment_pending = int(
        followup_progress.get("attachment_pending_count", 0) or 0
    )
    followup_cloud_pending = int(followup_progress.get("cloud_pending_count", 0) or 0)
    followup_daily_report_status = _string(
        followup_progress.get("daily_report_status", "")
    ).lower() or "idle"
    followup_status = _string(followup_progress.get("status", "")).lower() or "idle"
    can_resume_followup = bool(followup_progress.get("can_resume_followup", False))
    if followup_failed > 0:
        followup_tone = "danger"
        followup_status_text = "后续上传存在失败"
        followup_summary_text = f"待处理 {followup_pending} / 失败 {followup_failed}"
    elif followup_pending > 0:
        followup_tone = "warning"
        followup_status_text = "后续上传待处理"
        followup_summary_text = f"待处理 {followup_pending} / 失败 {followup_failed}"
    else:
        followup_tone = "success" if bool(review.get("all_confirmed", False)) else "neutral"
        followup_status_text = "后续上传已清空"
        followup_summary_text = "已清空"
    confirm_all_allowed = bool(review.get("has_any_session", False)) and not bool(review.get("all_confirmed", False))
    confirm_all_disabled_reason = ""
    if not bool(review.get("has_any_session", False)):
        confirm_all_disabled_reason = "当前批次未生成"
    elif bool(review.get("all_confirmed", False)):
        confirm_all_disabled_reason = "已全部确认"
    retry_all_visible = bool(batch_key) and bool(review.get("has_any_session", False))
    retry_all_allowed = retry_all_visible and bool(review.get("all_confirmed", False)) and cloud_retry_failure_count > 0
    retry_all_disabled_reason = ""
    if not retry_all_visible:
        retry_all_disabled_reason = "当前没有可重试的交接班批次"
    elif not bool(review.get("all_confirmed", False)):
        retry_all_disabled_reason = "待全部确认后可重试"
    elif cloud_retry_failure_count <= 0:
        retry_all_disabled_reason = "云表已全部同步"
    continue_followup_visible = bool(review.get("all_confirmed", False)) and can_resume_followup
    continue_followup_allowed = continue_followup_visible
    continue_followup_disabled_reason = "" if continue_followup_visible else "当前没有可继续的后续上传"
    continue_followup_label = "继续后续上传"
    if followup_failed > 0:
        continue_followup_label = f"继续后续上传（失败 {followup_failed}）"
    elif followup_pending > 0:
        continue_followup_label = f"继续后续上传（待处理 {followup_pending}）"

    def _present_cloud_sheet_sync_brief(raw: Any) -> Dict[str, Any]:
        cloud_sync = raw if isinstance(raw, dict) else {}
        status = _string(cloud_sync.get("status", "")).lower()
        attempted = bool(cloud_sync.get("attempted"))
        url = _string(cloud_sync.get("spreadsheet_url", ""))
        error = _string(cloud_sync.get("error", ""))
        if status == "success":
            return {
                "status": status,
                "text": "云表已同步",
                "tone": "success",
                "url": url,
                "error": "",
            }
        if status == "pending_upload":
            return {
                "status": status,
                "text": "云表待最终上传",
                "tone": "warning",
                "url": url,
                "error": error,
            }
        if status == "prepare_failed":
            return {
                "status": status,
                "text": "云表预建失败",
                "tone": "danger",
                "url": url,
                "error": error,
            }
        if status == "failed":
            return {
                "status": status,
                "text": "云表最终上传失败",
                "tone": "danger",
                "url": url,
                "error": error,
            }
        if status == "disabled":
            return {
                "status": status,
                "text": "云表未启用",
                "tone": "neutral",
                "url": url,
                "error": error,
            }
        if status == "skipped":
            return {
                "status": status,
                "text": "云表未执行",
                "tone": "neutral",
                "url": url,
                "error": error,
            }
        if attempted:
            return {
                "status": status or "attempted",
                "text": "云表已尝试同步",
                "tone": "info",
                "url": url,
                "error": error,
            }
        return {
            "status": status or "idle",
            "text": "云表未执行",
            "tone": "neutral",
            "url": url,
            "error": error,
        }

    def _present_review_link_delivery_brief(raw: Any) -> Dict[str, Any]:
        delivery = raw if isinstance(raw, dict) else {}
        status = _string(delivery.get("status", "")).lower()
        if status == "pending_access":
            text = "待审核地址就绪"
            tone = "warning"
        elif status == "success":
            text = "发送成功"
            tone = "success"
        elif status == "partial_failed":
            text = "部分失败"
            tone = "warning"
        elif status == "failed":
            text = "发送失败"
            tone = "danger"
        elif status == "disabled":
            text = "接收人已停用"
            tone = "neutral"
        elif status == "unconfigured":
            text = "当前楼未配置接收人"
            tone = "neutral"
        else:
            text = "待发送"
            tone = "neutral"
        return {
            "status": status or "idle",
            "text": text,
            "tone": tone,
            "error": _string(delivery.get("error", "")),
            "last_sent_at": _string(delivery.get("last_sent_at", "")),
            "last_attempt_at": _string(delivery.get("last_attempt_at", "")),
        }

    review_board_rows: List[Dict[str, Any]] = []
    for row in buildings:
        item = row if isinstance(row, dict) else {}
        building = _string(item.get("building", ""))
        has_session = bool(item.get("has_session", False))
        confirmed_row = bool(item.get("confirmed", False)) if has_session else False
        if has_session and confirmed_row:
            row_status = "confirmed"
            row_text = "已确认"
            row_tone = "success"
        elif has_session:
            row_status = "pending"
            row_text = "待确认"
            row_tone = "warning"
        else:
            row_status = "missing"
            row_text = "未生成"
            row_tone = "neutral"
        cloud_sheet_sync = _present_cloud_sheet_sync_brief(item.get("cloud_sheet_sync", {}))
        review_link_delivery = _present_review_link_delivery_brief(
            item.get("review_link_delivery", {})
        )
        recipient_status = _dict(recipient_status_map.get(building, {}))
        recipient_count = _int(recipient_status.get("recipient_count", 0))
        enabled_count = _int(recipient_status.get("enabled_count", 0))
        disabled_count = _int(recipient_status.get("disabled_count", 0))
        invalid_count = _int(recipient_status.get("invalid_count", 0))
        recipient_status_text = _string(recipient_status.get("status_text", ""))
        recipient_reason = _string(recipient_status.get("reason", ""))
        review_link_send_allowed = recipient_count > 0
        review_link_send_disabled_reason = ""
        review_link_send_reason_code = ""
        if not review_link_send_allowed:
            if enabled_count <= 0 and disabled_count > 0 and invalid_count <= 0:
                review_link_send_disabled_reason = recipient_reason or "当前楼审核链接接收人已全部停用"
                review_link_send_reason_code = "recipient_all_disabled"
            elif invalid_count > 0:
                review_link_send_disabled_reason = recipient_reason or "当前楼审核链接接收人存在无效配置"
                review_link_send_reason_code = "recipient_invalid"
            else:
                review_link_send_disabled_reason = recipient_reason or "当前楼未配置启用的审核链接接收人"
                review_link_send_reason_code = "recipient_unconfigured"
        review_board_rows.append(
            {
                "building": building,
                "status": row_status,
                "text": row_text,
                "tone": row_tone,
                "code": _string(review_link_map.get(building, {}).get("code", "")),
                "url": _string(review_link_map.get(building, {}).get("url", "")),
                "has_url": bool(_string(review_link_map.get(building, {}).get("url", ""))),
                "session_id": _string(item.get("session_id", "")),
                "revision": int(item.get("revision", 0) or 0),
                "updated_at": _string(item.get("updated_at", "")),
                "cloud_sheet_sync": cloud_sheet_sync,
                "review_link_delivery": review_link_delivery,
                "review_link_recipient_status": {
                    "text": recipient_status_text or ("已保存，可发送" if review_link_send_allowed else "当前楼未配置接收人"),
                    "reason": recipient_reason,
                    "recipient_count": recipient_count,
                    "enabled_count": enabled_count,
                    "disabled_count": disabled_count,
                    "invalid_count": invalid_count,
                },
                "actions": {
                    "review_link_send": {
                        "allowed": review_link_send_allowed,
                        "pending": False,
                        "label": "手动发送审核链接",
                        "disabled_reason": review_link_send_disabled_reason,
                        "reason_code": review_link_send_reason_code,
                    },
                },
            }
        )
    return {
        "tone": tone,
        "status_text": status_text,
        "summary_text": status_text,
        "batch_key": batch_key,
        "duty_date": duty_date,
        "duty_shift": duty_shift,
        "duty_text": f"{duty_date} / {duty_shift_text}" if duty_date and duty_shift_text else "",
        "has_any_session": bool(review.get("has_any_session", False)),
        "required": required,
        "confirmed": confirmed,
        "pending": pending,
        "all_confirmed": bool(review.get("all_confirmed", False)),
        "ready_for_followup_upload": bool(review.get("ready_for_followup_upload", False)),
        "cloud_retry_failure_count": cloud_retry_failure_count,
        "followup_failed_count": followup_failed,
        "followup_pending_count": followup_pending,
        "review_board_rows": review_board_rows,
        "followup_progress": {
            "status": followup_status,
            "can_resume_followup": can_resume_followup,
            "pending_count": followup_pending,
            "failed_count": followup_failed,
            "attachment_pending_count": followup_attachment_pending,
            "cloud_pending_count": followup_cloud_pending,
            "daily_report_status": followup_daily_report_status,
            "tone": followup_tone,
            "status_text": followup_status_text,
            "summary_text": followup_summary_text,
        },
        "actions": {
            "confirm_all": {
                "allowed": confirm_all_allowed,
                "pending": False,
                "label": "一键全确认" if confirm_all_allowed else (confirm_all_disabled_reason or "一键全确认"),
                "disabled_reason": confirm_all_disabled_reason,
                "visible": bool(review.get("has_any_session", False)),
            },
            "retry_cloud_sync_all": {
                "allowed": retry_all_allowed,
                "pending": False,
                "label": (
                    "一键全部重试云表上传"
                    if retry_all_allowed
                    else (retry_all_disabled_reason or "一键全部重试云表上传")
                ),
                "disabled_reason": retry_all_disabled_reason,
                "visible": retry_all_visible,
            },
            "continue_followup": {
                "allowed": continue_followup_allowed,
                "pending": False,
                "label": continue_followup_label,
                "disabled_reason": continue_followup_disabled_reason,
                "visible": continue_followup_visible,
            },
        },
    }


def present_alarm_upload_overview(shared_source_cache_overview: Any) -> Dict[str, Any]:
    overview = shared_source_cache_overview if isinstance(shared_source_cache_overview, dict) else {}
    families = _list(overview.get("families", []))
    family = next(
        (
            item
            for item in families
            if isinstance(item, dict) and _string(item.get("key", "")) == "alarm_event_family"
        ),
        {},
    )
    upload = family.get("upload_status", {}) if isinstance(family.get("upload_status", {}), dict) else {}
    tone = _string(upload.get("tone", "")) or _string(family.get("tone", "")) or "warning"
    status_text = _string(upload.get("status_text", "")) or "尚未上传"
    summary_text = _string(upload.get("summary_text", "")) or "尚未执行告警信息上传。"
    reason_code = _string(upload.get("reason_code", "")) or _string(family.get("reason_code", ""))
    if not reason_code:
        if tone == "danger":
            reason_code = "upload_failed"
        elif tone == "success":
            reason_code = "uploaded"
        elif _string(family.get("status_text", "")) == "等待当天最新文件":
            reason_code = "waiting_source_file"
        else:
            reason_code = "idle"
    return {
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "detail_text": summary_text,
        "reason_code": reason_code,
        "family_status_text": _string(family.get("status_text", "")),
        "family_summary_text": _string(family.get("summary_text", "")),
    }


def _present_monthly_report_delivery_row(payload: Any) -> Dict[str, Any]:
    row = payload if isinstance(payload, dict) else {}
    send_ready = bool(row.get("send_ready", False))
    reason_text = _string(row.get("reason", ""))
    return {
        "building": _string(row.get("building", "")) or "-",
        "supervisor": _string(row.get("supervisor", "")),
        "position": _string(row.get("position", "")),
        "recipient_id": _string(row.get("recipient_id", "")),
        "receive_id_type": _string(row.get("receive_id_type", "")) or "user_id",
        "send_ready": send_ready,
        "reason": reason_text,
        "file_name": _string(row.get("file_name", "")),
        "file_path": _string(row.get("file_path", "")),
        "file_exists": bool(row.get("file_exists", False)),
        "report_type": _string(row.get("report_type", "")),
        "target_month": _string(row.get("target_month", "")),
        "tone": "success" if send_ready else "warning",
        "status_text": "可发送" if send_ready else "不可发送",
        "detail_text": "已匹配设施运维主管并找到可发送文件。" if send_ready else (reason_text or "当前楼栋不可发送。"),
    }


def present_monthly_report_last_run_display(report_type: str, payload: Any) -> Dict[str, Any]:
    normalized_report_type = "change" if _string(report_type).lower() == "change" else "event"
    report_label = "变更" if normalized_report_type == "change" else "事件"
    last_run = payload if isinstance(payload, dict) else {}
    status = _string(last_run.get("status", "")).lower()
    target_month = _string(last_run.get("target_month", ""))
    generated_files = int(last_run.get("generated_files", 0) or 0)
    started_at = _string(last_run.get("started_at", ""))
    finished_at = _string(last_run.get("finished_at", "")) or started_at
    error_text = _string(last_run.get("error", ""))
    successful_buildings = [_string(item) for item in _list(last_run.get("successful_buildings", [])) if _string(item)]
    failed_buildings = [_string(item) for item in _list(last_run.get("failed_buildings", [])) if _string(item)]
    output_dir = _string(last_run.get("output_dir", ""))

    tone = "neutral"
    status_text = "尚未执行"
    summary_text = f"最近还没有生成{report_label}月度统计表。"
    reason_code = "idle"
    if status in {"ok", "success"}:
        tone = "success"
        status_text = "最近生成成功"
        summary_text = (
            f"最近生成：{finished_at or '-'}，目标月份 {target_month or '-'}，生成文件 {generated_files} 个。"
        )
        reason_code = "success"
    elif status == "partial_failed":
        tone = "warning"
        status_text = "最近生成部分失败"
        summary_text = (
            f"最近生成：{finished_at or '-'}，目标月份 {target_month or '-'}，请检查失败楼栋并补跑。"
        )
        reason_code = "partial_failed"
    elif status == "failed":
        tone = "danger"
        status_text = "最近生成失败"
        summary_text = error_text or "最近一次生成失败，请检查最近结果。"
        reason_code = "failed"
    elif finished_at or target_month:
        tone = "info"
        status_text = "最近已执行"
        summary_text = (
            f"最近执行：{finished_at or '-'}，目标月份 {target_month or '-'}，生成文件 {generated_files} 个。"
        )
        reason_code = "completed"

    return {
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "detail_text": summary_text,
        "reason_code": reason_code,
        "started_at": started_at,
        "target_month": target_month,
        "generated_files": generated_files,
        "finished_at": finished_at,
        "successful_buildings": successful_buildings,
        "failed_buildings": failed_buildings,
        "output_dir": output_dir,
        "error": error_text,
        "error_text": error_text,
    }


def present_monthly_report_delivery_display(
    report_type: str,
    report_last_run: Any,
    delivery_payload: Any,
) -> Dict[str, Any]:
    normalized_report_type = "change" if _string(report_type).lower() == "change" else "event"
    report_label = "变更" if normalized_report_type == "change" else "事件"
    last_run = report_last_run if isinstance(report_last_run, dict) else {}
    delivery = delivery_payload if isinstance(delivery_payload, dict) else {}
    delivery_last_run = delivery.get("last_run", {}) if isinstance(delivery.get("last_run", {}), dict) else {}
    rows = [
        _present_monthly_report_delivery_row(item)
        for item in _list(delivery.get("recipient_status_by_building", []))
        if isinstance(item, dict)
    ]
    send_ready_count = len([item for item in rows if bool(item.get("send_ready", False))])
    delivery_error = _string(delivery.get("error", ""))
    status = _string(delivery_last_run.get("status", "")).lower()
    is_test_mode = bool(delivery_last_run.get("test_mode", False))
    delivery_started_at = _string(delivery_last_run.get("started_at", ""))
    delivery_finished_at = _string(delivery_last_run.get("finished_at", "")) or delivery_started_at
    delivery_target_month = _string(delivery_last_run.get("target_month", "")) or _string(last_run.get("target_month", ""))
    delivery_successful_buildings = [
        _string(item) for item in _list(delivery_last_run.get("successful_buildings", [])) if _string(item)
    ]
    delivery_failed_buildings = [
        _string(item) for item in _list(delivery_last_run.get("failed_buildings", [])) if _string(item)
    ]
    test_receive_ids = [_string(item) for item in _list(delivery_last_run.get("test_receive_ids", [])) if _string(item)]
    test_successful_receivers = [
        _string(item) for item in _list(delivery_last_run.get("test_successful_receivers", [])) if _string(item)
    ]
    test_failed_receivers = [
        _string(item) for item in _list(delivery_last_run.get("test_failed_receivers", [])) if _string(item)
    ]
    delivery_last_run_error = _string(delivery_last_run.get("error", ""))

    tone = "neutral"
    status_text = "待生成"
    summary_text = f"请先生成{report_label}月度统计表，再执行文件发送。"
    reason_code = "report_missing"
    if delivery_error:
        tone = "danger"
        status_text = "发送前置检查失败"
        summary_text = delivery_error
        reason_code = "precheck_failed"
    elif status == "success":
        success_receiver_count = len(_list(delivery_last_run.get("test_successful_receivers", [])))
        tone = "success"
        status_text = "最近测试发送成功" if is_test_mode else "最近发送成功"
        summary_text = (
            f"最近测试发送：{_string(delivery_last_run.get('finished_at', '')) or _string(delivery_last_run.get('started_at', '')) or '-'}，"
            f"成功发送 {success_receiver_count} 人，文件 {_string(delivery_last_run.get('test_file_building', '')) or '-'} / {_string(delivery_last_run.get('test_file_name', '')) or '-'}"
            if is_test_mode
            else (
                f"最近发送：{_string(delivery_last_run.get('finished_at', '')) or _string(delivery_last_run.get('started_at', '')) or '-'}，"
                f"成功 {len(_list(delivery_last_run.get('successful_buildings', [])))} 楼。"
            )
        )
        reason_code = "success"
    elif status == "partial_failed":
        success_receiver_count = len(_list(delivery_last_run.get("test_successful_receivers", [])))
        failed_receiver_count = len(_list(delivery_last_run.get("test_failed_receivers", [])))
        tone = "warning"
        status_text = "最近测试发送部分失败" if is_test_mode else "最近发送部分失败"
        summary_text = (
            f"最近测试发送：{_string(delivery_last_run.get('finished_at', '')) or _string(delivery_last_run.get('started_at', '')) or '-'}，"
            f"成功 {success_receiver_count} 人，失败 {failed_receiver_count} 人。"
            if is_test_mode
            else f"最近发送：{_string(delivery_last_run.get('finished_at', '')) or _string(delivery_last_run.get('started_at', '')) or '-'}，请查看失败楼栋并修正收件人或文件。"
        )
        reason_code = "partial_failed"
    elif status == "failed":
        tone = "danger"
        status_text = "最近测试发送失败" if is_test_mode else "最近发送失败"
        summary_text = _string(delivery_last_run.get("error", "")) or (
            "最近一次测试发送失败，请查看最近结果。"
            if is_test_mode
            else "最近一次发送失败，请查看最近结果。"
        )
        reason_code = "failed"
    elif _string(last_run.get("target_month", "")):
        tone = "info" if send_ready_count > 0 else "warning"
        status_text = "待发送" if send_ready_count > 0 else "缺少收件人"
        summary_text = (
            f"当前有 {send_ready_count}/5 个楼栋满足发送条件。"
            if send_ready_count > 0
            else "当前没有楼栋满足发送条件，请先检查工程师目录和最近生成文件。"
        )
        reason_code = "ready_to_send" if send_ready_count > 0 else "missing_recipient"

    return {
        "overview": {
            "tone": tone,
            "status_text": status_text,
            "summary_text": summary_text,
            "detail_text": summary_text,
            "reason_code": reason_code,
            "send_ready_count": send_ready_count,
            "target_month": _string(last_run.get("target_month", "")),
            "report_type": normalized_report_type,
            "report_label": report_label,
        },
        "last_run": {
            "tone": tone,
            "status_text": status_text,
            "summary_text": summary_text,
            "detail_text": summary_text,
            "reason_code": reason_code,
            "started_at": delivery_started_at,
            "finished_at": delivery_finished_at,
            "target_month": delivery_target_month,
            "successful_buildings": delivery_successful_buildings,
            "failed_buildings": delivery_failed_buildings,
            "test_mode": is_test_mode,
            "test_receive_ids": test_receive_ids,
            "test_receive_id_type": _string(delivery_last_run.get("test_receive_id_type", "")),
            "test_successful_receivers": test_successful_receivers,
            "test_failed_receivers": test_failed_receivers,
            "test_file_name": _string(delivery_last_run.get("test_file_name", "")),
            "test_file_building": _string(delivery_last_run.get("test_file_building", "")),
            "error": delivery_last_run_error,
            "error_text": delivery_last_run_error,
        },
        "rows": rows,
        "send_ready_count": send_ready_count,
    }


def _normalize_task_overview(payload: Any) -> Dict[str, Any]:
    overview = payload if isinstance(payload, dict) else {}
    return {
        "tone": _string(overview.get("tone", "")) or "neutral",
        "status_text": _string(overview.get("status_text", "")) or "当前空闲",
        "summary_text": _string(overview.get("summary_text", "")) or "暂无长耗时任务，可直接从主动作开始。",
        "next_action_text": _string(overview.get("next_action_text", "")),
        "focus_title": _string(overview.get("focus_title", "")) or "当前没有选中任务",
        "focus_meta": _string(overview.get("focus_meta", "")) or "可以直接开始新的流程动作",
        "items": [item for item in _list(overview.get("items", [])) if isinstance(item, dict)],
    }


def _updater_source_label(payload: Dict[str, Any]) -> str:
    kind = _string(payload.get("source_kind", "")).lower()
    if kind == "shared_approved_source":
        return "共享目录批准源码（不访问互联网）"
    if kind == "shared_mirror":
        return "共享目录更新源（不访问互联网）"
    if kind == "git_remote":
        return "Git 仓库更新源"
    return _string(payload.get("source_label", "")) or "远端正式更新源"


def _updater_disabled_reason_text(raw: Any) -> str:
    key = _string(raw).lower()
    if key == "source_python_run":
        return "当前为 Python 本地源码运行，已跳过更新。"
    if key == "git_not_installed":
        return "当前电脑未安装 Git，无法执行代码拉取更新。"
    if key == "git_repo_missing":
        return "当前代码目录不是 Git 工作区，无法执行代码拉取更新。"
    if key == "git_remote_missing":
        return "当前未配置 Git 更新仓库地址。"
    if key == "shared_root_missing":
        return "共享目录未配置，无法检查批准版本。"
    return "当前运行模式已跳过更新。"


def present_updater_mirror_overview(payload: Any) -> Dict[str, Any]:
    updater = payload if isinstance(payload, dict) else {}
    updater_enabled = updater.get("enabled", True) is not False
    disabled_reason = _string(updater.get("disabled_reason", "")).lower()
    source_kind = _string(updater.get("source_kind", "")).lower()
    source_label = _updater_source_label(updater)
    update_mode = _string(updater.get("update_mode", "")).lower()
    mirror_ready = bool(updater.get("mirror_ready", False))
    mirror_version = _string(updater.get("mirror_version", ""))
    local_version = _string(updater.get("local_version", "")) or "-"
    branch = _string(updater.get("branch", ""))
    local_commit = _string(updater.get("local_commit", ""))
    remote_commit = _string(updater.get("remote_commit", ""))
    approved_commit = _string(updater.get("approved_commit", ""))
    last_published_commit = _string(updater.get("last_published_commit", ""))
    last_publish_attempt_commit = _string(updater.get("last_publish_attempt_commit", ""))
    last_publish_deferred_commit = _string(updater.get("last_publish_deferred_commit", ""))
    last_publish_command_id = _string(updater.get("last_publish_command_id", ""))
    last_internal_apply_completed_commit = _string(updater.get("last_internal_apply_completed_commit", ""))
    last_internal_apply_failed_commit = _string(updater.get("last_internal_apply_failed_commit", ""))
    worktree_dirty = bool(updater.get("worktree_dirty", False))
    local_revision = int(updater.get("local_release_revision", 0) or 0)
    last_publish_at = _string(updater.get("last_publish_at", ""))
    manifest_path = _string(updater.get("mirror_manifest_path", ""))
    error_text = _string(updater.get("last_publish_error", ""))
    internal_peer = updater.get("internal_peer", {}) if isinstance(updater.get("internal_peer", {}), dict) else {}
    internal_peer_available = bool(internal_peer.get("available", False))
    internal_peer_online = bool(internal_peer.get("online", False))
    internal_peer_version = _string(internal_peer.get("local_version", ""))
    internal_peer_commit = _string(internal_peer.get("local_commit", ""))
    internal_peer_revision = int(internal_peer.get("local_release_revision", 0) or 0)
    internal_peer_heartbeat_at = _string(internal_peer.get("heartbeat_at", ""))
    internal_peer_check_at = _string(internal_peer.get("last_check_at", ""))
    internal_peer_update_available = bool(internal_peer.get("update_available", False))
    internal_peer_restart_required = bool(internal_peer.get("restart_required", False))
    internal_peer_last_result = _string(internal_peer.get("last_result", "")).lower()
    internal_peer_last_command_status = _string(internal_peer.get("last_command_status", "")).lower()
    internal_peer_last_command_action = _string(internal_peer.get("last_command_action", "")).lower()
    internal_peer_command = (
        internal_peer.get("command", {})
        if isinstance(internal_peer.get("command", {}), dict)
        else {}
    )
    internal_peer_command_action = _string(internal_peer_command.get("action", "")).lower()
    internal_peer_command_status = _string(internal_peer_command.get("status", "")).lower()
    internal_peer_command_active = bool(internal_peer_command.get("active", False))
    internal_peer_command_source_commit = _string(internal_peer_command.get("source_commit", ""))
    internal_peer_last_command_source_commit = _string(internal_peer.get("last_command_source_commit", ""))
    internal_peer_version_text = (
        f"{internal_peer_version or '-'} / r{internal_peer_revision}"
        if internal_peer_revision > 0
        else (internal_peer_version or ("未上报" if internal_peer_available else "-"))
    )
    if not internal_peer_command_active:
        internal_peer_command_label = "无待执行命令"
    else:
        action_text = (
            "开始更新"
            if internal_peer_command_action == "apply"
            else (
                "检查更新"
                if internal_peer_command_action == "check"
                else ("重启生效" if internal_peer_command_action == "restart" else "更新命令")
            )
        )
        status_text = (
            "待执行"
            if internal_peer_command_status == "pending"
            else (
                "已接收"
                if internal_peer_command_status == "accepted"
                else ("执行中" if internal_peer_command_status == "running" else "处理中")
            )
        )
        internal_peer_command_label = f"{action_text}（{status_text}）"
    if internal_peer_check_at:
        internal_peer_check_text = internal_peer_check_at
    elif internal_peer_command_active and internal_peer_command_action == "check":
        internal_peer_check_text = "等待内网执行检查" if internal_peer_online else "等待内网上线执行"
    elif internal_peer_heartbeat_at:
        internal_peer_check_text = f"{internal_peer_heartbeat_at}（心跳）"
    else:
        internal_peer_check_text = "尚未检查" if internal_peer_available else "-"
    if not internal_peer_available:
        internal_peer_update_status_text = "未接入"
    elif internal_peer_command_active and internal_peer_command_action == "check":
        internal_peer_update_status_text = "待检查完成" if internal_peer_online else "待内网上线检查"
    elif not internal_peer_check_at and not internal_peer_last_result:
        internal_peer_update_status_text = "尚未检查"
    else:
        internal_peer_update_status_text = "已发现更新" if internal_peer_update_available else "未发现更新"
    if not internal_peer_available:
        internal_peer_status_text = "未接入"
    elif internal_peer_command_active:
        if not internal_peer_online:
            internal_peer_status_text = "离线，已有待执行命令"
        elif internal_peer_command_action == "apply":
            internal_peer_status_text = "在线，正在处理开始更新"
        elif internal_peer_command_action == "check":
            internal_peer_status_text = "在线，正在处理检查更新"
        elif internal_peer_command_action == "restart":
            internal_peer_status_text = "在线，正在处理重启生效"
        else:
            internal_peer_status_text = "在线，正在处理远程命令"
    else:
        internal_peer_status_text = "在线" if internal_peer_online else "离线"
    main_action_id = "check_apply"
    main_action_label = "检查并更新"
    main_action_allowed = bool(updater_enabled)
    main_action_disabled_reason = _updater_disabled_reason_text(disabled_reason) if not updater_enabled else ""
    main_action_reason_code = disabled_reason if not updater_enabled else ""
    queued_apply = bool(
        ((updater.get("queued_apply", {}) if isinstance(updater.get("queued_apply", {}), dict) else {}).get("queued", False))
    )
    dependency_sync_status = _string(updater.get("dependency_sync_status", "")).lower()
    last_result = _string(updater.get("last_result", "")).lower()
    restart_required = bool(updater.get("restart_required", False))
    active_business_block_results = {
        "downloading_patch",
        "applying_patch",
        "dependency_checking",
        "dependency_syncing",
        "dependency_rollback",
        "updated_restart_scheduled",
        "restart_pending",
    }
    business_actions_allowed = True
    business_actions_reason_code = ""
    business_actions_disabled_reason = ""
    business_actions_status_text = "允许执行业务动作"
    if queued_apply:
        business_actions_allowed = False
        business_actions_reason_code = "queued_apply"
        business_actions_disabled_reason = "更新任务已排队，当前请等待更新完成。"
        business_actions_status_text = "等待更新任务开始"
    elif restart_required:
        business_actions_allowed = False
        business_actions_reason_code = "restart_required"
        business_actions_disabled_reason = "更新已完成，需先重启生效。"
        business_actions_status_text = "等待重启生效"
    elif dependency_sync_status == "running":
        business_actions_allowed = False
        business_actions_reason_code = "dependency_syncing"
        business_actions_disabled_reason = "运行依赖正在同步，当前请等待完成。"
        business_actions_status_text = "运行依赖同步中"
    elif last_result in active_business_block_results:
        business_actions_allowed = False
        business_actions_reason_code = last_result
        business_actions_disabled_reason = "更新流程正在执行，当前请等待完成。"
        business_actions_status_text = "更新进行中"
    if updater_enabled:
        if bool(updater.get("restart_required", False)):
            main_action_id = "restart"
            main_action_label = "立即重启生效"
        elif update_mode == "git_pull":
            main_action_id = "check"
            main_action_label = "刷新本机代码状态"
        elif update_mode == "shared_approved_source":
            if bool(updater.get("update_available", False)):
                main_action_id = "apply"
                main_action_label = "应用共享更新"
            else:
                main_action_id = "check"
                main_action_label = "检查共享更新"
        elif queued_apply:
            main_action_id = "apply"
            main_action_label = "任务结束后自动更新"
        elif bool(updater.get("update_available", False) or updater.get("force_apply_available", False)):
            main_action_id = "apply"
            main_action_label = "开始更新"
    if updater_enabled and update_mode == "git_pull" and worktree_dirty:
        main_action_allowed = False
        main_action_disabled_reason = "检测到本地已修改文件，已阻止自动拉取代码。"
        main_action_reason_code = "dirty_worktree"
    publish_allowed = bool(
        updater_enabled
        and update_mode == "git_pull"
        and source_kind == "git_remote"
        and internal_peer_available
        and not internal_peer_command_active
        and not worktree_dirty
    )
    publish_label = "手动同步当前代码"
    publish_disabled_reason = ""
    publish_reason_code = ""
    if not updater_enabled:
        publish_allowed = False
        publish_disabled_reason = main_action_disabled_reason or "当前运行模式已跳过更新。"
        publish_reason_code = main_action_reason_code or "updater_disabled"
    elif update_mode != "git_pull":
        publish_allowed = False
        publish_disabled_reason = "当前不是 Git 源码更新模式，无法发布源码批准版本。"
        publish_reason_code = "not_git_pull"
    elif source_kind != "git_remote":
        publish_allowed = False
        publish_disabled_reason = "当前更新源不是 Git 仓库。"
        publish_reason_code = "not_git_remote"
    elif not internal_peer_available:
        publish_allowed = False
        publish_disabled_reason = "共享目录未配置，无法同步内网代码。"
        publish_reason_code = "shared_root_missing"
    elif internal_peer_command_active:
        publish_allowed = False
        publish_disabled_reason = "当前已有待执行内网更新命令，暂不覆盖共享源码包。"
        publish_reason_code = "command_active"
        publish_label = "等待内网命令完成"
    elif worktree_dirty:
        publish_allowed = False
        publish_disabled_reason = "存在本地代码改动，无法同步内网代码。"
        publish_reason_code = "dirty_worktree"
    internal_peer_check_allowed = bool(updater_enabled and internal_peer_available and not internal_peer_command_active)
    internal_peer_check_label = "远程更新已移除"
    internal_peer_check_disabled_reason = ""
    internal_peer_check_reason_code = ""
    if not updater_enabled:
        internal_peer_check_disabled_reason = main_action_disabled_reason or "当前运行模式已跳过更新。"
        internal_peer_check_reason_code = main_action_reason_code or "updater_disabled"
    elif not internal_peer_available:
        internal_peer_check_disabled_reason = "当前未接入内网端远程更新能力"
        internal_peer_check_reason_code = "internal_peer_unavailable"
    elif internal_peer_command_active:
        internal_peer_check_disabled_reason = "当前已有待执行远程命令"
        internal_peer_check_reason_code = "command_active"
        if internal_peer_command_action == "check":
            internal_peer_check_label = "等待检查完成..."
        elif internal_peer_command_action == "apply":
            internal_peer_check_label = "等待更新完成..."
        else:
            internal_peer_check_label = "已有待执行命令"
    internal_peer_apply_allowed = bool(
        updater_enabled
        and internal_peer_available
        and not internal_peer_command_active
        and internal_peer_update_available
    )
    internal_peer_apply_label = "远程更新已移除"
    internal_peer_apply_disabled_reason = ""
    internal_peer_apply_reason_code = ""
    if not updater_enabled:
        internal_peer_apply_disabled_reason = main_action_disabled_reason or "当前运行模式已跳过更新。"
        internal_peer_apply_reason_code = main_action_reason_code or "updater_disabled"
    elif not internal_peer_available:
        internal_peer_apply_disabled_reason = "当前未接入内网端远程更新能力"
        internal_peer_apply_reason_code = "internal_peer_unavailable"
    elif internal_peer_command_active:
        internal_peer_apply_disabled_reason = "当前已有待执行远程命令"
        internal_peer_apply_reason_code = "command_active"
        if internal_peer_command_action == "check":
            internal_peer_apply_label = "等待检查完成..."
        elif internal_peer_command_action == "apply":
            internal_peer_apply_label = (
                "更新进行中..." if internal_peer_online else "等待内网上线执行..."
            )
        else:
            internal_peer_apply_label = "已有待执行命令"
    elif not internal_peer_update_available:
        internal_peer_apply_disabled_reason = (
            "需先检查更新"
            if not internal_peer_check_at and not internal_peer_last_result
            else "当前未发现可更新版本"
        )
        internal_peer_apply_reason_code = (
            "needs_check"
            if not internal_peer_check_at and not internal_peer_last_result
            else "no_update"
        )
        internal_peer_apply_label = (
            "需先检查更新"
            if not internal_peer_check_at and not internal_peer_last_result
            else "未发现更新"
        )
    internal_peer_restart_allowed = bool(
        updater_enabled
        and internal_peer_available
        and not internal_peer_command_active
        and internal_peer_restart_required
    )
    internal_peer_restart_label = "远程更新已移除"
    internal_peer_restart_disabled_reason = ""
    internal_peer_restart_reason_code = ""
    if not updater_enabled:
        internal_peer_restart_disabled_reason = main_action_disabled_reason or "当前运行模式已跳过更新。"
        internal_peer_restart_reason_code = main_action_reason_code or "updater_disabled"
    elif not internal_peer_available:
        internal_peer_restart_disabled_reason = "当前未接入内网端远程更新能力"
        internal_peer_restart_reason_code = "internal_peer_unavailable"
    elif internal_peer_command_active:
        internal_peer_restart_disabled_reason = "当前已有待执行远程命令"
        internal_peer_restart_reason_code = "command_active"
        internal_peer_restart_label = "已有待执行命令"
    elif not internal_peer_restart_required:
        internal_peer_restart_disabled_reason = "内网端当前没有待重启更新。"
        internal_peer_restart_reason_code = "restart_not_required"
        internal_peer_restart_label = "无需重启"
    if not updater_enabled and disabled_reason == "source_python_run":
        return {
            "tone": "info",
            "kicker": "调试模式",
            "title": "本地运行模式",
            "status_text": "本地调试模式",
            "badge_text": "本地源码运行不更新",
            "summary_text": "当前为 Python 本地源码运行，已跳过自动更新与共享目录镜像检查。",
            "manifest_path": "",
            "error_text": "",
            "items": [
                {"label": "运行方式", "value": "Python 本地源码运行", "tone": "info"},
                {
                    "label": "当前版本",
                    "value": f"{local_version} / r{local_revision}" if local_revision > 0 else local_version,
                    "tone": "neutral",
                },
                {"label": "更新行为", "value": "不自动更新", "tone": "neutral"},
                {"label": "共享镜像", "value": "不检查", "tone": "neutral"},
                {
                    "label": "内网端状态",
                    "value": internal_peer_status_text,
                    "tone": "warning" if internal_peer_command_active else ("success" if internal_peer_online else "neutral"),
                },
            ],
            "internal_peer": {
                "available": internal_peer_available,
                "online": internal_peer_online,
                "update_available": internal_peer_update_available,
                "restart_required": internal_peer_restart_required,
                "status_text": internal_peer_status_text,
                "local_commit": internal_peer_commit,
                "last_command_source_commit": internal_peer_last_command_source_commit,
                "command": {
                    "active": internal_peer_command_active,
                    "action": internal_peer_command_action,
                    "status": internal_peer_command_status,
                    "source_commit": internal_peer_command_source_commit,
                    "message": _string(internal_peer_command.get("message", "")),
                },
            },
            "business_actions": {
                "allowed": business_actions_allowed,
                "reason_code": business_actions_reason_code,
                "disabled_reason": business_actions_disabled_reason,
                "status_text": business_actions_status_text,
            },
            "actions": {
                "main": _action(
                    main_action_id,
                    label=main_action_label,
                    allowed=main_action_allowed,
                    pending=False,
                    disabled_reason=main_action_disabled_reason,
                    reason_code=main_action_reason_code,
                ),
                "publish_approved": _action(
                    "publish_approved",
                    label=publish_label,
                    allowed=publish_allowed,
                    pending=False,
                    disabled_reason=publish_disabled_reason,
                    reason_code=publish_reason_code,
                ),
                "internal_peer_check": _action(
                    "internal_peer_check",
                    label=internal_peer_check_label,
                    allowed=internal_peer_check_allowed,
                    pending=False,
                    disabled_reason=internal_peer_check_disabled_reason,
                    reason_code=internal_peer_check_reason_code,
                ),
                "internal_peer_apply": _action(
                    "internal_peer_apply",
                    label=internal_peer_apply_label,
                    allowed=internal_peer_apply_allowed,
                    pending=False,
                    disabled_reason=internal_peer_apply_disabled_reason,
                    reason_code=internal_peer_apply_reason_code,
                ),
                "internal_peer_restart": _action(
                    "internal_peer_restart",
                    label=internal_peer_restart_label,
                    allowed=internal_peer_restart_allowed,
                    pending=False,
                    disabled_reason=internal_peer_restart_disabled_reason,
                    reason_code=internal_peer_restart_reason_code,
                ),
            },
        }
    if update_mode == "git_pull":
        published_commit = approved_commit or last_published_commit
        published_matches_local = bool(local_commit and published_commit and published_commit == local_commit)
        deferred_commit = (
            last_publish_deferred_commit
            if last_publish_deferred_commit and last_publish_deferred_commit != published_commit
            else ""
        )
        pending_sync_commit = deferred_commit or (
            local_commit if local_commit and not published_matches_local else ""
        )
        command_commit = internal_peer_command_source_commit or internal_peer_last_command_source_commit
        command_commit_text = _short_commit(command_commit)
        internal_peer_command_value = internal_peer_command_label
        if command_commit_text:
            internal_peer_command_value = f"{internal_peer_command_value} / {command_commit_text}"
        internal_peer_last_command_value = "-"
        if internal_peer_last_command_status or internal_peer_last_command_source_commit:
            last_action_text = "应用代码" if internal_peer_last_command_action == "apply" else (
                "检查状态" if internal_peer_last_command_action == "check" else (
                    "重启生效" if internal_peer_last_command_action == "restart" else "远程命令"
                )
            )
            last_status_text = (
                "成功"
                if internal_peer_last_command_status == "completed"
                else ("失败" if internal_peer_last_command_status == "failed" else internal_peer_last_command_status or "-")
            )
            internal_peer_last_command_value = f"{last_action_text} {last_status_text}"
            if internal_peer_last_command_source_commit:
                internal_peer_last_command_value = (
                    f"{internal_peer_last_command_value} / {_short_commit(internal_peer_last_command_source_commit)}"
                )
        tone = "info"
        status_text = "等待 Git 提交状态"
        summary_text = "手动 git pull 后，外网端会自动打包 Git 跟踪的 .py 文件并下发内网端应用；内网端不需要 Git。"
        badge_text = "代码同步 / .py only"
        if not updater_enabled:
            tone = "warning"
            if disabled_reason == "git_not_installed":
                status_text = "未安装 Git"
                summary_text = "外网端未安装 Git，无法检测手动 git pull 后的提交变化。"
            elif disabled_reason == "git_repo_missing":
                status_text = "当前目录不是 Git 仓库"
                summary_text = "当前代码目录缺少 .git，无法检测提交变化，也不会自动同步内网端。"
            elif disabled_reason == "git_remote_missing":
                status_text = "未配置 Git 远端"
                summary_text = "当前 Git 工作区没有可用远端，仍可展示本地提交，但不能判断远端状态。"
            else:
                status_text = "当前运行模式不支持代码同步"
                summary_text = _updater_disabled_reason_text(disabled_reason)
        elif restart_required:
            tone = "warning"
            status_text = "代码已更新，等待重启生效"
            summary_text = "当前端已应用或发布新代码，但还需要重启后才会运行新代码。"
        elif internal_peer_command_active and internal_peer_command_action == "apply":
            tone = "info"
            status_text = "内网端正在应用代码"
            summary_text = "已下发内网端应用命令，完成前不会覆盖共享目录源码包。"
        elif last_result == "git_pulling" or dependency_sync_status == "running":
            tone = "info"
            status_text = "代码更新处理中"
            summary_text = "正在处理代码更新或依赖同步，请保持当前页面打开。"
        elif worktree_dirty:
            tone = "warning"
            status_text = "检测到本地修改"
            summary_text = "当前 Git 工作区存在本地已修改文件，已阻止自动同步内网端。"
        elif deferred_commit:
            tone = "warning"
            status_text = "等待已有内网命令完成"
            summary_text = "检测到新的本地提交，但内网端已有待执行命令；完成后会再次尝试同步。"
        elif published_matches_local and internal_peer_commit and internal_peer_commit == local_commit:
            tone = "success"
            status_text = "内外端代码一致"
            summary_text = "外网当前提交已发布，内网端上报的运行提交也一致。"
        elif published_matches_local:
            tone = "success"
            status_text = "当前提交已发布"
            summary_text = "外网当前提交已打包到共享目录，并已下发或等待内网端应用。"
        elif pending_sync_commit:
            tone = "warning"
            status_text = "本地提交待同步"
            summary_text = "当前本地提交还没有发布到共享目录；updater 线程会自动尝试同步。"
        elif last_result == "up_to_date":
            tone = "success"
            status_text = "当前代码已是最新"
            summary_text = "当前 Git 工作区已经与远端保持一致，未检测到需要同步的提交。"
        items = [
            {"label": "同步方式", "value": "Git 跟踪 .py 文件", "tone": "info"},
            {
                "label": "旧版版本号",
                "value": f"{local_version} / r{local_revision}" if local_revision > 0 else local_version,
                "tone": "neutral",
            },
            {"label": "当前分支", "value": branch or "-", "tone": "neutral"},
            {"label": "外网当前提交", "value": _short_commit(local_commit) or "-", "tone": "neutral"},
            {"label": "远端提交", "value": _short_commit(remote_commit) or "-", "tone": "neutral"},
            {
                "label": "已发布提交",
                "value": _short_commit(published_commit) or "-",
                "tone": "success" if published_matches_local else "neutral",
            },
            {
                "label": "待同步提交",
                "value": _short_commit(pending_sync_commit) or "-",
                "tone": "warning" if pending_sync_commit else "neutral",
            },
            {
                "label": "最近发布",
                "value": last_publish_at or "-",
                "tone": "info" if last_publish_at else "neutral",
            },
            {
                "label": "工作区状态",
                "value": "存在本地修改" if worktree_dirty else "干净",
                "tone": "warning" if worktree_dirty else "success",
            },
            {
                "label": "内网端状态",
                "value": internal_peer_status_text,
                "tone": "warning" if internal_peer_command_active else ("success" if internal_peer_online else "neutral"),
            },
            {
                "label": "内网端提交",
                "value": _short_commit(internal_peer_commit) or ("待内网上报" if internal_peer_available else "-"),
                "tone": "success" if internal_peer_commit and internal_peer_commit == published_commit else "neutral",
            },
            {
                "label": "远程命令",
                "value": internal_peer_command_value,
                "tone": "warning" if internal_peer_command_active else "neutral",
            },
            {
                "label": "内网最近命令",
                "value": internal_peer_last_command_value,
                "tone": "danger" if internal_peer_last_command_status == "failed" else (
                    "success" if internal_peer_last_command_status == "completed" else "neutral"
                ),
            },
        ]
        if last_internal_apply_completed_commit:
            items.append(
                {
                    "label": "本端最近应用",
                    "value": _short_commit(last_internal_apply_completed_commit),
                    "tone": "success",
                }
            )
        if last_internal_apply_failed_commit:
            items.append(
                {
                    "label": "本端应用失败",
                    "value": _short_commit(last_internal_apply_failed_commit),
                    "tone": "danger",
                }
            )
        return {
            "tone": tone,
            "kicker": "代码同步",
            "title": "本机更新状态",
            "status_text": status_text,
            "badge_text": badge_text,
            "summary_text": summary_text,
            "manifest_path": manifest_path,
            "manifest_label": "源码包清单",
            "error_text": error_text,
            "items": items,
            "sync": {
                "mode": "git_py_only",
                "local_commit": local_commit,
                "remote_commit": remote_commit,
                "published_commit": published_commit,
                "pending_sync_commit": pending_sync_commit,
                "deferred_commit": deferred_commit,
                "last_publish_attempt_commit": last_publish_attempt_commit,
                "last_publish_command_id": last_publish_command_id,
                "internal_peer_commit": internal_peer_commit,
                "internal_peer_command_source_commit": internal_peer_command_source_commit,
                "internal_peer_last_command_source_commit": internal_peer_last_command_source_commit,
            },
            "internal_peer": {
                "available": internal_peer_available,
                "online": internal_peer_online,
                "update_available": internal_peer_update_available,
                "restart_required": internal_peer_restart_required,
                "status_text": internal_peer_status_text,
                "local_commit": internal_peer_commit,
                "last_command_source_commit": internal_peer_last_command_source_commit,
                "command": {
                    "active": internal_peer_command_active,
                    "action": internal_peer_command_action,
                    "status": internal_peer_command_status,
                    "source_commit": internal_peer_command_source_commit,
                    "message": _string(internal_peer_command.get("message", "")),
                },
            },
            "business_actions": {
                "allowed": business_actions_allowed,
                "reason_code": business_actions_reason_code,
                "disabled_reason": business_actions_disabled_reason,
                "status_text": business_actions_status_text,
            },
            "actions": {
                "main": _action(
                    main_action_id,
                    label=main_action_label,
                    allowed=main_action_allowed,
                    pending=False,
                    disabled_reason=main_action_disabled_reason,
                    reason_code=main_action_reason_code,
                ),
                "publish_approved": _action(
                    "publish_approved",
                    label=publish_label,
                    allowed=publish_allowed,
                    pending=False,
                    disabled_reason=publish_disabled_reason,
                    reason_code=publish_reason_code,
                ),
                "internal_peer_check": _action(
                    "internal_peer_check",
                    label=internal_peer_check_label,
                    allowed=internal_peer_check_allowed,
                    pending=False,
                    disabled_reason=internal_peer_check_disabled_reason,
                    reason_code=internal_peer_check_reason_code,
                ),
                "internal_peer_apply": _action(
                    "internal_peer_apply",
                    label=internal_peer_apply_label,
                    allowed=internal_peer_apply_allowed,
                    pending=False,
                    disabled_reason=internal_peer_apply_disabled_reason,
                    reason_code=internal_peer_apply_reason_code,
                ),
                "internal_peer_restart": _action(
                    "internal_peer_restart",
                    label=internal_peer_restart_label,
                    allowed=internal_peer_restart_allowed,
                    pending=False,
                    disabled_reason=internal_peer_restart_disabled_reason,
                    reason_code=internal_peer_restart_reason_code,
                ),
            },
        }
    git_items = []
    if update_mode == "git_pull":
        git_items = [
            {"label": "更新模式", "value": "Git 拉取代码", "tone": "info"},
            {"label": "当前分支", "value": branch or "-", "tone": "neutral"},
            {"label": "本地提交", "value": local_commit[:7] if local_commit else "-", "tone": "neutral"},
            {"label": "远端提交", "value": remote_commit[:7] if remote_commit else "-", "tone": "neutral"},
            {
                "label": "工作区状态",
                "value": "存在本地修改" if worktree_dirty else "干净",
                "tone": "warning" if worktree_dirty else "success",
            },
        ]
    tone = "neutral"
    status_text = "尚未发布到共享目录"
    summary_text = "当前更新链路尚未生成可供内网跟随的批准版本。"
    if source_kind in {"shared_mirror", "shared_approved_source"}:
        if mirror_ready:
            tone = "success"
            status_text = "已检测到共享目录批准版本"
            summary_text = (
                "当前使用共享目录批准源码更新源，不访问互联网；点击“应用共享更新”后才会应用。"
                if source_kind == "shared_approved_source"
                else "当前使用共享目录更新源，不访问互联网；检测到新批准版本后会自动跟随。"
            )
        elif error_text:
            tone = "danger"
            status_text = "共享目录更新源异常"
            summary_text = "当前使用共享目录更新源，但镜像读取失败，请先检查共享目录可访问性。"
        else:
            tone = "warning"
            status_text = "等待外网端发布批准版本"
            summary_text = "当前使用共享目录更新源，不访问互联网；外网端发布批准版本后会自动跟随。"
    elif mirror_ready:
        tone = "success"
        status_text = "已发布批准版本到共享目录"
        summary_text = "外网端已把当前已验证版本发布到共享目录，内网端可自动跟随。"
    elif error_text:
        tone = "danger"
        status_text = "共享目录发布失败"
        summary_text = "当前仍使用远端正式更新源，但最近一次共享目录镜像发布失败。"
    else:
        tone = "warning"
        status_text = "尚未发布批准版本"
        summary_text = "当前仍使用远端正式更新源；完成验证后会把批准版本发布到共享目录。"
    if internal_peer_command_active:
        waiting_text = "内网端已在线，正在处理远程命令。" if internal_peer_online else "内网端当前离线，待上线后会自动执行远程命令。"
        if internal_peer_command_action == "check":
            summary_text = f"{waiting_text} 检查完成后才会刷新“远程版本 / 最近检查 / 内网可更新”，完成前“开始更新”按钮保持不可点击。"
        elif internal_peer_command_action == "apply":
            summary_text = f"{waiting_text} 开始更新命令完成前，不再接受新的远程更新命令。"
        else:
            summary_text = waiting_text
    internal_peer_update_tone = (
        "warning"
        if internal_peer_update_available
        else (
            "info"
            if (internal_peer_command_active and internal_peer_command_action == "check")
            or (not internal_peer_check_at and not internal_peer_last_result)
            else "neutral"
        )
    )
    items = [
        {"label": "更新源", "value": source_label, "tone": "warning" if source_kind == "shared_mirror" else "info"},
        {
            "label": "本机版本",
            "value": f"{local_version} / r{local_revision}" if local_revision > 0 else local_version,
            "tone": "neutral",
        },
        {
            "label": "批准版本号",
            "value": mirror_version or ("待外网端发布" if source_kind == "shared_mirror" else "尚未发布"),
            "tone": "success" if mirror_ready else "neutral",
        },
        {
            "label": "共享目录更新时间",
            "value": last_publish_at or "-",
            "tone": "info" if last_publish_at else "neutral",
        },
        {
            "label": "内网端状态",
            "value": internal_peer_status_text,
            "tone": "warning" if internal_peer_command_active else ("success" if internal_peer_online else "neutral"),
        },
        {"label": "远程版本", "value": internal_peer_version_text, "tone": "neutral"},
        {"label": "内网端最近检查", "value": internal_peer_check_text, "tone": "neutral"},
        {
            "label": "远程命令",
            "value": internal_peer_command_label,
            "tone": "warning" if internal_peer_command_active else "neutral",
        },
        {
            "label": "内网可更新",
            "value": internal_peer_update_status_text,
            "tone": internal_peer_update_tone,
        },
    ]
    if git_items:
        items = git_items + items
    return {
        "tone": tone,
        "kicker": "更新镜像",
        "title": "共享目录批准版本",
        "status_text": status_text,
        "badge_text": status_text,
        "summary_text": summary_text,
        "manifest_path": manifest_path,
        "manifest_label": "镜像清单",
        "error_text": error_text,
        "items": items,
        "internal_peer": {
            "available": internal_peer_available,
            "online": internal_peer_online,
            "update_available": internal_peer_update_available,
            "restart_required": internal_peer_restart_required,
            "status_text": internal_peer_status_text,
            "local_commit": internal_peer_commit,
            "last_command_source_commit": internal_peer_last_command_source_commit,
            "command": {
                "active": internal_peer_command_active,
                "action": internal_peer_command_action,
                "status": internal_peer_command_status,
                "source_commit": internal_peer_command_source_commit,
                "message": _string(internal_peer_command.get("message", "")),
            },
        },
        "business_actions": {
            "allowed": business_actions_allowed,
            "reason_code": business_actions_reason_code,
            "disabled_reason": business_actions_disabled_reason,
            "status_text": business_actions_status_text,
        },
        "actions": {
            "main": _action(
                main_action_id,
                label=main_action_label,
                allowed=main_action_allowed,
                pending=False,
                disabled_reason=main_action_disabled_reason,
                reason_code=main_action_reason_code,
            ),
            "publish_approved": _action(
                "publish_approved",
                label=publish_label,
                allowed=publish_allowed,
                pending=False,
                disabled_reason=publish_disabled_reason,
                reason_code=publish_reason_code,
            ),
            "internal_peer_check": _action(
                "internal_peer_check",
                label=internal_peer_check_label,
                allowed=internal_peer_check_allowed,
                pending=False,
                disabled_reason=internal_peer_check_disabled_reason,
                reason_code=internal_peer_check_reason_code,
            ),
            "internal_peer_apply": _action(
                "internal_peer_apply",
                label=internal_peer_apply_label,
                allowed=internal_peer_apply_allowed,
                pending=False,
                disabled_reason=internal_peer_apply_disabled_reason,
                reason_code=internal_peer_apply_reason_code,
            ),
            "internal_peer_restart": _action(
                "internal_peer_restart",
                label=internal_peer_restart_label,
                allowed=internal_peer_restart_allowed,
                pending=False,
                disabled_reason=internal_peer_restart_disabled_reason,
                reason_code=internal_peer_restart_reason_code,
            ),
        },
    }


def present_shared_root_diagnostic_overview(payload: Any) -> Dict[str, Any]:
    diagnostic = payload if isinstance(payload, dict) else {}
    raw_items = _list(diagnostic.get("items", []))
    raw_paths = _list(diagnostic.get("paths", []))
    raw_notes = _list(diagnostic.get("notes", []))
    items = [
        {
            "label": _string(item.get("label", "")) or "-",
            "value": _string(item.get("value", "")) or "-",
            "tone": _string(item.get("tone", "")) or "neutral",
        }
        for item in raw_items
        if isinstance(item, dict)
    ]
    paths = [
        {
            "label": _string(item.get("label", "")) or "-",
            "path": _string(item.get("path", "")) or "未配置",
            "canonical_path": _string(item.get("canonical_path", "")),
            "show_canonical_path": (
                bool(_string(item.get("canonical_path", "")))
                and _string(item.get("canonical_path", "")) != _string(item.get("path", ""))
            ),
        }
        for item in raw_paths
        if isinstance(item, dict)
    ]
    notes = [_string(item) for item in raw_notes if _string(item)]
    tone = _string(diagnostic.get("tone", "")) or "neutral"
    status_text = _string(diagnostic.get("status_text", "")) or "未诊断"
    summary_text = (
        _string(diagnostic.get("summary_text", ""))
        or "当前还没有共享目录一致性诊断结果。"
    )
    return {
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "detail_text": summary_text,
        "reason_code": _string(diagnostic.get("status", "")) or "unknown",
        "kicker": "共享目录诊断",
        "title": "共享目录一致性",
        "items": items,
        "paths": paths,
        "notes": notes,
        "actions": {},
    }


def present_internal_runtime_display(summary: Any, *, task_overview: Any = None) -> Dict[str, Any]:
    payload = summary if isinstance(summary, dict) else {}
    source_cache = payload.get("source_cache", {}) if isinstance(payload.get("source_cache", {}), dict) else {}
    pool = payload.get("pool", {}) if isinstance(payload.get("pool", {}), dict) else {}
    source_cache_overview = source_cache.get("overview", {}) if isinstance(source_cache.get("overview", {}), dict) else {}
    pool_overview = pool.get("overview", {}) if isinstance(pool.get("overview", {}), dict) else {}
    alarm_family = source_cache.get("alarm_event_family", {}) if isinstance(source_cache.get("alarm_event_family", {}), dict) else {}
    current_hour = (
        source_cache.get("current_hour_refresh_overview", {})
        if isinstance(source_cache.get("current_hour_refresh_overview", {}), dict)
        else {}
    )
    task = _normalize_task_overview(task_overview)
    current_hour_pending = _string(current_hour.get("status_text", "")) == "下载中"
    manual_alarm_payload = alarm_family.get("manual_refresh", {}) if isinstance(alarm_family.get("manual_refresh", {}), dict) else {}
    manual_alarm_pending = bool(manual_alarm_payload.get("running", False))
    home_overview = {
        "tone": _string(source_cache_overview.get("tone", "")) or "neutral",
        "status_text": _string(source_cache_overview.get("status_text", "")) or "等待内网运行态",
        "summary_text": _string(source_cache_overview.get("summary_text", "")) or "内网端首页应优先关注浏览器池、共享文件和当前小时刷新。",
        "next_action_text": (
            "先处理最近异常，再决定是否重新下载当前小时文件。"
            if _string(source_cache_overview.get("error_text", ""))
            else "先看浏览器池和共享文件是否健康，再执行手动动作。"
        ),
        "items": [
            {"label": "共享文件", "value": _string(source_cache_overview.get("status_text", "")) or "-", "tone": _string(source_cache_overview.get("tone", "")) or "neutral"},
            {"label": "浏览器池", "value": _string(pool_overview.get("status_text", "")) or "-", "tone": _string(pool_overview.get("tone", "")) or "neutral"},
            {"label": "当前轮次", "value": _string(current_hour.get("status_text", "")) or "-", "tone": _string(current_hour.get("tone", "")) or "neutral"},
            {"label": "当前任务", "value": task["status_text"], "tone": task["tone"]},
        ],
        "actions": [
            _action(
                "refresh_current_hour",
                label="下载中..." if current_hour_pending else "立即下载当前小时全部文件",
                desc="立即刷新当前小时四组共享文件",
                allowed=not current_hour_pending,
                pending=current_hour_pending,
                disabled_reason="当前小时共享文件正在下载",
            ),
            _action(
                "refresh_manual_alarm",
                label="拉取中..." if manual_alarm_pending else "一键拉取告警文件",
                desc="单独拉取近 60 天告警 JSON",
                allowed=not manual_alarm_pending,
                pending=manual_alarm_pending,
                disabled_reason="当前正在拉取告警文件",
            ),
            _action(
                "open_config",
                label="打开本地配置",
                desc="检查共享目录、浏览器池和桥接参数",
            ),
        ],
    }

    slots = [item for item in _list(pool_overview.get("slots", [])) if isinstance(item, dict)]
    problem_slots = [
        slot
        for slot in slots
        if bool(slot.get("suspended", False)) or _string(slot.get("login_state", "")).lower() == "failed"
    ]
    families = [item for item in _list(source_cache_overview.get("families", [])) if isinstance(item, dict)]
    problem_families = [
        family
        for family in families
        if bool(family.get("has_failures", False)) or bool(family.get("has_blocked", False))
    ]
    failed_buildings = _list(current_hour.get("failed_buildings", []))
    blocked_buildings = _list(current_hour.get("blocked_buildings", []))
    failure_text = (
        _string(current_hour.get("last_error", ""))
        or _string(pool_overview.get("error_text", ""))
        or _string(source_cache_overview.get("error_text", ""))
    )
    has_current_failure = bool(
        failed_buildings or blocked_buildings or problem_slots or problem_families
    )
    tone = _string(source_cache_overview.get("tone", "")) or "neutral"
    status_text = _string(source_cache_overview.get("status_text", "")) or "等待内网运行态"
    reason_text = _string(source_cache_overview.get("summary_text", "")) or "当前没有足够的内网运行态摘要。"
    action_text = "先确认浏览器池和共享目录是否正常，再决定是否触发下载。"
    if has_current_failure:
        tone = "danger"
        status_text = "当前有需要人工处理的问题"
        if failed_buildings or blocked_buildings:
            reason_text = _string(current_hour.get("summary_text", "")) or failure_text or reason_text
        elif problem_slots:
            slot = problem_slots[0]
            reason_text = f"{_string(slot.get('building', '-'))} {_string(slot.get('detail_text', '')) or _string(slot.get('status_text', '当前楼栋状态异常'))}"
        elif problem_families:
            family = problem_families[0]
            reason_text = _string(family.get("status_text", "")) or failure_text or reason_text
        else:
            reason_text = failure_text or reason_text
        action_text = "优先看失败楼栋或登录失败楼，再决定是否重新下载当前小时或手动拉取告警。"
    elif _string(current_hour.get("tone", "")) in {"warning", "info"}:
        tone = _string(current_hour.get("tone", "")) or "warning"
        status_text = _string(current_hour.get("status_text", "")) or "当前共享文件仍在推进"
        reason_text = _string(current_hour.get("summary_text", "")) or reason_text
        action_text = "先等待本轮执行结束；需要抢修时再用手动拉取。"
    status_diagnosis_overview = {
        "tone": tone,
        "status_text": status_text,
        "reason_text": reason_text,
        "action_text": action_text,
        "items": home_overview["items"],
        "actions": [
            _action(
                "refresh_current_hour",
                label="下载中..." if current_hour_pending else "立即下载当前小时全部文件",
                allowed=not current_hour_pending,
                pending=current_hour_pending,
                disabled_reason="当前小时共享文件正在下载",
            ),
            _action(
                "refresh_manual_alarm",
                label="拉取中..." if manual_alarm_pending else "一键拉取告警文件",
                allowed=not manual_alarm_pending,
                pending=manual_alarm_pending,
                disabled_reason="当前正在拉取告警文件",
            ),
            _action("open_config", label="打开本地配置"),
        ],
    }
    history_tone = "neutral"
    history_status_text = "暂无历史"
    history_summary_text = "这里只保留最近调度、最近成功、最近错误和共享缓存日志，不再重复展示当前实时状态。"
    if _string(current_hour.get("tone", "")) == "danger" or _string(source_cache_overview.get("tone", "")) == "danger":
        history_tone = "danger"
        history_status_text = "最近存在失败"
        history_summary_text = "最近一轮小时下载或手动补下存在失败，请检查对应楼栋登录态、共享目录权限和下载页面可用性。"
    elif _string(source_cache.get("last_success_at", "")) or _string(current_hour.get("last_success_at", "")):
        history_tone = "success"
        history_status_text = "最近调度正常"
        history_summary_text = "最近一次共享缓存调度和手动补下已完成，可在这里查看历史时间点和最近日志。"
    elif _string(source_cache.get("last_run_at", "")) or _string(current_hour.get("last_run_at", "")):
        history_tone = "warning"
        history_status_text = "已有历史记录"
        history_summary_text = "最近已有共享缓存调度记录，当前卡片只保留历史摘要和最近日志。"
    history_overview = {
        "tone": history_tone,
        "status_text": history_status_text,
        "summary_text": history_summary_text,
        "detail_text": "历史卡片只展示后端聚合后的最近时间点和最近错误，不再回退前端本地日志拼装。",
        "last_error": (
            _string(current_hour.get("last_error", ""))
            or _string(source_cache_overview.get("error_text", ""))
        ),
        "items": [
            {
                "label": "当前小时桶",
                "value": _string(source_cache.get("current_hour_bucket", "")) or "-",
                "tone": "info" if _string(source_cache.get("current_hour_bucket", "")) else "neutral",
            },
            {
                "label": "最近小时调度",
                "value": _string(source_cache.get("last_run_at", "")) or "-",
                "tone": "info" if _string(source_cache.get("last_run_at", "")) else "neutral",
            },
            {
                "label": "最近小时成功",
                "value": _string(source_cache.get("last_success_at", "")) or "-",
                "tone": "success" if _string(source_cache.get("last_success_at", "")) else "neutral",
            },
            {
                "label": "当前小时最近触发",
                "value": _string(current_hour.get("last_run_at", "")) or "-",
                "tone": "warning" if _string(current_hour.get("last_run_at", "")) else "neutral",
            },
            {
                "label": "当前小时最近完成",
                "value": _string(current_hour.get("last_success_at", "")) or "-",
                "tone": "success" if _string(current_hour.get("last_success_at", "")) else "neutral",
            },
            {
                "label": "最近错误",
                "value": (
                    _string(current_hour.get("last_error", ""))
                    or _string(source_cache_overview.get("error_text", ""))
                    or "-"
                ),
                "tone": (
                    "danger"
                    if (
                        _string(current_hour.get("last_error", ""))
                        or _string(source_cache_overview.get("error_text", ""))
                    )
                    else "neutral"
                ),
            },
        ],
        "actions": {},
    }
    runtime_overview = {
        "tone": _string(source_cache_overview.get("tone", "")) or "neutral",
        "status_text": _string(source_cache_overview.get("status_text", "")) or "等待内网运行态",
        "summary_text": _string(source_cache_overview.get("summary_text", "")) or "内网端首页应优先关注浏览器池、共享文件和当前小时刷新。",
        "items": [item for item in _list(source_cache_overview.get("items", [])) if isinstance(item, dict)],
        "cache_root": _string(source_cache.get("cache_root", "")) or _string(source_cache_overview.get("cache_root", "")),
        "error_text": _string(source_cache_overview.get("error_text", "")),
        "pool_status_text": _string(pool_overview.get("status_text", "")),
        "pool_summary_text": _string(pool_overview.get("summary_text", "")),
        "pool_items": [item for item in _list(pool_overview.get("items", [])) if isinstance(item, dict)],
        "pool_error_text": _string(pool_overview.get("error_text", "")),
        "slots": slots,
        "current_hour_refresh": current_hour,
        "families": families,
    }
    return {
        "current_task_overview": task,
        "home_overview": home_overview,
        "status_diagnosis_overview": status_diagnosis_overview,
        "history_overview": history_overview,
        "runtime_overview": runtime_overview,
    }


def present_internal_runtime_building_display(status: Any) -> Dict[str, Any]:
    payload = status if isinstance(status, dict) else {}
    building = _string(payload.get("building", "")) or "-"
    page_slot = payload.get("page_slot", {}) if isinstance(payload.get("page_slot", {}), dict) else {}
    source_families = payload.get("source_families", {}) if isinstance(payload.get("source_families", {}), dict) else {}

    def _derive_slot_display(slot: Dict[str, Any]) -> Dict[str, str]:
        explicit_status_text = _string(slot.get("status_text", ""))
        explicit_tone = _string(slot.get("tone", ""))
        explicit_detail = _string(slot.get("detail_text", ""))
        explicit_login_text = _string(slot.get("login_text", ""))
        explicit_login_tone = _string(slot.get("login_tone", ""))
        if explicit_status_text:
            return {
                "status_text": explicit_status_text,
                "tone": explicit_tone or "neutral",
                "detail_text": explicit_detail,
                "login_text": explicit_login_text or "等待后端状态",
                "login_tone": explicit_login_tone or "neutral",
                "reason_code": _string(slot.get("status_key", "")) or "unknown",
            }

        login_state = _string(slot.get("login_state", "")).lower()
        if explicit_login_text:
            login_text = explicit_login_text
            login_tone = explicit_login_tone or "neutral"
        elif login_state == "ready":
            login_text = "已登录"
            login_tone = "success"
        elif login_state == "logging_in":
            login_text = "登录中"
            login_tone = "info"
        elif login_state == "expired":
            login_text = "登录失效"
            login_tone = "warning"
        elif login_state == "failed":
            login_text = "登录失败"
            login_tone = "danger"
        else:
            login_text = "等待后端状态"
            login_tone = "neutral"

        if bool(slot.get("suspended")):
            return {
                "status_text": "已暂停等待恢复",
                "tone": "warning",
                "detail_text": _string(slot.get("suspend_reason", "")) or explicit_detail,
                "login_text": login_text,
                "login_tone": login_tone,
                "reason_code": "suspended",
            }
        if bool(slot.get("in_use")) or _string(slot.get("last_result", "")).lower() == "running":
            return {
                "status_text": "使用中",
                "tone": "info",
                "detail_text": explicit_detail or "当前楼页签正在被下载或补采任务占用。",
                "login_text": login_text,
                "login_tone": login_tone,
                "reason_code": "running",
            }
        if bool(slot.get("page_ready")):
            return {
                "status_text": "待命",
                "tone": "success",
                "detail_text": explicit_detail or "页签已就绪，等待下载任务。",
                "login_text": login_text,
                "login_tone": login_tone,
                "reason_code": "ready",
            }
        if login_state == "logging_in":
            return {
                "status_text": "登录中",
                "tone": "info",
                "detail_text": explicit_detail or "当前楼页签正在准备登录态。",
                "login_text": login_text,
                "login_tone": login_tone,
                "reason_code": "logging_in",
            }
        if login_state == "failed":
            return {
                "status_text": "登录失败",
                "tone": "danger",
                "detail_text": _string(slot.get("login_error", "")) or explicit_detail,
                "login_text": login_text,
                "login_tone": login_tone,
                "reason_code": "login_failed",
            }
        return {
            "status_text": "等待后端状态",
            "tone": "neutral",
            "detail_text": explicit_detail or "当前楼页签状态由后端实时汇总。",
            "login_text": login_text,
            "login_tone": login_tone,
            "reason_code": "unknown",
        }

    def _derive_family_display(row: Dict[str, Any]) -> Dict[str, str]:
        explicit_status_text = _string(row.get("status_text", ""))
        explicit_tone = _string(row.get("tone", ""))
        explicit_detail = _string(row.get("detail_text", ""))
        if explicit_status_text:
            return {
                "status_text": explicit_status_text,
                "tone": explicit_tone or "neutral",
                "detail_text": explicit_detail,
            }

        normalized_status = _string(row.get("status", "")).lower()
        if bool(row.get("blocked")):
            return {
                "status_text": "等待内网恢复",
                "tone": "warning",
                "detail_text": _string(row.get("blocked_reason", "")) or explicit_detail,
            }
        if normalized_status == "failed":
            return {
                "status_text": "失败",
                "tone": "danger",
                "detail_text": _string(row.get("last_error", "")) or explicit_detail,
            }
        if normalized_status == "downloading":
            return {
                "status_text": "下载中",
                "tone": "info",
                "detail_text": explicit_detail,
            }
        if normalized_status == "consumed":
            return {
                "status_text": "已消费",
                "tone": "neutral",
                "detail_text": explicit_detail,
            }
        if normalized_status == "ready" or bool(row.get("ready")):
            return {
                "status_text": "已就绪",
                "tone": "success",
                "detail_text": explicit_detail,
            }
        return {
            "status_text": "等待中",
            "tone": "warning",
            "detail_text": explicit_detail,
        }

    family_titles = {
        "handover_log_family": "交接班日志源文件",
        "handover_capacity_report_family": "交接班容量报表源文件",
        "monthly_report_family": "全景平台月报源文件",
        "branch_power_family": "支路功率源文件",
        "alarm_event_family": "告警信息源文件",
    }
    family_items = []
    family_map: Dict[str, Any] = {}
    for key, title in family_titles.items():
        raw_row = source_families.get(key, {}) if isinstance(source_families.get(key, {}), dict) else {}
        display = _derive_family_display(raw_row)
        row = {
            **raw_row,
            "key": key,
            "title": title,
            "tone": display["tone"],
            "status_text": display["status_text"],
            "detail_text": display["detail_text"],
        }
        family_items.append(row)
        family_map[key] = row

    slot_display = _derive_slot_display(page_slot)
    slot_tone = slot_display["tone"]
    slot_status_text = slot_display["status_text"]
    slot_detail_text = slot_display["detail_text"]
    login_text = slot_display["login_text"]
    login_tone = slot_display["login_tone"]

    items = [
        {
            "label": "楼栋页签",
            "value": slot_status_text,
            "tone": slot_tone,
        },
        {
            "label": "登录状态",
            "value": login_text,
            "tone": login_tone,
        },
        *[
            {
                "label": title,
                "value": family_map[key]["status_text"],
                "tone": family_map[key]["tone"],
            }
            for key, title in family_titles.items()
        ],
    ]
    return {
        "building": building,
        "tone": slot_tone,
        "status_text": slot_status_text,
        "summary_text": slot_detail_text,
        "reason_code": slot_display["reason_code"],
        "page_slot": page_slot,
        "source_families": family_map,
        "families": family_items,
        "items": items,
    }


def present_external_dashboard_display(
    *,
    shared_source_cache_overview: Any,
    review_status: Any,
    review_links: Any = None,
    review_recipient_status_by_building: Any = None,
    task_overview: Any = None,
    shared_root_diagnostic: Any = None,
) -> Dict[str, Any]:
    cache = shared_source_cache_overview if isinstance(shared_source_cache_overview, dict) else {}
    review = present_handover_review_overview(
        review_status,
        review_links=review_links,
        recipient_status_by_building=review_recipient_status_by_building,
    )
    alarm = present_alarm_upload_overview(cache)
    task = _normalize_task_overview(task_overview)
    shared_root = present_shared_root_diagnostic_overview(shared_root_diagnostic)

    tone = "success"
    status_text = "可以继续外网主流程"
    summary_text = "共享文件已就绪，当前可以直接进入自动流程、交接班或告警上传。"
    next_action_text = "优先从“每日用电明细自动流程”开始；需要专项处理时再进入交接班日志或告警信息上传。"
    if not bool(cache.get("can_proceed_latest", False)):
        tone = _string(cache.get("tone", "")) or "warning"
        status_text = _string(cache.get("status_text", "")) or "等待共享文件就绪"
        summary_text = _string(cache.get("summary_text", "")) or "共享文件还没准备好，先不要急着做外网上传。"
        next_action_text = "先去状态总览确认缺哪一组文件、哪几个楼还在等待。"
    elif review.get("has_any_session") and not review.get("all_confirmed"):
        tone = _string(review.get("tone", "")) or "warning"
        status_text = "当前批次还有待确认楼栋"
        summary_text = _string(review.get("summary_text", "")) or "交接班批次还没完成确认。"
        next_action_text = "先处理交接班确认，再继续后续云表或派生上传动作。"
    elif _string(alarm.get("tone", "")) == "danger":
        tone = "warning"
        status_text = "最近专项上传有异常"
        summary_text = _string(alarm.get("summary_text", "")) or "最近专项上传失败，但共享源文件仍保留。"
        next_action_text = "进入告警信息上传模块看任务摘要，不要只盯卡片提示。"
    elif _string(task.get("tone", "")) in {"info", "warning"}:
        tone = task["tone"]
        status_text = task["status_text"]
        summary_text = task["summary_text"]
        next_action_text = task["next_action_text"]
    home_overview = {
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "next_action_text": next_action_text,
        "items": [
            {"label": "共享文件", "value": _string(cache.get("status_text", "")) or "-", "tone": _string(cache.get("tone", "")) or "neutral"},
            {"label": "交接班确认", "value": _string(review.get("summary_text", "")) or "当前无待确认批次", "tone": _string(review.get("tone", "")) or "neutral"},
            {"label": "告警上传", "value": _string(alarm.get("status_text", "")) or "-", "tone": _string(alarm.get("tone", "")) or "neutral"},
            {"label": "当前任务", "value": task["status_text"], "tone": task["tone"]},
        ],
        "actions": [
            _action("open_auto_flow", label="每日用电明细自动流程", desc="从共享文件主链开始执行外网默认流程"),
            _action("open_handover_log", label="交接班处理", desc="处理审核、回补和交接班后续上传"),
            _action("open_alarm_upload", label="告警上传", desc="检查今天最新告警文件并执行 60 天上传"),
        ],
    }

    tone = "success"
    status_text = "外网链路可继续执行"
    reason_text = "共享文件已就绪，没有发现需要先处理的阻塞。"
    action_text = "优先使用自动流程；需要专项处理时再进入交接班或告警上传模块。"
    if not bool(cache.get("can_proceed_latest", False)):
        tone = _string(cache.get("tone", "")) or "warning"
        status_text = _string(cache.get("status_text", "")) or "等待共享文件就绪"
        reason_text = _string(cache.get("summary_text", "")) or "共享文件还不完整。"
        action_text = "先看最新共享文件就绪情况，确认缺失楼栋和等待原因。"
    elif review.get("has_any_session") and not review.get("all_confirmed"):
        tone = _string(review.get("tone", "")) or "warning"
        status_text = "当前批次还有待确认楼栋"
        reason_text = _string(review.get("summary_text", "")) or "交接班确认未结束。"
        action_text = "先完成交接班确认，再执行后续上传或派生动作。"
    elif _string(alarm.get("tone", "")) == "danger":
        tone = "warning"
        status_text = "最近告警上传异常"
        reason_text = _string(alarm.get("summary_text", "")) or "最近一次告警上传失败。"
        action_text = "进入告警上传模块查看任务摘要，文件状态本身仍以 ready 为准。"
    status_diagnosis_overview = {
        "tone": tone,
        "status_text": status_text,
        "reason_text": reason_text,
        "action_text": action_text,
        "items": home_overview["items"],
        "actions": [
            _action("open_auto_flow", label="进入自动流程"),
            _action("open_handover_log", label="进入交接班"),
            _action("open_alarm_upload", label="进入告警上传"),
        ],
    }
    return {
        "current_task_overview": task,
        "handover_review_overview": review,
        "alarm_upload_overview": alarm,
        "shared_root_diagnostic_overview": shared_root,
        "home_overview": home_overview,
        "status_diagnosis_overview": status_diagnosis_overview,
    }


def present_external_system_overview(
    *,
    health_lite: Any = None,
    runtime_resources_summary: Any = None,
    task_overview: Any = None,
    shared_root_diagnostic: Any = None,
    updater_overview: Any = None,
) -> Dict[str, Any]:
    health = health_lite if isinstance(health_lite, dict) else {}
    runtime_resources = runtime_resources_summary if isinstance(runtime_resources_summary, dict) else {}
    task = task_overview if isinstance(task_overview, dict) else {}
    shared_root = shared_root_diagnostic if isinstance(shared_root_diagnostic, dict) else {}
    updater = updater_overview if isinstance(updater_overview, dict) else {}
    deployment = _dict(health.get("deployment"))
    network = _dict(runtime_resources.get("network"))
    role_mode = _normalize_role_mode(deployment.get("role_mode", ""))
    role_label = _format_role_label(role_mode)
    current_ssid = _string(network.get("current_ssid", "")) or "未识别"

    tone = "success"
    status_text = "当前运行环境已就绪"
    summary_text = "角色、网络、共享目录和当前任务状态均已由后端聚合。"
    detail_text = ""
    if _string(shared_root.get("tone", "")) in {"warning", "danger"}:
        tone = _string(shared_root.get("tone", "")) or "warning"
        status_text = _string(shared_root.get("status_text", "")) or "共享目录需要关注"
        summary_text = _string(shared_root.get("summary_text", "")) or "共享目录诊断提示当前环境还有待处理项。"
        detail_text = _string(shared_root.get("detail_text", "")) or _string(shared_root.get("summary_text", ""))
    elif _string(task.get("tone", "")) in {"warning", "danger", "info"}:
        tone = _string(task.get("tone", "")) or "info"
        status_text = _string(task.get("status_text", "")) or "当前任务执行中"
        summary_text = _string(task.get("summary_text", "")) or "当前环境可继续，但请先关注任务状态。"
        detail_text = _string(task.get("detail_text", "")) or _string(task.get("next_action_text", ""))

    items = [
        {
            "label": "当前角色",
            "value": role_label,
            "tone": "info" if role_mode else "neutral",
        },
        {
            "label": "当前网络",
            "value": current_ssid,
            "tone": "info" if current_ssid != "未识别" else "neutral",
        },
        {
            "label": "当前任务",
            "value": _string(task.get("status_text", "")) or "当前空闲",
            "tone": _string(task.get("tone", "")) or "neutral",
        },
        {
            "label": "共享目录",
            "value": _string(shared_root.get("status_text", "")) or "等待后端诊断",
            "tone": _string(shared_root.get("tone", "")) or "neutral",
        },
    ]
    updater_text = _string(updater.get("status_text", ""))
    if updater_text:
        items.append(
            {
                "label": "更新镜像",
                "value": updater_text,
                "tone": _string(updater.get("tone", "")) or "neutral",
            }
        )

    return {
        "kicker": "系统与网络",
        "title": "当前运行环境",
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "detail_text": detail_text,
        "reason_code": "environment_ready" if tone == "success" else "environment_attention",
        "items": items,
        "actions": [],
    }


def present_external_scheduler_overview(
    *,
    scheduler_overview_summary: Any = None,
    scheduler_overview_items: Any = None,
) -> Dict[str, Any]:
    summary = scheduler_overview_summary if isinstance(scheduler_overview_summary, dict) else {}
    raw_items = _list(scheduler_overview_items)
    items = [
        {
            "label": _string(item.get("label", "")) or "-",
            "value": _string(item.get("value", "")) or "-",
            "tone": _string(item.get("tone", "")) or "neutral",
        }
        for item in _list(summary.get("items"))
        if isinstance(item, dict)
    ]
    if not items:
        items = [
            {
                "label": "已启动调度",
                "value": f"{_int(summary.get('running_count', 0))} 项",
                "tone": "success" if _int(summary.get("running_count", 0)) > 0 else "neutral",
            },
            {
                "label": "未启动调度",
                "value": f"{_int(summary.get('stopped_count', 0))} 项",
                "tone": "warning" if _int(summary.get("stopped_count", 0)) > 0 else "neutral",
            },
            {
                "label": "待关注项",
                "value": f"{_int(summary.get('attention_count', 0))} 项",
                "tone": "warning" if _int(summary.get("attention_count", 0)) > 0 else "neutral",
            },
        ]
    detail_text = _string(summary.get("detail_text", ""))
    if not detail_text and raw_items:
        detail_text = "调度详情已由后端聚合，可进入对应模块查看单项配置与动作。"
    return {
        "kicker": "调度状态",
        "title": "月报与交接班调度",
        "tone": _string(summary.get("tone", "")) or "neutral",
        "status_text": _string(summary.get("status_text", "")) or "等待后端调度状态",
        "summary_text": _string(summary.get("summary_text", "")) or "调度状态由后端聚合后返回。",
        "detail_text": detail_text,
        "reason_code": _string(summary.get("reason_code", "")),
        "items": items,
        "actions": [
            action
            for action in _list(summary.get("actions"))
            if isinstance(action, dict)
        ],
    }


def present_external_module_hero_overviews(
    *,
    scheduler_overview_summary: Any = None,
    scheduler_status_summary: Any = None,
    review_status: Any = None,
    shared_source_cache_overview: Any = None,
    runtime_resources_summary: Any = None,
    job_panel_summary: Any = None,
    feature_target_displays: Any = None,
) -> Dict[str, Any]:
    scheduler_summary = scheduler_overview_summary if isinstance(scheduler_overview_summary, dict) else {}
    scheduler_status = scheduler_status_summary if isinstance(scheduler_status_summary, dict) else {}
    review_overview = present_handover_review_overview(review_status)
    source_cache = shared_source_cache_overview if isinstance(shared_source_cache_overview, dict) else {}
    runtime_resources = runtime_resources_summary if isinstance(runtime_resources_summary, dict) else {}
    job_panel = job_panel_summary if isinstance(job_panel_summary, dict) else {}
    target_displays = feature_target_displays if isinstance(feature_target_displays, dict) else {}
    families = _list(source_cache.get("families", []))
    alarm_family = next(
        (
            item
            for item in families
            if isinstance(item, dict) and _string(item.get("key", "")) == "alarm_event_family"
        ),
        {},
    )
    runtime_network = _dict(runtime_resources.get("network"))
    job_panel_display = _dict(job_panel.get("display"))
    task_overview = _dict(job_panel_display.get("overview"))
    current_ssid = _string(runtime_network.get("current_ssid", "")) or "-"
    task_status_text = _string(task_overview.get("status_text", "")) or "当前空闲"
    running_count = _int(task_overview.get("running_count", 0))
    waiting_count = _int(task_overview.get("waiting_count", 0))
    bridge_active_count = _int(task_overview.get("bridge_active_count", 0))
    day_metric_target = _dict(target_displays.get("day_metric_upload"))
    wet_bulb_target = _dict(target_displays.get("wet_bulb_collection"))

    def _scheduler_metric(snapshot: Any, default_status: str = "-") -> Dict[str, Any]:
        payload = snapshot if isinstance(snapshot, dict) else {}
        display = payload.get("display", {}) if isinstance(payload.get("display", {}), dict) else {}
        return {
            "status_text": _string(display.get("status_text", "")) or _string(payload.get("status", "")) or default_status,
            "next_run_time": _string(payload.get("next_run_time", "")) or "-",
        }

    wet_bulb_scheduler = _scheduler_metric(scheduler_status.get("wet_bulb_collection_scheduler", {}))
    auto_flow_scheduler = _scheduler_metric(scheduler_status.get("scheduler", {}))
    day_metric_scheduler = _scheduler_metric(scheduler_status.get("day_metric_upload_scheduler", {}))
    monthly_event_scheduler = _scheduler_metric(scheduler_status.get("monthly_event_report_scheduler", {}))
    monthly_change_scheduler = _scheduler_metric(scheduler_status.get("monthly_change_report_scheduler", {}))
    scheduler_metrics = [
        {
            "label": _string(item.get("label", "")) or "-",
            "value": _string(item.get("value", "")) or "-",
        }
        for item in _list(scheduler_summary.get("items", []))
        if isinstance(item, dict)
    ]
    if not scheduler_metrics:
        scheduler_metrics = [
            {"label": "已启动调度", "value": f"{int(scheduler_summary.get('running_count', 0) or 0)} 项"},
            {"label": "未启动调度", "value": f"{int(scheduler_summary.get('stopped_count', 0) or 0)} 项"},
            {"label": "待关注项", "value": f"{int(scheduler_summary.get('attention_count', 0) or 0)} 项"},
        ]

    return {
        "scheduler_overview": {
            "eyebrow": "统一扫读",
            "title": "调度总览",
            "description": "集中查看全部调度是否已启动、何时执行，以及哪些调度需要进入对应模块处理。",
            "metrics": scheduler_metrics,
        },
        "auto_flow": {
            "eyebrow": "推荐主路径",
            "title": "自动流程主控面板",
            "description": "适合日常标准流程，先切内网下载，再切外网计算并上传。",
            "metrics": [
                {"label": "当前网络", "value": current_ssid},
                {"label": "调度状态", "value": auto_flow_scheduler["status_text"]},
                {
                    "label": "当前任务",
                    "value": (
                        f"运行中 {running_count} / 等待 {waiting_count}"
                        if running_count > 0 or waiting_count > 0
                        else task_status_text
                    ),
                },
            ],
        },
        "multi_date": {
            "eyebrow": "批量补跑",
            "title": "多日用电明细自动流程",
            "description": "适合补跑连续日期，保持统一下载与上传流程。",
            "metrics": [
                {"label": "当前网络", "value": current_ssid},
                {"label": "当前任务", "value": task_status_text},
                {"label": "等待资源", "value": f"{waiting_count} 项"},
            ],
        },
        "manual_upload": {
            "eyebrow": "仅外网上传",
            "title": "手动补传",
            "description": "不执行内网下载，直接使用手动选择的文件进行补传。",
            "metrics": [
                {"label": "角色", "value": "固定按当前角色执行"},
                {"label": "当前网络", "value": current_ssid},
                {"label": "当前任务", "value": task_status_text},
            ],
        },
        "sheet_import": {
            "eyebrow": "一次性导表",
            "title": "5Sheet 导入",
            "description": "清空目标表后重新导入 5 个工作表，用于手动修复或覆盖。",
            "metrics": [
                {"label": "角色", "value": "固定按当前角色执行"},
                {"label": "当前网络", "value": current_ssid},
                {
                    "label": "状态",
                    "value": "处理中" if running_count > 0 or bridge_active_count > 0 else "待命",
                },
            ],
        },
        "handover_log": {
            "eyebrow": "5楼审核联动",
            "title": "交接班日志工作台",
            "description": "围绕文件生成、楼栋审核与确认后续动作组织界面，突出主路径与文件状态。",
            "metrics": [
                {"label": "目标班次", "value": _string(review_overview.get("duty_text", "")) or "-"},
                {"label": "确认进度", "value": f"{int(review_overview.get('confirmed', 0) or 0)}/{int(review_overview.get('required', 0) or 0)}"},
                {"label": "审核概况", "value": _string(review_overview.get("summary_text", "")) or "-"},
            ],
        },
        "day_metric_upload": {
            "eyebrow": "独立重写",
            "title": "12项独立上传",
            "description": "按日期下载或导入本地文件，单独提取并重写 12 项；不进入交接班审核链路。",
            "metrics": [
                {"label": "调度状态", "value": day_metric_scheduler["status_text"]},
                {"label": "下次执行", "value": day_metric_scheduler["next_run_time"]},
                {"label": "目标状态", "value": _string(day_metric_target.get("status_text", "")) or "未配置"},
            ],
        },
        "wet_bulb_collection": {
            "eyebrow": "独立采集",
            "title": "湿球温度定时采集",
            "description": "复用交接班规则引擎提取湿球温度和冷源模式，并按楼栋写入多维表。",
            "metrics": [
                {"label": "调度状态", "value": wet_bulb_scheduler["status_text"]},
                {"label": "下次执行", "value": wet_bulb_scheduler["next_run_time"]},
                {"label": "目标状态", "value": _string(wet_bulb_target.get("status_text", "")) or "未配置"},
            ],
        },
        "monthly_event_report": {
            "eyebrow": "月度本地生成",
            "title": "体系月度统计表",
            "description": "读取上一个自然月的事件与变更数据，按楼栋生成两类月度统计表并输出到本地目录。",
            "metrics": [
                {"label": "事件调度", "value": monthly_event_scheduler["status_text"]},
                {"label": "变更调度", "value": monthly_change_scheduler["status_text"]},
                {
                    "label": "最近执行",
                    "value": monthly_event_scheduler["next_run_time"]
                    if monthly_event_scheduler["next_run_time"] != "-"
                    else monthly_change_scheduler["next_run_time"],
                },
            ],
        },
        "alarm_event_upload": {
            "eyebrow": "专项上传",
            "title": "告警信息上传",
            "description": "按楼读取当天最新一份告警文件，缺失则回退昨天最新，并将 60 天内记录写入目标多维表。",
            "metrics": [
                {"label": "最近上传", "value": _string(alarm_family.get("uploadLastRunAt", "")) or "-"},
                {"label": "上传记录", "value": f"{int(alarm_family.get('uploadRecordCount', 0) or 0)} 条"},
                {"label": "参与文件", "value": f"{int(alarm_family.get('uploadFileCount', 0) or 0)} 份"},
            ],
        },
    }
