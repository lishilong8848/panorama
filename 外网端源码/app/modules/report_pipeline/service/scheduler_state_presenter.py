from __future__ import annotations

from typing import Any, Dict, Iterable, List


_SCHEDULER_DECISION_TEXT_MAP = {
    "skip:not_started": "未启动",
    "skip:disabled": "已禁用",
    "skip:before_schedule_time": "未到执行时间",
    "skip:before_next_run": "未到下次执行时间",
    "skip:already_success_today": "今日已成功执行",
    "skip:missed_and_no_catchup": "已错过时间且未启用补跑",
    "skip:already_attempted_no_retry": "今日已尝试且不重试",
    "skip:retry_already_done": "今日重试已执行",
    "skip:busy": "执行时任务占用",
    "skip:skip_busy": "执行时任务占用",
    "skip:stopped": "已停止",
    "run:due": "满足触发条件",
}

_SCHEDULER_TRIGGER_TEXT_MAP = {
    "success": "成功",
    "failed": "失败",
    "skip_busy": "任务占用已跳过",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _int(value: Any) -> int:
    try:
        return int(str(value or "").strip())
    except Exception:
        return 0


def _cfg_get(config: Any, *candidate_paths: Iterable[str]) -> Any:
    payload = _dict(config)
    for path in candidate_paths:
        current: Any = payload
        valid = True
        for key in path:
            if not isinstance(current, dict) or key not in current:
                valid = False
                break
            current = current.get(key)
        if valid:
            return current
    return None


def _map_scheduler_decision_text(value: Any) -> str:
    text = _text(value)
    return _SCHEDULER_DECISION_TEXT_MAP.get(text, text)


def _map_scheduler_trigger_text(value: Any) -> str:
    text = _text(value)
    return _SCHEDULER_TRIGGER_TEXT_MAP.get(text, text)


def _format_scheduler_date_text(value: Any, fallback: str = "未安排") -> str:
    text = _text(value)
    return text or fallback


def _interval_run_text(value: Any) -> str:
    minutes = _int(value)
    return f"每 {minutes} 分钟" if minutes > 0 else "未设置"


def _daily_minute_run_text(snapshot: Any, config: Any, *, default_minute: int = 30) -> str:
    snapshot_payload = _dict(snapshot)
    config_payload = _dict(config)
    minute = _int(snapshot_payload.get("minute_offset"))
    if minute <= 0 and str(snapshot_payload.get("minute_offset", "")).strip() not in {"0", "00"}:
        minute = _int(config_payload.get("minute_offset"))
    if minute <= 0 and str(config_payload.get("minute_offset", "")).strip() not in {"0", "00"}:
        minute = default_minute
    minute = max(0, minute) % 60
    return f"每天 00:{minute:02d} 左右"


def _monthly_run_text(day_of_month: Any, run_time: Any) -> str:
    day = _int(day_of_month)
    time_text = _text(run_time)
    if day > 0 and time_text:
        return f"每月 {day} 号 {time_text}"
    if day > 0:
        return f"每月 {day} 号"
    if time_text:
        return time_text
    return "未设置"


def _overview_part(
    *,
    label: str,
    run_time_text: str,
    next_run_time: Any,
    last_trigger_at: Any,
    result_text: str,
) -> Dict[str, Any]:
    return {
        "label": _text(label),
        "run_time_text": _text(run_time_text) or "未设置",
        "next_run_text": _format_scheduler_date_text(next_run_time),
        "last_trigger_text": _format_scheduler_date_text(last_trigger_at, "暂无记录"),
        "result_text": _text(result_text) or "暂无记录",
    }


def _summary_choice(*values: Any, fallback: str) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text
    return fallback


def present_scheduler_state(
    snapshot: Any,
    *,
    role_mode: str = "",
    external_only: bool = True,
    start_label: str = "启动调度",
    stop_label: str = "停止调度",
) -> Dict[str, Any]:
    payload = snapshot if isinstance(snapshot, dict) else {}
    normalized_role = _text(role_mode).lower()
    remembered_enabled = bool(payload.get("remembered_enabled", False))
    running = bool(payload.get("running", False))
    raw_status = _text(payload.get("status", ""))
    next_run_time = _text(payload.get("next_run_time", ""))
    last_decision = _text(payload.get("last_decision", ""))
    last_trigger_result = _text(payload.get("last_trigger_result", ""))
    last_trigger_at = _text(payload.get("last_trigger_at", ""))
    executor_bound = bool(payload.get("executor_bound", False))
    decision_text = _map_scheduler_decision_text(last_decision)
    trigger_text = _map_scheduler_trigger_text(last_trigger_result)
    next_run_text = _format_scheduler_date_text(next_run_time)
    last_trigger_text = _format_scheduler_date_text(last_trigger_at, "暂无记录")

    role_blocked = external_only and normalized_role == "internal"
    role_blocked_reason = "当前为内网端，该调度仅允许在外网端操作。"

    if running:
        tone = "success"
        status_text = raw_status or "运行中"
        summary_text = "当前调度已在运行。"
    elif remembered_enabled:
        tone = "info"
        status_text = "已记住开启"
        if executor_bound:
            summary_text = "当前记忆为开启，等待调度线程下一轮触发。"
        else:
            summary_text = "当前记忆为开启，但执行器尚未绑定。"
    else:
        tone = "neutral"
        status_text = raw_status or "未启动"
        summary_text = "当前记忆为关闭，不会自动触发。"

    if not executor_bound and remembered_enabled and not running:
        tone = "warning"

    start_allowed = not role_blocked and not remembered_enabled
    stop_allowed = not role_blocked and remembered_enabled

    if role_blocked:
        start_disabled_reason = role_blocked_reason
        stop_disabled_reason = role_blocked_reason
    else:
        start_disabled_reason = "" if start_allowed else "当前已记住开启"
        stop_disabled_reason = "" if stop_allowed else "当前已记住关闭"

    detail_parts = []
    if next_run_time:
        detail_parts.append(f"下次执行：{next_run_text}")
    if decision_text:
        detail_parts.append(f"最近决策：{decision_text}")
    if trigger_text:
        detail_parts.append(f"最近结果：{trigger_text}")

    return {
        "tone": tone,
        "status_text": status_text,
        "summary_text": summary_text,
        "detail_text": " / ".join(detail_parts),
        "next_run_text": next_run_text,
        "last_trigger_text": last_trigger_text,
        "decision_text": decision_text or "暂无记录",
        "trigger_text": trigger_text or "暂无记录",
        "actions": {
            "start": {
                "allowed": start_allowed,
                "label": "已记住开启" if remembered_enabled else start_label,
                "disabled_reason": start_disabled_reason,
                "pending": False,
            },
            "stop": {
                "allowed": stop_allowed,
                "label": stop_label,
                "disabled_reason": stop_disabled_reason,
                "pending": False,
            },
        },
    }


def present_scheduler_overview_items(
    config: Any,
    scheduler_status_summary: Any,
    *,
    role_mode: str = "",
) -> List[Dict[str, Any]]:
    config_payload = _dict(config)
    summary_payload = _dict(scheduler_status_summary)

    common_scheduler_cfg = _dict(
        _cfg_get(config_payload, ("scheduler",), ("common", "scheduler"))
    )
    handover_scheduler_cfg = _dict(
        _cfg_get(config_payload, ("handover_log", "scheduler"), ("features", "handover_log", "scheduler"))
    )
    wet_bulb_cfg = _dict(
        _cfg_get(config_payload, ("wet_bulb_collection", "scheduler"), ("features", "wet_bulb_collection", "scheduler"))
    )
    day_metric_cfg = _dict(
        _cfg_get(config_payload, ("day_metric_upload", "scheduler"), ("features", "day_metric_upload", "scheduler"))
    )
    branch_power_cfg = _dict(
        _cfg_get(config_payload, ("branch_power_upload", "scheduler"), ("features", "branch_power_upload", "scheduler"))
    )
    alarm_cfg = _dict(
        _cfg_get(config_payload, ("alarm_export", "scheduler"), ("features", "alarm_export", "scheduler"))
    )
    monthly_event_cfg = _dict(
        _cfg_get(
            config_payload,
            ("handover_log", "monthly_event_report", "scheduler"),
            ("features", "handover_log", "monthly_event_report", "scheduler"),
        )
    )
    monthly_change_cfg = _dict(
        _cfg_get(
            config_payload,
            ("handover_log", "monthly_change_report", "scheduler"),
            ("features", "handover_log", "monthly_change_report", "scheduler"),
        )
    )

    scheduler_snapshot = _dict(summary_payload.get("scheduler"))
    handover_snapshot = _dict(summary_payload.get("handover_scheduler"))
    wet_bulb_snapshot = _dict(summary_payload.get("wet_bulb_collection_scheduler"))
    day_metric_snapshot = _dict(summary_payload.get("day_metric_upload_scheduler"))
    branch_power_snapshot = _dict(summary_payload.get("branch_power_upload_scheduler"))
    alarm_snapshot = _dict(summary_payload.get("alarm_event_upload_scheduler"))
    monthly_event_snapshot = _dict(summary_payload.get("monthly_event_report_scheduler"))
    monthly_change_snapshot = _dict(summary_payload.get("monthly_change_report_scheduler"))

    auto_flow_display = present_scheduler_state(scheduler_snapshot, role_mode=role_mode)
    handover_display = present_scheduler_state(handover_snapshot, role_mode=role_mode)
    wet_bulb_display = present_scheduler_state(wet_bulb_snapshot, role_mode=role_mode)
    day_metric_display = present_scheduler_state(day_metric_snapshot, role_mode=role_mode)
    branch_power_display = present_scheduler_state(branch_power_snapshot, role_mode=role_mode)
    alarm_display = present_scheduler_state(alarm_snapshot, role_mode=role_mode)
    monthly_event_display = present_scheduler_state(monthly_event_snapshot, role_mode=role_mode)
    monthly_change_display = present_scheduler_state(monthly_change_snapshot, role_mode=role_mode)

    handover_morning = _dict(handover_snapshot.get("morning"))
    handover_afternoon = _dict(handover_snapshot.get("afternoon"))
    handover_morning_summary = _summary_choice(
        _map_scheduler_decision_text(handover_morning.get("last_decision")),
        _map_scheduler_trigger_text(handover_morning.get("last_trigger_result")),
        fallback="",
    )
    handover_afternoon_summary = _summary_choice(
        _map_scheduler_decision_text(handover_afternoon.get("last_decision")),
        _map_scheduler_trigger_text(handover_afternoon.get("last_trigger_result")),
        fallback="",
    )

    return [
        {
            "key": "auto_flow",
            "title": "每日用电明细自动流程",
            "module_id": "auto_flow",
            "focus_key": "",
            "tone": auto_flow_display.get("tone", "neutral"),
            "status_text": auto_flow_display.get("status_text", "未启动"),
            "summary_text": _summary_choice(
                _map_scheduler_decision_text(scheduler_snapshot.get("last_decision")),
                _map_scheduler_trigger_text(scheduler_snapshot.get("last_trigger_result")),
                auto_flow_display.get("summary_text", ""),
                fallback="标准月报主流程调度",
            ),
            "parts": [
                _overview_part(
                    label="每日调度",
                    run_time_text=_text(common_scheduler_cfg.get("run_time")) or "未设置",
                    next_run_time=scheduler_snapshot.get("next_run_time"),
                    last_trigger_at=scheduler_snapshot.get("last_trigger_at"),
                    result_text=_map_scheduler_trigger_text(scheduler_snapshot.get("last_trigger_result")),
                )
            ],
        },
        {
            "key": "handover_log",
            "title": "交接班日志",
            "module_id": "handover_log",
            "focus_key": "",
            "tone": handover_display.get("tone", "neutral"),
            "status_text": handover_display.get("status_text", "未启动"),
            "summary_text": _summary_choice(
                handover_morning_summary,
                handover_afternoon_summary,
                handover_display.get("summary_text", ""),
                fallback="上午补跑夜班，下午执行白班",
            ),
            "parts": [
                _overview_part(
                    label="上午调度",
                    run_time_text=_text(handover_scheduler_cfg.get("morning_time")) or "未设置",
                    next_run_time=handover_morning.get("next_run_time"),
                    last_trigger_at=handover_morning.get("last_trigger_at"),
                    result_text=_map_scheduler_trigger_text(handover_morning.get("last_trigger_result")),
                ),
                _overview_part(
                    label="下午调度",
                    run_time_text=_text(handover_scheduler_cfg.get("afternoon_time")) or "未设置",
                    next_run_time=handover_afternoon.get("next_run_time"),
                    last_trigger_at=handover_afternoon.get("last_trigger_at"),
                    result_text=_map_scheduler_trigger_text(handover_afternoon.get("last_trigger_result")),
                ),
            ],
        },
        {
            "key": "day_metric_upload",
            "title": "12项独立上传",
            "module_id": "day_metric_upload",
            "focus_key": "",
            "tone": day_metric_display.get("tone", "neutral"),
            "status_text": day_metric_display.get("status_text", "未启动"),
            "summary_text": _summary_choice(
                _map_scheduler_decision_text(day_metric_snapshot.get("last_decision")),
                _map_scheduler_trigger_text(day_metric_snapshot.get("last_trigger_result")),
                day_metric_display.get("summary_text", ""),
                fallback="固定处理当天、全部启用楼栋",
            ),
            "parts": [
                _overview_part(
                    label="循环调度",
                    run_time_text=_interval_run_text(day_metric_cfg.get("interval_minutes")),
                    next_run_time=day_metric_snapshot.get("next_run_time"),
                    last_trigger_at=day_metric_snapshot.get("last_trigger_at"),
                    result_text=_map_scheduler_trigger_text(day_metric_snapshot.get("last_trigger_result")),
                )
            ],
        },
        {
            "key": "branch_power_upload",
            "title": "自动上传支路功率",
            "module_id": "branch_power_upload",
            "focus_key": "",
            "tone": branch_power_display.get("tone", "neutral"),
            "status_text": branch_power_display.get("status_text", "未启动"),
            "summary_text": _summary_choice(
                _map_scheduler_decision_text(branch_power_snapshot.get("last_decision")),
                _map_scheduler_trigger_text(branch_power_snapshot.get("last_trigger_result")),
                branch_power_display.get("summary_text", ""),
                fallback="每天读取前一业务日支路三源整日文件",
            ),
            "parts": [
                _overview_part(
                    label="每日调度",
                    run_time_text=_daily_minute_run_text(branch_power_snapshot, branch_power_cfg),
                    next_run_time=branch_power_snapshot.get("next_run_time"),
                    last_trigger_at=branch_power_snapshot.get("last_trigger_at"),
                    result_text=_map_scheduler_trigger_text(branch_power_snapshot.get("last_trigger_result")),
                )
            ],
        },
        {
            "key": "wet_bulb_collection",
            "title": "湿球温度定时采集",
            "module_id": "wet_bulb_collection",
            "focus_key": "",
            "tone": wet_bulb_display.get("tone", "neutral"),
            "status_text": wet_bulb_display.get("status_text", "未启动"),
            "summary_text": _summary_choice(
                _map_scheduler_decision_text(wet_bulb_snapshot.get("last_decision")),
                _map_scheduler_trigger_text(wet_bulb_snapshot.get("last_trigger_result")),
                wet_bulb_display.get("summary_text", ""),
                fallback="按固定分钟间隔循环执行",
            ),
            "parts": [
                _overview_part(
                    label="循环调度",
                    run_time_text=_interval_run_text(wet_bulb_cfg.get("interval_minutes")),
                    next_run_time=wet_bulb_snapshot.get("next_run_time"),
                    last_trigger_at=wet_bulb_snapshot.get("last_trigger_at"),
                    result_text=_map_scheduler_trigger_text(wet_bulb_snapshot.get("last_trigger_result")),
                )
            ],
        },
        {
            "key": "monthly_event_report",
            "title": "体系月度统计表-事件",
            "module_id": "monthly_event_report",
            "focus_key": "monthly_event",
            "tone": monthly_event_display.get("tone", "neutral"),
            "status_text": monthly_event_display.get("status_text", "未启动"),
            "summary_text": _summary_choice(
                _map_scheduler_decision_text(monthly_event_snapshot.get("last_decision")),
                _map_scheduler_trigger_text(monthly_event_snapshot.get("last_trigger_result")),
                monthly_event_display.get("summary_text", ""),
                fallback="固定读取上一个自然月事件数据",
            ),
            "parts": [
                _overview_part(
                    label="事件月报",
                    run_time_text=_monthly_run_text(
                        monthly_event_cfg.get("day_of_month"),
                        monthly_event_cfg.get("run_time"),
                    ),
                    next_run_time=monthly_event_snapshot.get("next_run_time"),
                    last_trigger_at=monthly_event_snapshot.get("last_trigger_at"),
                    result_text=_map_scheduler_trigger_text(monthly_event_snapshot.get("last_trigger_result")),
                )
            ],
        },
        {
            "key": "monthly_change_report",
            "title": "体系月度统计表-变更",
            "module_id": "monthly_event_report",
            "focus_key": "monthly_change",
            "tone": monthly_change_display.get("tone", "neutral"),
            "status_text": monthly_change_display.get("status_text", "未启动"),
            "summary_text": _summary_choice(
                _map_scheduler_decision_text(monthly_change_snapshot.get("last_decision")),
                _map_scheduler_trigger_text(monthly_change_snapshot.get("last_trigger_result")),
                monthly_change_display.get("summary_text", ""),
                fallback="固定读取上一个自然月变更数据",
            ),
            "parts": [
                _overview_part(
                    label="变更月报",
                    run_time_text=_monthly_run_text(
                        monthly_change_cfg.get("day_of_month"),
                        monthly_change_cfg.get("run_time"),
                    ),
                    next_run_time=monthly_change_snapshot.get("next_run_time"),
                    last_trigger_at=monthly_change_snapshot.get("last_trigger_at"),
                    result_text=_map_scheduler_trigger_text(monthly_change_snapshot.get("last_trigger_result")),
                )
            ],
        },
        {
            "key": "alarm_event_upload",
            "title": "告警信息上传",
            "module_id": "alarm_event_upload",
            "focus_key": "",
            "tone": alarm_display.get("tone", "neutral"),
            "status_text": alarm_display.get("status_text", "未启动"),
            "summary_text": _summary_choice(
                _map_scheduler_decision_text(alarm_snapshot.get("last_decision")),
                _map_scheduler_trigger_text(alarm_snapshot.get("last_trigger_result")),
                alarm_display.get("summary_text", ""),
                fallback="固定执行全部楼栋 60 天上传",
            ),
            "parts": [
                _overview_part(
                    label="每日调度",
                    run_time_text=_text(alarm_cfg.get("run_time")) or "未设置",
                    next_run_time=alarm_snapshot.get("next_run_time"),
                    last_trigger_at=alarm_snapshot.get("last_trigger_at"),
                    result_text=_map_scheduler_trigger_text(alarm_snapshot.get("last_trigger_result")),
                )
            ],
        },
    ]


def present_scheduler_overview_summary(items: Any) -> Dict[str, Any]:
    rows = [item for item in (items if isinstance(items, list) else []) if isinstance(item, dict)]
    active_count = 0
    attention_items: List[Dict[str, Any]] = []
    upcoming_parts: List[Dict[str, Any]] = []
    for item in rows:
        status_text = _text(item.get("status_text"))
        if status_text in {"运行中", "已记住开启"}:
            active_count += 1
        tone = _text(item.get("tone"))
        if tone in {"warning", "danger"}:
            attention_items.append(item)
        for part in item.get("parts", []):
            if not isinstance(part, dict):
                continue
            next_run_text = _text(part.get("next_run_text"))
            if next_run_text and next_run_text != "未安排":
                upcoming_parts.append(
                    {
                        "title": _text(item.get("title")),
                        "label": _text(part.get("label")),
                        "next_run_text": next_run_text,
                    }
                )
    upcoming_parts.sort(key=lambda row: _text(row.get("next_run_text")))
    next_item = upcoming_parts[0] if upcoming_parts else {}
    attention_item = attention_items[0] if attention_items else {}
    stopped_count = max(0, len(rows) - active_count)
    if attention_item:
        status_text = "有待关注项"
        tone = _text(attention_item.get("tone")) or "warning"
        reason_code = "attention"
    elif active_count > 0:
        status_text = "状态正常"
        tone = "success"
        reason_code = "active"
    else:
        status_text = "全部未启动"
        tone = "neutral"
        reason_code = "idle"
    next_scheduler_text = (
        f"{_text(next_item.get('title'))}"
        f"{' · ' + _text(next_item.get('label')) if _text(next_item.get('label')) else ''}"
        f" / {_text(next_item.get('next_run_text'))}"
        if next_item
        else "当前没有已安排的调度"
    )
    attention_text = (
        f"{_text(attention_item.get('title'))}：{_text(attention_item.get('summary_text')) or _text(attention_item.get('status_text'))}"
        if attention_item
        else "当前没有待关注调度"
    )
    summary_text = (
        "请先查看待关注调度，再进入对应模块处理。"
        if attention_item
        else "这里集中查看全部调度状态，需要调整时进入对应模块操作。"
    )
    return {
        "running_count": active_count,
        "stopped_count": stopped_count,
        "attention_count": len(attention_items),
        "status_text": status_text,
        "tone": tone,
        "reason_code": reason_code,
        "next_scheduler_label": _text(next_item.get("title")) or "暂无安排",
        "next_scheduler_text": next_scheduler_text,
        "attention_text": attention_text,
        "summary_text": summary_text,
        "detail_text": attention_text if attention_item else next_scheduler_text,
        "items": [
            {
                "label": "已启动调度",
                "value": f"{active_count} 项",
                "tone": "success" if active_count > 0 else "neutral",
            },
            {
                "label": "未启动调度",
                "value": f"{stopped_count} 项",
                "tone": "warning" if stopped_count > 0 else "neutral",
            },
            {
                "label": "待关注项",
                "value": f"{len(attention_items)} 项",
                "tone": "warning" if attention_items else "success",
            },
            {
                "label": "最近即将执行",
                "value": _text(next_item.get("title")) or "暂无安排",
                "tone": "info" if next_item else "neutral",
            },
        ],
        "actions": [
            {
                "id": "open_scheduler_overview",
                "label": "查看调度总览",
                "desc": "进入调度总览查看全部调度状态",
                "allowed": True,
                "pending": False,
                "disabled_reason": "",
            }
        ],
    }
