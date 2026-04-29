from __future__ import annotations

from app.modules.report_pipeline.service.scheduler_state_presenter import (
    present_scheduler_overview_items,
    present_scheduler_overview_summary,
    present_scheduler_state,
)


def test_present_scheduler_state_prefers_memory_state_for_actions() -> None:
    payload = present_scheduler_state(
        {
            "running": False,
            "status": "未初始化",
            "remembered_enabled": True,
            "executor_bound": True,
            "next_run_time": "2026-04-17 21:00:00",
            "last_decision": "waiting",
            "last_trigger_result": "",
        },
        role_mode="external",
    )

    assert payload["tone"] == "info"
    assert payload["status_text"] == "已记住开启"
    assert payload["actions"]["start"]["allowed"] is False
    assert payload["actions"]["start"]["label"] == "已记住开启"
    assert payload["actions"]["stop"]["allowed"] is True
    assert "下次执行" in payload["detail_text"]
    assert payload["next_run_text"] == "2026-04-17 21:00:00"
    assert payload["decision_text"] == "waiting"
    assert payload["trigger_text"] == "暂无记录"


def test_present_scheduler_state_blocks_external_only_controls_in_internal_role() -> None:
    payload = present_scheduler_state(
        {
            "running": False,
            "status": "未启动",
            "remembered_enabled": False,
            "executor_bound": True,
        },
        role_mode="internal",
        external_only=True,
    )

    assert payload["actions"]["start"]["allowed"] is False
    assert payload["actions"]["stop"]["allowed"] is False
    assert payload["actions"]["start"]["disabled_reason"] == "当前为内网端，该调度仅允许在外网端操作。"


def test_present_scheduler_overview_items_builds_backend_display_payload() -> None:
    config = {
        "common": {"scheduler": {"interval_minutes": 60}},
        "features": {
            "handover_log": {
                "scheduler": {
                    "morning_time": "07:00:00",
                    "afternoon_time": "16:00:00",
                },
                "monthly_event_report": {"scheduler": {"day_of_month": 1, "run_time": "01:00:00"}},
                "monthly_change_report": {"scheduler": {"day_of_month": 2, "run_time": "02:00:00"}},
            },
            "day_metric_upload": {"scheduler": {"interval_minutes": 30}},
            "wet_bulb_collection": {"scheduler": {"interval_minutes": 15}},
            "alarm_export": {"scheduler": {"run_time": "08:10:00"}},
        },
    }
    summary = {
        "scheduler": {
            "running": True,
            "status": "运行中",
            "remembered_enabled": True,
            "next_run_time": "2026-04-17 21:00:00",
            "last_trigger_at": "2026-04-17 20:00:00",
            "last_trigger_result": "success",
        },
        "handover_scheduler": {
            "running": False,
            "remembered_enabled": True,
            "morning": {"next_run_time": "2026-04-18 07:00:00", "last_trigger_result": "success"},
            "afternoon": {"next_run_time": "2026-04-17 16:00:00", "last_trigger_result": "failed"},
        },
        "day_metric_upload_scheduler": {"running": False, "remembered_enabled": False},
        "wet_bulb_collection_scheduler": {"running": False, "remembered_enabled": True},
        "alarm_event_upload_scheduler": {"running": False, "remembered_enabled": False},
        "monthly_event_report_scheduler": {"running": False, "remembered_enabled": False},
        "monthly_change_report_scheduler": {"running": False, "remembered_enabled": False},
    }

    items = present_scheduler_overview_items(config, summary, role_mode="external")
    auto_flow = next(item for item in items if item["key"] == "auto_flow")
    handover = next(item for item in items if item["key"] == "handover_log")

    assert auto_flow["status_text"] == "运行中"
    assert auto_flow["parts"][0]["run_time_text"] == "每 60 分钟"
    assert handover["parts"][0]["run_time_text"] == "07:00:00"
    assert handover["parts"][1]["result_text"] == "失败"


def test_present_scheduler_state_maps_decision_and_trigger_texts() -> None:
    payload = present_scheduler_state(
        {
            "running": False,
            "status": "未启动",
            "remembered_enabled": False,
            "executor_bound": True,
            "next_run_time": "2026-04-18 08:00:00",
            "last_trigger_at": "2026-04-18 07:00:00",
            "last_decision": "skip:before_next_run",
            "last_trigger_result": "skip_busy",
        },
        role_mode="external",
    )

    assert payload["decision_text"] == "未到下次执行时间"
    assert payload["trigger_text"] == "任务占用已跳过"
    assert payload["last_trigger_text"] == "2026-04-18 07:00:00"


def test_present_scheduler_overview_summary_prefers_attention_item() -> None:
    summary = present_scheduler_overview_summary(
        [
            {
                "title": "每日用电明细自动流程",
                "status_text": "运行中",
                "summary_text": "正常",
                "tone": "success",
                "parts": [{"label": "循环调度", "next_run_text": "2026-04-17 20:00:00"}],
            },
            {
                "title": "告警信息上传",
                "status_text": "异常",
                "summary_text": "最近失败",
                "tone": "warning",
                "parts": [{"label": "每日调度", "next_run_text": "2026-04-17 19:00:00"}],
            },
        ]
    )

    assert summary["attention_count"] == 1
    assert summary["status_text"] == "有待关注项"
    assert summary["reason_code"] == "attention"
    assert summary["next_scheduler_label"] == "告警信息上传"
    assert summary["attention_text"] == "告警信息上传：最近失败"
    assert summary["detail_text"] == "告警信息上传：最近失败"
    assert summary["items"][0]["label"] == "已启动调度"
    assert summary["actions"][0]["id"] == "open_scheduler_overview"
