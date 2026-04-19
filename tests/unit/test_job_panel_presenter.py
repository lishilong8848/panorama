from __future__ import annotations

from types import SimpleNamespace

from app.modules.report_pipeline.service.job_panel_presenter import (
    build_bridge_tasks_summary,
    build_job_panel_summary,
    present_job_item,
    present_bridge_task,
)


class _FakeJobService:
    def list_jobs(self, *, limit: int = 60, statuses=None):  # noqa: ANN001
        return [
            {
                "job_id": "job-running",
                "name": "交接班日志生成",
                "feature": "handover_from_download",
                "status": "running",
                "started_at": "2026-04-17 20:10:00",
                "cancel_requested": False,
                "summary": "",
                "error": "",
                "wait_reason": "",
            },
            {
                "job_id": "job-waiting",
                "name": "告警上传",
                "feature": "alarm_export",
                "status": "waiting_resource",
                "created_at": "2026-04-17 20:11:00",
                "cancel_requested": True,
                "summary": "",
                "error": "",
                "wait_reason": "waiting:browser_controlled",
            },
        ][:limit]

    def active_job_ids(self, *, include_waiting: bool = True):  # noqa: ANN001
        return ["job-running", "job-waiting"] if include_waiting else ["job-running"]

    def job_counts(self):
        return {"running": 1, "waiting_resource": 1}


class _FakeSharedBridgeService:
    def list_tasks(self, *, limit: int = 60):  # noqa: ANN001
        return [
            {
                "task_id": "bridge-waiting",
                "feature": "handover_from_download",
                "feature_label": "交接班使用共享文件生成",
                "status": "ready_for_external",
                "current_stage_name": "准备交接班共享文件",
                "current_stage_role": "internal",
                "current_stage_status": "waiting_next_side",
                "updated_at": "2026-04-17 20:12:00",
                "summary": "",
                "display_error": "",
                "current_stage_error": "",
                "error": "",
                "events": [
                    {
                        "event_type": "waiting_source_sync",
                        "event_text": "等待内网补采同步",
                    }
                ],
            }
        ][:limit]


def test_job_panel_summary_returns_backend_ready_display_rows() -> None:
    container = SimpleNamespace(
        job_service=_FakeJobService(),
        shared_bridge_service=_FakeSharedBridgeService(),
    )

    payload = build_job_panel_summary(container, limit=20)
    display = payload["display"]
    assert display["overview"]["handover_generation_busy"] is True
    assert "交接班日志生成任务" in display["overview"]["handover_generation_status_text"]

    running = display["running_jobs"][0]
    assert running["display_title"] == "交接班日志生成"
    assert running["display_meta"] == "状态：执行中 | 时间：2026-04-17 20:10:00"
    assert running["actions"]["cancel"]["allowed"] is True
    assert running["actions"]["cancel"]["label"] == "取消任务"
    assert running["actions"]["cancel"]["target_kind"] == "job"
    assert running["actions"]["cancel"]["target_id"] == "job-running"

    waiting_bridge = next(
        item for item in display["waiting_resource_items"] if item["item_kind"] == "bridge"
    )
    assert waiting_bridge["display_title"] == "交接班使用共享文件生成"
    assert waiting_bridge["display_meta"] == "状态：等待内网补采同步 | 时间：2026-04-17 20:12:00"
    assert waiting_bridge["display_detail"] == "说明：内网端 / 准备交接班共享文件 / 等待内网补采同步"
    assert waiting_bridge["actions"]["cancel"]["allowed"] is True
    assert waiting_bridge["actions"]["cancel"]["target_kind"] == "bridge"
    assert waiting_bridge["actions"]["cancel"]["target_id"] == "bridge-waiting"

    waiting_job = next(
        item for item in display["waiting_resource_items"] if item["item_kind"] == "job"
    )
    assert waiting_job["actions"]["cancel"]["allowed"] is False
    assert waiting_job["actions"]["cancel"]["pending"] is True
    assert waiting_job["actions"]["cancel"]["label"] == "取消中..."
    assert waiting_job["actions"]["cancel"]["target_kind"] == "job"
    assert waiting_job["actions"]["cancel"]["target_id"] == "job-waiting"


def test_present_bridge_task_returns_backend_ready_display_fields() -> None:
    payload = present_bridge_task(
        {
            "task_id": "bridge-running",
            "feature": "monthly_report_pipeline",
            "status": "external_running",
            "updated_at": "2026-04-17 21:00:00",
            "summary": "",
            "error": "",
            "current_stage_name": "使用共享文件上传月报",
            "current_stage_role": "external",
            "current_stage_status": "external_running",
            "artifacts": [
                {"status": "ready"},
                {"status": "pending"},
            ],
        }
    )

    assert payload["item_kind"] == "bridge"
    assert payload["display_title"] == "月报主流程"
    assert payload["display_meta"] == "状态：外网处理中 | 时间：2026-04-17 21:00:00"
    assert payload["display_detail"] == "说明：外网端 / 使用共享文件上传月报 / 外网处理中"
    assert payload["stage_summary_text"] == "外网端 / 使用共享文件上传月报 / 外网处理中"
    assert payload["artifact_summary_text"] == "产物 1/2"
    assert payload["error_text"] == ""
    assert payload["actions"]["cancel"]["allowed"] is True
    assert payload["actions"]["cancel"]["target_kind"] == "bridge"
    assert payload["actions"]["cancel"]["target_id"] == "bridge-running"
    assert payload["actions"]["retry"]["target_kind"] == "bridge"
    assert payload["actions"]["retry"]["target_id"] == "bridge-running"


def test_present_job_item_shows_dependency_repair_state() -> None:
    payload = present_job_item(
        {
            "job_id": "job-dependency",
            "name": "日报截图重截",
            "feature": "daily_report_recapture",
            "status": "waiting_resource",
            "created_at": "2026-04-19 21:51:33",
            "wait_reason": "waiting:dependency_sync",
            "summary": "",
            "error": "",
            "cancel_requested": False,
            "stages": [
                {
                    "stage_id": "main",
                    "worker_status": "dependency_repairing",
                }
            ],
        }
    )

    assert payload["status_text"] == "修复依赖中"
    assert payload["tone"] == "warning"
    assert payload["display_meta"] == "状态：修复依赖中 | 时间：2026-04-19 21:51:33"
    assert payload["display_detail"] == "说明：正在自动补齐运行依赖"
    assert payload["actions"]["cancel"]["allowed"] is True


def test_build_bridge_tasks_summary_returns_backend_display_groups() -> None:
    payload = build_bridge_tasks_summary(
        [
            {
                "task_id": "bridge-active",
                "feature": "handover_from_download",
                "feature_label": "交接班使用共享文件生成",
                "status": "external_running",
                "updated_at": "2026-04-18 09:00:00",
                "current_stage_name": "使用共享文件生成交接班",
                "current_stage_role": "external",
                "current_stage_status": "external_running",
            },
            {
                "task_id": "bridge-waiting",
                "feature": "monthly_report_pipeline",
                "feature_label": "月报主流程",
                "status": "ready_for_external",
                "updated_at": "2026-04-18 09:02:00",
                "events": [{"event_type": "waiting_source_sync", "event_text": "等待内网补采同步"}],
            },
            {
                "task_id": "bridge-finished",
                "feature": "day_metric_from_download",
                "feature_label": "12项使用共享文件上传",
                "status": "success",
                "updated_at": "2026-04-18 09:05:00",
            },
        ],
        count=3,
    )

    assert payload["count"] == 3
    assert payload["display"]["active_count"] == 2
    assert payload["display"]["waiting_count"] == 1
    assert payload["display"]["finished_count"] == 1
    assert payload["display"]["overview"]["status_text"] == "当前有共享桥接任务"
    assert payload["display"]["active_tasks"][0]["display_title"] == "交接班使用共享文件生成"
    assert payload["display"]["waiting_resource_items"][0]["display_title"] == "月报主流程"
    assert payload["display"]["recent_finished_tasks"][0]["display_title"] == "12项使用共享文件上传"
