from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.modules.shared_bridge.api import routes


class _FakeBridgeService:
    def __init__(self) -> None:
        self.detail_payload = {
            "task_id": "bridge-1",
            "feature": "monthly_report_pipeline",
            "mode": "auto_once",
            "status": "internal_running",
            "error": "",
            "stages": [
                {
                    "stage_id": "internal_download",
                    "handler": "monthly_auto_once_internal",
                    "status": "running",
                    "role_target": "internal",
                    "error": "",
                },
                {
                    "stage_id": "external_resume",
                    "handler": "monthly_resume_from_shared_external",
                    "status": "pending",
                    "role_target": "external",
                    "error": "",
                },
            ],
            "events": [
                {
                    "event_type": "log",
                    "level": "info",
                    "side": "internal",
                    "payload": {"message": "内网下载进行中"},
                }
            ],
            "artifacts": [
                {
                    "artifact_id": "artifact-1",
                    "artifact_kind": "resume_state",
                    "status": "ready",
                    "relative_path": "artifacts/monthly_report/bridge-1/resume/manifest.json",
                }
            ],
        }
        self.cancel_result = False
        self.retry_result = False
        self.current_hour_refresh_result = {
            "accepted": True,
            "running": True,
            "scope": "current_hour",
            "bucket_key": "2026-03-30 02",
        }

    def list_tasks(self, limit: int = 100):  # noqa: ANN001
        return [
            {
                "task_id": "bridge-1",
                "feature": "monthly_report_pipeline",
                "mode": "auto_once",
                "status": "queued_for_internal",
                "error": "",
                "limit": limit,
            }
        ]

    def get_task(self, task_id: str):  # noqa: ANN001
        return self.detail_payload if task_id == "bridge-1" else None

    def cancel_task(self, task_id: str) -> bool:  # noqa: ANN001
        return self.cancel_result and task_id == "bridge-1"

    def retry_task(self, task_id: str) -> bool:  # noqa: ANN001
        return self.retry_result and task_id == "bridge-1"

    def start_current_hour_source_cache_refresh(self):
        return dict(self.current_hour_refresh_result)


def _fake_request(service: _FakeBridgeService | None = None, *, role_mode: str = "external"):
    container = SimpleNamespace(
        deployment_snapshot=lambda: {"role_mode": role_mode, "node_id": "node-ext-01", "node_label": "外网机"},
        shared_bridge_snapshot=lambda: {"enabled": True, "root_dir": "D:/QJPT_Shared", "db_status": "ok"},
        shared_bridge_service=service,
    )
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_bridge_health_returns_snapshots() -> None:
    request = _fake_request(_FakeBridgeService())

    response = routes.bridge_health(request)

    assert response["ok"] is True
    assert response["deployment"]["role_mode"] == "external"
    assert response["shared_bridge"]["root_dir"] == "D:/QJPT_Shared"


def test_bridge_tasks_returns_list() -> None:
    request = _fake_request(_FakeBridgeService())

    response = routes.bridge_tasks(request, limit=25)

    assert response["ok"] is True
    assert response["tasks"][0]["task_id"] == "bridge-1"
    assert response["tasks"][0]["limit"] == 25
    assert response["tasks"][0]["feature_label"] == "月报主流程"
    assert response["tasks"][0]["current_stage_name"] == "准备月报共享文件"


def test_bridge_task_detail_contains_stage_labels_and_display_error() -> None:
    request = _fake_request(_FakeBridgeService())

    response = routes.bridge_task_detail("bridge-1", request)

    assert response["ok"] is True
    assert response["task"]["feature_label"] == "月报主流程"
    assert response["task"]["current_stage_name"] == "准备月报共享文件"
    assert response["task"]["stages"][0]["stage_name"] == "准备月报共享文件"
    assert response["task"]["artifacts"][0]["artifact_kind_label"] == "续传状态"
    assert response["task"]["events"][0]["event_text"] == "内网下载进行中"


def test_bridge_task_detail_maps_internal_error_codes_to_chinese() -> None:
    service = _FakeBridgeService()
    service.detail_payload["error"] = "internal_download_failed"
    service.detail_payload["stages"][0]["error"] = "internal_download_failed"
    request = _fake_request(service)

    response = routes.bridge_task_detail("bridge-1", request)

    assert response["task"]["display_error"] == "共享文件准备失败"
    assert response["task"]["current_stage_error"] == "共享文件准备失败"
    assert response["task"]["stages"][0]["error_text"] == "共享文件准备失败"


def test_bridge_task_detail_maps_database_error_text_to_chinese() -> None:
    service = _FakeBridgeService()
    service.detail_payload["error"] = "database is locked"
    request = _fake_request(service)

    response = routes.bridge_task_detail("bridge-1", request)

    assert response["task"]["display_error"] == "共享桥接数据库正忙，请稍后重试"


def test_bridge_task_detail_labels_cache_fill_feature_and_stage() -> None:
    service = _FakeBridgeService()
    service.detail_payload = {
        "task_id": "bridge-1",
        "feature": "handover_cache_fill",
        "mode": "day_metric",
        "status": "ready_for_external",
        "error": "",
        "stages": [
            {
                "stage_id": "internal_fill",
                "handler": "handover_cache_fill_internal",
                "status": "success",
                "role_target": "internal",
                "error": "",
            },
            {
                "stage_id": "external_continue",
                "handler": "handover_cache_fill_external",
                "status": "pending",
                "role_target": "external",
                "error": "",
            },
        ],
        "events": [],
        "artifacts": [],
    }
    request = _fake_request(service)

    response = routes.bridge_task_detail("bridge-1", request)

    assert response["task"]["feature_label"] == "交接班历史共享文件补采"
    assert response["task"]["stages"][0]["stage_name"] == "补采12项历史共享文件"
    assert response["task"]["stages"][1]["stage_name"] == "使用共享文件上传12项"


def test_bridge_task_detail_404_message_is_clean() -> None:
    request = _fake_request(_FakeBridgeService())

    with pytest.raises(HTTPException) as excinfo:
        routes.bridge_task_detail("missing", request)

    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == "共享任务不存在"


def test_bridge_task_cancel_404_message_is_clean() -> None:
    request = _fake_request(_FakeBridgeService())

    with pytest.raises(HTTPException) as excinfo:
        routes.bridge_task_cancel("missing", request)

    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == "共享任务不存在"


def test_bridge_task_retry_404_message_is_clean() -> None:
    request = _fake_request(_FakeBridgeService())

    with pytest.raises(HTTPException) as excinfo:
        routes.bridge_task_retry("missing", request)

    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == "共享任务不存在"


def test_bridge_task_cancel_is_read_only_on_internal_role() -> None:
    request = _fake_request(_FakeBridgeService(), role_mode="internal")

    with pytest.raises(HTTPException) as excinfo:
        routes.bridge_task_cancel("bridge-1", request)

    assert excinfo.value.status_code == 409
    assert "只提供共享任务只读查看" in str(excinfo.value.detail)


def test_bridge_task_retry_is_read_only_on_internal_role() -> None:
    request = _fake_request(_FakeBridgeService(), role_mode="internal")

    with pytest.raises(HTTPException) as excinfo:
        routes.bridge_task_retry("bridge-1", request)

    assert excinfo.value.status_code == 409
    assert "只提供共享任务只读查看" in str(excinfo.value.detail)


def test_bridge_source_cache_refresh_current_hour_accepts_internal_role() -> None:
    service = _FakeBridgeService()
    request = _fake_request(service, role_mode="internal")
    request.app.state.container.add_system_log = lambda *_args, **_kwargs: None

    response = routes.bridge_source_cache_refresh_current_hour(request)

    assert response["ok"] is True
    assert response["accepted"] is True
    assert response["running"] is True
    assert response["message"] == "已开始下载当前小时全部文件"


def test_bridge_source_cache_refresh_current_hour_rejects_external_role() -> None:
    request = _fake_request(_FakeBridgeService(), role_mode="external")

    with pytest.raises(HTTPException) as excinfo:
        routes.bridge_source_cache_refresh_current_hour(request)

    assert excinfo.value.status_code == 409
    assert excinfo.value.detail == "当前仅内网端允许手动触发当前小时下载"


def test_bridge_source_cache_refresh_today_is_gone() -> None:
    request = _fake_request(_FakeBridgeService(), role_mode="internal")

    with pytest.raises(HTTPException) as excinfo:
        routes.bridge_source_cache_refresh_today(request)

    assert excinfo.value.status_code == 410
    assert "当前小时" in str(excinfo.value.detail)
