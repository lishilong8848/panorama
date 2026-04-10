from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.modules.shared_bridge.api import routes


class _FakeBridgeService:
    def __init__(self) -> None:
        self.list_tasks_error: Exception | None = None
        self.get_task_error: Exception | None = None
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
        self.manual_alarm_refresh_result = {
            "accepted": True,
            "running": True,
            "scope": "alarm_manual",
            "bucket_key": "2026-03-30 10:11:12",
        }
        self.delete_manual_alarm_result = {
            "accepted": True,
            "deleted_count": 3,
            "deleted_buildings": ["A楼", "B楼", "C楼"],
        }
        self.shared_root_self_check_result = {
            "status": "warning",
            "status_text": "发现记录与文件不一致",
            "tone": "warning",
            "message": "存在 ready 记录，但当前角色无法访问其中部分文件。",
            "role_mode": "external",
            "role_label": "外网端",
            "root_dir": "Z:/share",
            "db_path": "Z:/share/bridge.db",
            "checked_at": "2026-04-09 12:00:00",
            "enabled_buildings": ["A楼", "B楼"],
            "directories": [],
            "families": [
                {
                    "key": "handover_log_family",
                    "title": "交接班日志源文件",
                    "tone": "danger",
                    "status_text": "记录存在但文件不可见",
                    "summary_text": "数据库有 ready 记录，但当前角色看不到对应文件。",
                    "path": "Z:/share/交接班日志源文件",
                    "ready_entry_count": 2,
                    "accessible_ready_count": 0,
                    "missing_ready_count": 2,
                    "latest_downloaded_at": "2026-04-09 11:58:00",
                    "sample_ready_path": "",
                    "sample_missing_path": "Z:/share/交接班日志源文件/202604/...",
                    "query_error": "",
                }
            ],
            "summary": {
                "ready_entry_count": 2,
                "accessible_ready_count": 0,
                "missing_ready_count": 2,
                "initialized_count": 1,
            },
            "error": "",
        }
        self.cached_tasks = [
            {
                "task_id": "bridge-cached-1",
                "feature": "monthly_report_pipeline",
                "mode": "auto_once",
                "status": "queued_for_internal",
                "error": "",
            }
        ]
        self.cached_detail = {
            "task_id": "bridge-cached-1",
            "feature": "monthly_report_pipeline",
            "mode": "auto_once",
            "status": "queued_for_internal",
            "error": "",
            "stages": [],
            "events": [],
            "artifacts": [],
        }

    def list_tasks(self, limit: int = 100):  # noqa: ANN001
        if self.list_tasks_error is not None:
            raise self.list_tasks_error
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
        if self.get_task_error is not None:
            raise self.get_task_error
        return self.detail_payload if task_id == "bridge-1" else None

    def get_cached_tasks(self, *, limit: int | None = None):  # noqa: ANN001
        tasks = list(self.cached_tasks)
        if limit is None:
            return tasks
        return tasks[: max(1, int(limit or 1))]

    def get_cached_task(self, task_id: str):  # noqa: ANN001
        if str(task_id or "").strip() == str(self.cached_detail.get("task_id", "")).strip():
            return dict(self.cached_detail)
        return None

    @staticmethod
    def _is_recoverable_store_error(exc: Exception) -> bool:
        return "unable to open database file" in str(exc).lower()

    def cancel_task(self, task_id: str) -> bool:  # noqa: ANN001
        return self.cancel_result and task_id == "bridge-1"

    def retry_task(self, task_id: str) -> bool:  # noqa: ANN001
        return self.retry_result and task_id == "bridge-1"

    def start_current_hour_source_cache_refresh(self):
        return dict(self.current_hour_refresh_result)

    def start_manual_alarm_source_cache_refresh(self):
        return dict(self.manual_alarm_refresh_result)

    def delete_manual_alarm_source_cache_files(self):
        return dict(self.delete_manual_alarm_result)

    def upload_alarm_event_source_cache_full_to_bitable(self, *, emit_log=None):
        if callable(emit_log):
            emit_log("[共享缓存] 外网告警文件上传开始: mode=full, scope=all, kept_days=60")
        return {
            "accepted": True,
            "mode": "full",
            "scope": "all",
            "uploaded_record_count": 120,
            "consumed_count": 0,
            "failed_entries": [],
        }

    def upload_alarm_event_source_cache_single_building_to_bitable(self, *, building: str, emit_log=None):
        if callable(emit_log):
            emit_log(f"[共享缓存] 外网告警文件上传开始: mode=single_building, scope={building}, kept_days=60")
        return {
            "accepted": True,
            "mode": "single_building",
            "scope": building,
            "uploaded_record_count": 24,
            "consumed_count": 0,
            "failed_entries": [],
        }

    def diagnose_shared_root(self, *, initialize: bool = True, ready_limit_per_family: int = 400):
        return {
            **self.shared_root_self_check_result,
            "initialize": initialize,
            "ready_limit_per_family": ready_limit_per_family,
        }


class _FakeJob:
    def __init__(self, payload):
        self._payload = dict(payload)

    def to_dict(self):
        return dict(self._payload)


class _FakeJobService:
    def __init__(self) -> None:
        self.active_by_dedupe: dict[str, dict] = {}
        self.started_jobs: list[dict] = []

    def find_active_job_by_dedupe_key(self, dedupe_key: str):
        return self.active_by_dedupe.get(str(dedupe_key or "").strip())

    def start_job(
        self,
        *,
        name,
        run_func,
        resource_keys=None,
        priority="manual",
        feature="",
        dedupe_key="",
        submitted_by="manual",
    ):
        payload = {
            "job_id": f"job-{len(self.started_jobs) + 1}",
            "name": name,
            "status": "queued",
            "feature": feature,
            "priority": priority,
            "resource_keys": list(resource_keys or []),
            "dedupe_key": dedupe_key,
            "submitted_by": submitted_by,
            "created_at": "2026-04-03 02:00:00",
        }
        self.started_jobs.append(
            {
                "payload": dict(payload),
                "run_func": run_func,
            }
        )
        if dedupe_key:
            self.active_by_dedupe[str(dedupe_key).strip()] = dict(payload)
        return _FakeJob(payload)


def _fake_request(service: _FakeBridgeService | None = None, *, role_mode: str = "external"):
    job_service = _FakeJobService()
    container = SimpleNamespace(
        deployment_snapshot=lambda: {"role_mode": role_mode, "node_id": "node-ext-01", "node_label": "外网机"},
        shared_bridge_snapshot=lambda: {"enabled": True, "root_dir": "D:/QJPT_Shared", "db_status": "ok"},
        shared_bridge_service=service,
        job_service=job_service,
        add_system_log=lambda *_args, **_kwargs: None,
    )
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_bridge_health_returns_snapshots() -> None:
    request = _fake_request(_FakeBridgeService())

    response = routes.bridge_health(request)

    assert response["ok"] is True
    assert response["deployment"]["role_mode"] == "external"
    assert response["shared_bridge"]["root_dir"] == "D:/QJPT_Shared"


def test_bridge_shared_root_self_check_returns_diagnostics() -> None:
    request = _fake_request(_FakeBridgeService())

    response = routes.bridge_shared_root_self_check(request)

    assert response["ok"] is True
    assert response["status_text"] == "发现记录与文件不一致"
    assert response["root_dir"] == "Z:/share"
    assert response["summary"]["missing_ready_count"] == 2
    assert response["families"][0]["title"] == "交接班日志源文件"


def test_bridge_tasks_returns_list() -> None:
    request = _fake_request(_FakeBridgeService())

    response = routes.bridge_tasks(request, limit=25)

    assert response["ok"] is True
    assert response["tasks"][0]["task_id"] == "bridge-1"
    assert response["tasks"][0]["limit"] == 25
    assert response["tasks"][0]["feature_label"] == "月报主流程"
    assert response["tasks"][0]["current_stage_name"] == "准备月报共享文件"


def test_bridge_tasks_falls_back_to_cached_tasks_when_store_read_is_recoverable() -> None:
    service = _FakeBridgeService()
    service.list_tasks_error = RuntimeError("unable to open database file")
    request = _fake_request(service)

    response = routes.bridge_tasks(request, limit=25)

    assert response["ok"] is True
    assert response["tasks"][0]["task_id"] == "bridge-cached-1"


def test_bridge_task_detail_contains_stage_labels_and_display_error() -> None:
    request = _fake_request(_FakeBridgeService())

    response = routes.bridge_task_detail("bridge-1", request)

    assert response["ok"] is True
    assert response["task"]["feature_label"] == "月报主流程"
    assert response["task"]["current_stage_name"] == "准备月报共享文件"
    assert response["task"]["stages"][0]["stage_name"] == "准备月报共享文件"
    assert response["task"]["artifacts"][0]["artifact_kind_label"] == "续传状态"
    assert response["task"]["events"][0]["event_text"] == "内网下载进行中"


def test_bridge_task_detail_falls_back_to_cached_detail_when_store_read_is_recoverable() -> None:
    service = _FakeBridgeService()
    service.get_task_error = RuntimeError("unable to open database file")
    request = _fake_request(service)

    response = routes.bridge_task_detail("bridge-cached-1", request)

    assert response["ok"] is True
    assert response["task"]["task_id"] == "bridge-cached-1"


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


def test_bridge_source_cache_refresh_alarm_manual_accepts_internal_role() -> None:
    service = _FakeBridgeService()
    request = _fake_request(service, role_mode="internal")
    request.app.state.container.add_system_log = lambda *_args, **_kwargs: None

    response = routes.bridge_source_cache_refresh_alarm_manual(request)

    assert response["ok"] is True
    assert response["accepted"] is True
    assert response["running"] is True
    assert response["message"] == "已开始手动拉取告警信息文件"


def test_bridge_source_cache_delete_manual_alarm_files_accepts_internal_role() -> None:
    service = _FakeBridgeService()
    request = _fake_request(service, role_mode="internal")
    request.app.state.container.add_system_log = lambda *_args, **_kwargs: None

    response = routes.bridge_source_cache_delete_manual_alarm_files(request)

    assert response["ok"] is True
    assert response["accepted"] is True
    assert response["deleted_count"] == 3


def test_bridge_source_cache_alarm_upload_full_accepts_external_role() -> None:
    service = _FakeBridgeService()
    request = _fake_request(service, role_mode="external")

    response = routes.bridge_source_cache_alarm_upload_full(request)

    assert response["ok"] is True
    assert response["accepted"] is True
    assert response["running"] is True
    assert response["job"]["feature"] == "alarm_event_upload"
    assert response["job"]["dedupe_key"] == "alarm_event_upload:full"
    assert "已提交" in response["message"]


def test_bridge_source_cache_alarm_upload_building_accepts_external_role() -> None:
    service = _FakeBridgeService()
    request = _fake_request(service, role_mode="external")

    response = routes.bridge_source_cache_alarm_upload_building(request, building="C楼")

    assert response["ok"] is True
    assert response["accepted"] is True
    assert response["running"] is True
    assert response["scope"] == "C楼"
    assert response["job"]["dedupe_key"] == "alarm_event_upload:building:C楼"
    assert "使用共享文件上传60天" in response["message"]


def test_bridge_source_cache_alarm_upload_full_returns_running_state_when_already_running() -> None:
    service = _FakeBridgeService()
    request = _fake_request(service, role_mode="external")
    request.app.state.container.job_service.active_by_dedupe["alarm_event_upload:full"] = {
        "job_id": "job-existing-full",
        "name": "告警信息全量上传（60天）",
        "status": "running",
        "feature": "alarm_event_upload",
        "dedupe_key": "alarm_event_upload:full",
        "created_at": "2026-04-02 10:00:00",
    }

    response = routes.bridge_source_cache_alarm_upload_full(request)

    assert response["ok"] is True
    assert response["accepted"] is False
    assert response["running"] is True
    assert response["reason"] == "already_running"
    assert response["job"]["job_id"] == "job-existing-full"
    assert "聚焦到现有任务" in response["message"]


def test_bridge_source_cache_alarm_upload_building_returns_running_state_when_already_running() -> None:
    service = _FakeBridgeService()
    request = _fake_request(service, role_mode="external")
    request.app.state.container.job_service.active_by_dedupe["alarm_event_upload:building:C楼"] = {
        "job_id": "job-existing-building",
        "name": "告警信息单楼刷新上传（60天）- C楼",
        "status": "running",
        "feature": "alarm_event_upload",
        "dedupe_key": "alarm_event_upload:building:C楼",
        "created_at": "2026-04-02 10:00:00",
    }

    response = routes.bridge_source_cache_alarm_upload_building(request, building="C楼")

    assert response["ok"] is True
    assert response["accepted"] is False
    assert response["running"] is True
    assert response["reason"] == "already_running"
    assert response["job"]["job_id"] == "job-existing-building"


def test_bridge_source_cache_alarm_upload_full_rejects_internal_role() -> None:
    request = _fake_request(_FakeBridgeService(), role_mode="internal")

    with pytest.raises(HTTPException) as excinfo:
        routes.bridge_source_cache_alarm_upload_full(request)

    assert excinfo.value.status_code == 409
    assert "仅外网端允许上传告警信息文件到多维表" in str(excinfo.value.detail)


def test_bridge_source_cache_debug_alarm_page_actions_is_gone() -> None:
    request = _fake_request(_FakeBridgeService(), role_mode="internal")

    with pytest.raises(HTTPException) as excinfo:
        routes.bridge_source_cache_debug_alarm_page_actions(request, building="A楼")

    assert excinfo.value.status_code == 410
    assert "仅支持 API 拉取" in str(excinfo.value.detail)


def test_bridge_source_cache_refresh_today_is_gone() -> None:
    request = _fake_request(_FakeBridgeService(), role_mode="internal")

    with pytest.raises(HTTPException) as excinfo:
        routes.bridge_source_cache_refresh_today(request)

    assert excinfo.value.status_code == 410
    assert "当前小时" in str(excinfo.value.detail)
