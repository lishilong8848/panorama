from __future__ import annotations

import json
import sqlite3
from datetime import datetime

from app.modules.shared_bridge.service.shared_bridge_mailbox_store import SharedBridgeMailboxStore
from app.modules.shared_bridge.service.shared_source_cache_index_store import SharedSourceCacheIndexStore
from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore


def test_source_cache_entries_dual_write_json_index_and_read_prefer_index(tmp_path) -> None:
    shared_root = tmp_path / "share"
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()

    source_file = shared_root / "交接班日志源文件" / "202604" / "20260410--09" / "20260410--09--交接班日志源文件--A楼.xlsx"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_bytes(b"ok")

    store.upsert_source_cache_entry(
        source_family="handover_log_family",
        building="A楼",
        bucket_kind="latest",
        bucket_key="2026-04-10 09",
        duty_date="2026-04-10",
        duty_shift="day",
        downloaded_at="2026-04-10 09:30:00",
        relative_path=source_file.relative_to(shared_root).as_posix(),
        status="ready",
        file_hash="hash-a",
        size_bytes=2,
        metadata={"naming_version": 2},
    )

    index_path = shared_root / "source_cache_index" / "handover_log_family" / "A楼" / "2026-04-10_09--day.json"
    assert index_path.exists()
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    assert payload["status"] == "ready"
    assert payload["relative_path"] == source_file.relative_to(shared_root).as_posix()

    with store.connect() as conn:
        conn.execute("DELETE FROM source_cache_entries")

    rows = store.list_source_cache_entries(
        source_family="handover_log_family",
        building="A楼",
        bucket_kind="latest",
        status="ready",
    )
    assert len(rows) == 1
    assert rows[0]["relative_path"] == source_file.relative_to(shared_root).as_posix()


def test_runtime_service_falls_back_to_mailbox_when_shared_db_is_busy(tmp_path, monkeypatch) -> None:
    shared_root = tmp_path / "share"
    runtime_config = {
        "deployment": {"role_mode": "external"},
        "shared_bridge": {"enabled": True, "root_dir": str(shared_root)},
        "internal_source_cache": {"enabled": True},
    }
    service = SharedBridgeRuntimeService(runtime_config=runtime_config, app_version="test")

    created = service.create_monthly_auto_once_task(requested_by="manual")
    task_id = str(created.get("task_id", "") or "").strip()
    assert task_id
    assert (shared_root / "bridge_mailbox" / task_id / "request.json").exists()
    assert (shared_root / "bridge_mailbox" / task_id / "internal.json").exists()

    def _raise_busy(*_args, **_kwargs):  # noqa: ANN001
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(service._store, "ensure_ready", lambda: None)
    monkeypatch.setattr(service._store, "list_tasks", _raise_busy)
    monkeypatch.setattr(service._store, "get_task", _raise_busy)

    tasks = service.list_tasks(limit=20)
    detail = service.get_task(task_id)

    assert tasks
    assert tasks[0]["task_id"] == task_id
    assert detail is not None
    assert detail["task_id"] == task_id
    assert detail["feature"] == "monthly_report_pipeline"


def test_runtime_service_prefers_mailbox_for_task_reads_even_when_store_exists(tmp_path, monkeypatch) -> None:
    shared_root = tmp_path / "share"
    runtime_config = {
        "deployment": {"role_mode": "external"},
        "shared_bridge": {"enabled": True, "root_dir": str(shared_root)},
        "internal_source_cache": {"enabled": True},
    }
    service = SharedBridgeRuntimeService(runtime_config=runtime_config, app_version="test")

    created = service.create_monthly_auto_once_task(requested_by="manual")
    task_id = str(created.get("task_id", "") or "").strip()
    assert task_id

    def _unexpected(*_args, **_kwargs):  # noqa: ANN001
        raise AssertionError("shared bridge sqlite read should not be used for task read path")

    monkeypatch.setattr(service._store, "ensure_ready", _unexpected)
    monkeypatch.setattr(service._store, "list_tasks", _unexpected)
    monkeypatch.setattr(service._store, "get_task", _unexpected)

    tasks = service.list_tasks(limit=20)
    detail = service.get_task(task_id)

    assert tasks
    assert tasks[0]["task_id"] == task_id
    assert detail is not None
    assert detail["task_id"] == task_id


def test_health_snapshot_uses_cached_internal_alert_status_without_store_projection_read(tmp_path, monkeypatch) -> None:
    shared_root = tmp_path / "share"
    runtime_config = {
        "deployment": {"role_mode": "external"},
        "shared_bridge": {"enabled": True, "root_dir": str(shared_root)},
        "internal_source_cache": {"enabled": True},
    }
    service = SharedBridgeRuntimeService(runtime_config=runtime_config, app_version="test")
    service._cached_internal_alert_status = {
        "buildings": [
            {
                "building": "A楼",
                "status": "problem",
                "status_text": "异常",
                "summary": "A楼 登录失败，等待内网恢复",
                "detail": "页面无响应",
                "last_problem_at": "2026-04-10 10:00:00",
                "last_recovered_at": "",
                "active_count": 1,
            }
        ],
        "active_count": 1,
        "last_notified_at": "2026-04-10 10:05:00",
    }
    monkeypatch.setattr(service._store, "list_external_alert_projections", lambda: (_ for _ in ()).throw(AssertionError("should not read projection")))
    monkeypatch.setattr(
        service._source_cache_service,
        "get_health_snapshot",
        lambda mode="external_full": service._empty_internal_source_cache_snapshot(),
    )

    snapshot = service.get_health_snapshot()

    assert snapshot["internal_alert_status"]["active_count"] == 1
    assert snapshot["internal_alert_status"]["buildings"][0]["building"] == "A楼"


def test_mailbox_store_alias_paths_share_same_task_snapshot(tmp_path) -> None:
    canonical_root = tmp_path / "share"
    alias_root = tmp_path / "nested" / ".." / "share"
    task_payload = {
        "task_id": "task-alias-1",
        "feature": "monthly_report_pipeline",
        "mode": "auto_once",
        "status": "ready_for_external",
        "dedupe_key": "monthly|2026-04-10",
        "request": {"resume_job_id": "job-1"},
        "result": {"status": "ready_for_external"},
        "created_by_role": "external",
        "created_by_node_id": "ext-01",
        "requested_by": "manual",
        "created_at": "2026-04-10 23:00:00",
        "updated_at": "2026-04-10 23:01:00",
        "stages": [],
        "artifacts": [],
        "events": [],
    }

    writer = SharedBridgeMailboxStore(canonical_root)
    reader = SharedBridgeMailboxStore(str(alias_root))

    writer.write_request(task_payload)
    writer.write_side_snapshot(task=task_payload, side="external")

    loaded = reader.load_task("task-alias-1")

    assert loaded is not None
    assert loaded["task_id"] == "task-alias-1"
    assert loaded["status"] == "ready_for_external"
    assert loaded["request"]["resume_job_id"] == "job-1"


def test_source_cache_index_store_alias_paths_share_same_entries(tmp_path) -> None:
    canonical_root = tmp_path / "share"
    alias_root = tmp_path / "nested" / ".." / "share"
    writer = SharedSourceCacheIndexStore(canonical_root)
    reader = SharedSourceCacheIndexStore(str(alias_root))

    writer.upsert_entry(
        {
            "entry_id": "entry-alias-1",
            "source_family": "handover_log_family",
            "building": "A楼",
            "bucket_kind": "latest",
            "bucket_key": "2026-04-10 10",
            "duty_date": "2026-04-10",
            "duty_shift": "day",
            "downloaded_at": "2026-04-10 10:30:00",
            "relative_path": "交接班日志源文件/202604/20260410--10/20260410--10--交接班日志源文件--A楼.xlsx",
            "status": "ready",
            "file_hash": "hash-1",
            "size_bytes": 123,
            "metadata": {"naming_version": 2},
        }
    )

    rows = reader.list_entries(
        source_family="handover_log_family",
        building="A楼",
        bucket_kind="latest",
        status="ready",
    )

    assert len(rows) == 1
    assert rows[0]["entry_id"] == "entry-alias-1"
    assert rows[0]["bucket_key"] == "2026-04-10 10"


def test_runtime_service_alias_root_reads_mailbox_task_snapshot(tmp_path) -> None:
    canonical_root = tmp_path / "share"
    alias_root = tmp_path / "nested" / ".." / "share"
    runtime_config_writer = {
        "deployment": {"role_mode": "external"},
        "shared_bridge": {"enabled": True, "root_dir": str(canonical_root)},
        "internal_source_cache": {"enabled": True},
    }
    runtime_config_reader = {
        "deployment": {"role_mode": "external"},
        "shared_bridge": {"enabled": True, "root_dir": str(alias_root)},
        "internal_source_cache": {"enabled": True},
    }

    writer = SharedBridgeRuntimeService(runtime_config=runtime_config_writer, app_version="test")
    reader = SharedBridgeRuntimeService(runtime_config=runtime_config_reader, app_version="test")

    created = writer.create_monthly_auto_once_task(requested_by="manual")
    task_id = str(created["task_id"])

    tasks = reader.list_tasks(limit=20)
    detail = reader.get_task(task_id)

    assert tasks
    assert any(str(item.get("task_id", "") or "").strip() == task_id for item in tasks)
    assert detail is not None
    assert detail["task_id"] == task_id
    assert detail["feature"] == "monthly_report_pipeline"


def test_runtime_service_alias_root_reads_latest_source_cache_selection(tmp_path) -> None:
    canonical_root = tmp_path / "share"
    alias_root = tmp_path / "nested" / ".." / "share"
    store = SharedBridgeStore(canonical_root)
    store.ensure_ready()
    current_bucket = datetime.now().strftime("%Y-%m-%d %H")

    source_file = canonical_root / "交接班日志源文件" / "202604" / f"{current_bucket.replace(' ', '--')}" / "latest--交接班日志源文件--A楼.xlsx"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_bytes(b"ok")

    store.upsert_source_cache_entry(
        source_family="handover_log_family",
        building="A楼",
        bucket_kind="latest",
        bucket_key=current_bucket,
        duty_date="2026-04-10",
        duty_shift="day",
        downloaded_at=f"{current_bucket}:30:00",
        relative_path=source_file.relative_to(canonical_root).as_posix(),
        status="ready",
        file_hash="hash-a",
        size_bytes=2,
        metadata={"naming_version": 2},
    )

    runtime_config = {
        "deployment": {"role_mode": "external"},
        "shared_bridge": {"enabled": True, "root_dir": str(alias_root)},
        "internal_source_cache": {"enabled": True},
    }
    service = SharedBridgeRuntimeService(runtime_config=runtime_config, app_version="test")

    entries = service.get_latest_source_cache_entries(source_family="handover_log_family", buildings=["A楼"])
    selection = service.get_latest_source_cache_selection(source_family="handover_log_family", buildings=["A楼"])

    assert len(entries) == 1
    assert entries[0]["building"] == "A楼"
    assert str(entries[0]["file_path"] or "").endswith("A楼.xlsx")
    assert selection["can_proceed"] is True
    assert selection["best_bucket_key"] == current_bucket
    assert len(selection["selected_entries"]) == 1
    assert selection["selected_entries"][0]["building"] == "A楼"
