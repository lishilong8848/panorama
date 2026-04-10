from __future__ import annotations

from pathlib import Path

from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService


def _runtime_config(tmp_path: Path, role_mode: str) -> dict:
    return {
        "deployment": {
            "role_mode": role_mode,
            "node_id": f"{role_mode}-node",
            "node_label": role_mode,
        },
        "shared_bridge": {
            "enabled": True,
            "root_dir": str(tmp_path),
            "poll_interval_sec": 1,
            "heartbeat_interval_sec": 1,
            "claim_lease_sec": 30,
            "stale_task_timeout_sec": 1800,
            "artifact_retention_days": 7,
            "sqlite_busy_timeout_ms": 5000,
        },
    }

def test_alarm_export_legacy_bridge_task_is_failed_without_db_access(tmp_path: Path) -> None:
    service = SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "internal"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    service._store.ensure_ready()
    now_text = "2026-03-29 12:00:00"
    task_id = "legacy-alarm-export-task"
    with service._store.connect() as conn:
        conn.execute(
            """
            INSERT INTO bridge_tasks(
                task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                task_id,
                "alarm_export",
                "bridge",
                "external",
                "external-node",
                "manual",
                "queued_for_internal",
                "alarm_export|legacy",
                "{}",
                "{}",
                "",
                now_text,
                now_text,
            ),
        )
        conn.execute(
            """
            INSERT INTO bridge_stages(
                task_id, stage_id, role_target, handler, status, input_json, result_json,
                claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
            ) VALUES(?, ?, ?, ?, ?, ?, '{}', '', '', '', '', '', '', 0)
            """,
            (
                task_id,
                "internal_query",
                "internal",
                "alarm_export_internal_query",
                "pending",
                "{}",
            ),
        )
    service._process_one_task_if_needed()

    updated = service.get_task(task_id)
    assert updated is not None
    assert updated["status"] == "failed"
    assert "已退役" in str(updated.get("error", ""))


def test_health_snapshot_degrades_to_cached_result_when_store_temporarily_unavailable(tmp_path: Path) -> None:
    service = SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    service._store.ensure_ready()

    initial = service.get_health_snapshot()
    assert initial["db_status"] == "disabled"

    def _raise_permission_error():
        raise PermissionError("[WinError 5] 拒绝访问")

    service._store.list_external_alert_projections = _raise_permission_error  # type: ignore[method-assign]

    degraded = service.get_health_snapshot()

    assert degraded["enabled"] is True
    assert degraded["db_status"] == "unavailable"
    assert degraded["internal_alert_status"] == initial["internal_alert_status"]
    assert "暂时不可用" in str(degraded["last_error"] or "")
