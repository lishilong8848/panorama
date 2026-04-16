from __future__ import annotations

import sqlite3
from collections import Counter

from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore


def test_retry_partial_failed_task_requeues_external_stage_only(tmp_path) -> None:
    store = SharedBridgeStore(tmp_path)
    store.ensure_ready()
    task = store.create_day_metric_from_download_task(
        selected_dates=["2026-03-28"],
        building_scope="single",
        building="A楼",
        created_by_role="external",
        created_by_node_id="ext-01",
        requested_by="manual",
    )

    claimed_internal = store.claim_next_task(role_target="internal", node_id="int-01", lease_sec=30)
    assert claimed_internal is not None
    int_token = next(
        (stage.get("claim_token", "") for stage in claimed_internal.get("stages", []) if stage.get("stage_id") == "internal_download"),
        "",
    )
    store.complete_stage(
        task_id=task["task_id"],
        stage_id="internal_download",
        claim_token=int_token,
        side="internal",
        stage_result={"status": "ok", "selected_dates": ["2026-03-28"], "selected_buildings": ["A楼"]},
        next_task_status="ready_for_external",
        task_result={
            "status": "ready_for_external",
            "internal": {"status": "ok", "selected_dates": ["2026-03-28"], "selected_buildings": ["A楼"]},
        },
    )

    claimed_external = store.claim_next_task(role_target="external", node_id="ext-01", lease_sec=30)
    assert claimed_external is not None
    ext_token = next(
        (stage.get("claim_token", "") for stage in claimed_external.get("stages", []) if stage.get("stage_id") == "external_upload"),
        "",
    )
    store.complete_stage(
        task_id=task["task_id"],
        stage_id="external_upload",
        claim_token=ext_token,
        side="external",
        stage_result={"status": "partial_failed", "error": "upload failed"},
        next_task_status="partial_failed",
        task_error="upload failed",
        task_result={
            "status": "partial_failed",
            "internal": {"status": "ok", "selected_dates": ["2026-03-28"], "selected_buildings": ["A楼"]},
            "external": {"status": "partial_failed", "error": "upload failed"},
        },
    )

    assert store.retry_task(task["task_id"]) is True

    updated = store.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "ready_for_external"
    assert updated["result"]["status"] == "ready_for_external"
    assert updated["result"]["internal"]["selected_buildings"] == ["A楼"]

    stages = {stage["stage_id"]: stage for stage in updated["stages"]}
    assert stages["internal_download"]["status"] == "success"
    assert stages["external_upload"]["status"] == "pending"


def test_sweep_expired_running_tasks_marks_task_stale(tmp_path) -> None:
    store = SharedBridgeStore(tmp_path)
    store.ensure_ready()
    task = store.create_internal_browser_alert_task(
        building="A楼",
        failure_kind="login_failed",
        alert_state="problem",
        status_key="login_failed",
        summary="A楼 登录失败",
        latest_detail="A楼 登录失败",
        first_seen_at="2026-04-01 10:00:00",
        last_seen_at="2026-04-01 10:00:00",
        resolved_at="",
        occurrence_count=1,
        still_unresolved=True,
        created_by_role="internal",
        created_by_node_id="int-01",
    )

    claimed = store.claim_next_task(role_target="external", node_id="ext-01", lease_sec=30)
    assert claimed is not None
    with store.connect() as conn:
        conn.execute(
            """
            UPDATE bridge_stages
            SET started_at='2026-04-01 10:00:00', lease_expires_at='2026-04-01 10:01:00'
            WHERE task_id=? AND stage_id='external_notify'
            """,
            (task["task_id"],),
        )
        conn.execute(
            "UPDATE bridge_tasks SET updated_at='2026-04-01 10:01:00' WHERE task_id=?",
            (task["task_id"],),
        )

    swept = store.sweep_expired_running_tasks(stale_task_timeout_sec=60)

    updated = store.get_task(task["task_id"])
    assert swept == 1
    assert updated is not None
    assert updated["status"] == "stale"
    assert any(event["event_type"] == "lease_expired" for event in updated["events"])


def test_cleanup_terminal_history_and_stale_nodes(tmp_path) -> None:
    store = SharedBridgeStore(tmp_path)
    store.ensure_ready()
    task = store.create_internal_browser_alert_task(
        building="A楼",
        failure_kind="login_failed",
        alert_state="problem",
        status_key="login_failed",
        summary="A楼 登录失败",
        latest_detail="A楼 登录失败",
        first_seen_at="2026-04-01 10:00:00",
        last_seen_at="2026-04-01 10:00:00",
        resolved_at="",
        occurrence_count=1,
        still_unresolved=True,
        created_by_role="internal",
        created_by_node_id="int-01",
    )
    with store.connect() as conn:
        conn.execute(
            "UPDATE bridge_tasks SET status='failed', updated_at='2026-03-01 10:00:00' WHERE task_id=?",
            (task["task_id"],),
        )
        conn.execute(
            "INSERT OR REPLACE INTO bridge_nodes(node_id, role_mode, node_label, host_name, version, last_seen_at, status) VALUES(?, ?, ?, ?, ?, ?, ?)",
            ("old-node", "external", "外网端", "host", "test", "2026-03-01 10:00:00", "online"),
        )

    cleanup = store.cleanup_terminal_history(retention_days=14)
    deleted_nodes = store.cleanup_stale_nodes(retention_days=2)

    assert cleanup["deleted_tasks"] == 1
    assert store.get_task(task["task_id"]) is None
    assert deleted_nodes == 1


def test_read_only_connect_uses_plain_path_for_unc_share(monkeypatch) -> None:
    store = SharedBridgeStore(r"\\172.16.1.2\share\bridge-root")
    captured: dict[str, object] = {}

    class _FakeConnection:
        def __init__(self) -> None:
            self.in_transaction = False
            self.row_factory = None
            self.executed: list[str] = []

        def execute(self, sql, *_args, **_kwargs):  # noqa: ANN001
            self.executed.append(str(sql))
            return self

        def close(self) -> None:
            return None

        def rollback(self) -> None:
            return None

    fake_conn = _FakeConnection()

    def _fake_connect(database, **kwargs):  # noqa: ANN001
        captured["database"] = database
        captured["kwargs"] = kwargs
        return fake_conn

    monkeypatch.setattr(sqlite3, "connect", _fake_connect)

    with store.connect(read_only=True) as conn:
        assert conn is fake_conn

    assert captured["database"] == str(store.db_path)
    assert "uri" not in captured["kwargs"]
    assert "PRAGMA query_only=ON" in fake_conn.executed


def test_find_active_task_by_dedupe_key_prefers_mailbox_snapshot(tmp_path) -> None:
    store = SharedBridgeStore(tmp_path)
    store.ensure_ready()
    task = store.create_monthly_auto_once_task(
        created_by_role="external",
        created_by_node_id="ext-01",
        requested_by="manual",
    )
    task_id = str(task["task_id"])
    dedupe_key = str(task["dedupe_key"])

    with store.connect() as conn:
        conn.execute("DELETE FROM bridge_tasks WHERE task_id=?", (task_id,))
        conn.execute("DELETE FROM bridge_stages WHERE task_id=?", (task_id,))
        conn.execute("DELETE FROM bridge_events WHERE task_id=?", (task_id,))

    found = store.find_active_task_by_dedupe_key(dedupe_key)

    assert found is not None
    assert found["task_id"] == task_id


def test_create_task_does_not_requery_shared_db_after_write(tmp_path, monkeypatch) -> None:
    store = SharedBridgeStore(tmp_path)
    store.ensure_ready()

    monkeypatch.setattr(store, "get_task", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not requery task")))

    task = store.create_monthly_auto_once_task(
        created_by_role="external",
        created_by_node_id="ext-01",
        requested_by="manual",
    )

    assert task["feature"] == "monthly_report_pipeline"
    assert task["task_id"]


def test_claim_next_task_does_not_requery_shared_db_after_claim(tmp_path, monkeypatch) -> None:
    store = SharedBridgeStore(tmp_path)
    store.ensure_ready()
    created = store.create_monthly_auto_once_task(
        created_by_role="external",
        created_by_node_id="ext-01",
        requested_by="manual",
    )

    monkeypatch.setattr(store, "get_task", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not requery task")))

    claimed = store.claim_next_task(role_target="internal", node_id="int-01", lease_sec=30)

    assert claimed is not None
    assert claimed["task_id"] == created["task_id"]
    assert claimed["status"] == "internal_running"


def test_retry_task_can_skip_retried_event_for_automatic_requeue(tmp_path) -> None:
    store = SharedBridgeStore(tmp_path)
    store.ensure_ready()
    task = store.create_day_metric_from_download_task(
        selected_dates=["2026-03-28"],
        building_scope="single",
        building="A楼",
        created_by_role="external",
        created_by_node_id="ext-01",
        requested_by="manual",
    )

    claimed_internal = store.claim_next_task(role_target="internal", node_id="int-01", lease_sec=30)
    assert claimed_internal is not None
    int_token = next(
        (stage.get("claim_token", "") for stage in claimed_internal.get("stages", []) if stage.get("stage_id") == "internal_download"),
        "",
    )
    store.complete_stage(
        task_id=task["task_id"],
        stage_id="internal_download",
        claim_token=int_token,
        side="internal",
        stage_result={"status": "ok", "selected_dates": ["2026-03-28"], "selected_buildings": ["A楼"]},
        next_task_status="ready_for_external",
        task_result={
            "status": "ready_for_external",
            "internal": {"status": "ok", "selected_dates": ["2026-03-28"], "selected_buildings": ["A楼"]},
        },
    )

    claimed_external = store.claim_next_task(role_target="external", node_id="ext-01", lease_sec=30)
    assert claimed_external is not None

    assert store.retry_task(task["task_id"], record_event=False) is True

    updated = store.get_task(task["task_id"])
    assert updated is not None
    event_types = Counter(str(item.get("event_type", "") or "").strip() for item in updated.get("events", []))
    assert event_types["claimed"] == 2
    assert event_types["completed"] == 1
    assert event_types["retried"] == 0


def test_heartbeat_claim_only_extends_stage_lease_by_default(tmp_path) -> None:
    store = SharedBridgeStore(tmp_path)
    store.ensure_ready()
    task = store.create_monthly_auto_once_task(
        created_by_role="external",
        created_by_node_id="ext-01",
        requested_by="manual",
    )

    claimed = store.claim_next_task(role_target="internal", node_id="int-01", lease_sec=30)
    assert claimed is not None
    task_before = store.get_task(task["task_id"])
    assert task_before is not None
    stage_before = next(item for item in task_before["stages"] if item["stage_id"] == "internal_download")
    token = str(stage_before["claim_token"] or "")
    updated_before = str(task_before["updated_at"] or "")
    lease_before = str(stage_before["lease_expires_at"] or "")

    store.heartbeat_claim(
        task_id=task["task_id"],
        stage_id="internal_download",
        claim_token=token,
        lease_sec=45,
    )

    task_after = store.get_task(task["task_id"])
    assert task_after is not None
    stage_after = next(item for item in task_after["stages"] if item["stage_id"] == "internal_download")
    assert str(stage_after["lease_expires_at"] or "") != lease_before
    assert str(task_after["updated_at"] or "") == updated_before


def test_complete_stage_can_skip_completed_event_for_handoff(tmp_path) -> None:
    store = SharedBridgeStore(tmp_path)
    store.ensure_ready()
    task = store.create_monthly_auto_once_task(
        created_by_role="external",
        created_by_node_id="ext-01",
        requested_by="manual",
    )

    claimed = store.claim_next_task(role_target="internal", node_id="int-01", lease_sec=30)
    assert claimed is not None
    token = next(
        (stage.get("claim_token", "") for stage in claimed.get("stages", []) if stage.get("stage_id") == "internal_download"),
        "",
    )

    store.complete_stage(
        task_id=task["task_id"],
        stage_id="internal_download",
        claim_token=token,
        side="internal",
        stage_result={"status": "ok"},
        next_task_status="ready_for_external",
        task_result={"status": "ready_for_external", "internal": {"status": "ok"}},
        record_event=False,
        sync_mailbox=False,
    )
    store.append_event(
        task_id=task["task_id"],
        stage_id="internal_download",
        side="internal",
        level="info",
        event_type="await_external",
        payload={"message": "内网下载完成，等待外网继续处理"},
    )

    updated = store.get_task(task["task_id"])
    assert updated is not None
    event_types = Counter(str(item.get("event_type", "") or "").strip() for item in updated.get("events", []))
    assert event_types["await_external"] == 1
    assert event_types["completed"] == 0


def test_cancel_terminal_task_skips_redundant_mailbox_sync(tmp_path, monkeypatch) -> None:
    store = SharedBridgeStore(tmp_path)
    store.ensure_ready()
    task = store.create_monthly_auto_once_task(
        created_by_role="external",
        created_by_node_id="ext-01",
        requested_by="manual",
    )

    claimed = store.claim_next_task(role_target="internal", node_id="int-01", lease_sec=30)
    assert claimed is not None
    token = next(
        (stage.get("claim_token", "") for stage in claimed.get("stages", []) if stage.get("stage_id") == "internal_download"),
        "",
    )
    store.complete_stage(
        task_id=task["task_id"],
        stage_id="internal_download",
        claim_token=token,
        side="internal",
        stage_result={"status": "ok"},
        next_task_status="success",
        task_result={"status": "success"},
    )

    sync_calls: list[str] = []
    original_sync = store._sync_task_mailbox_from_conn

    def _tracked_sync(conn, task_id, *, side_hint=""):  # noqa: ANN001
        sync_calls.append(str(task_id))
        return original_sync(conn, task_id, side_hint=side_hint)

    monkeypatch.setattr(store, "_sync_task_mailbox_from_conn", _tracked_sync)

    assert store.cancel_task(task["task_id"]) is True
    assert sync_calls == []


def test_cancel_task_writes_mailbox_after_sqlite_lock_released(tmp_path, monkeypatch) -> None:
    store = SharedBridgeStore(tmp_path)
    store.ensure_ready()
    task = store.create_monthly_auto_once_task(
        created_by_role="external",
        created_by_node_id="ext-01",
        requested_by="manual",
    )
    lock_states: list[bool] = []

    def _track_request(payload):  # noqa: ANN001
        lock_states.append(store._write_lock.locked())
        return None

    def _track_side_snapshot(*, task, side):  # noqa: ANN001
        lock_states.append(store._write_lock.locked())
        return None

    monkeypatch.setattr(store._mailbox_store, "write_request", _track_request)
    monkeypatch.setattr(store._mailbox_store, "write_side_snapshot", _track_side_snapshot)

    assert store.cancel_task(task["task_id"]) is True
    assert lock_states
    assert all(locked is False for locked in lock_states)


def test_write_connect_uses_delete_journal_mode(monkeypatch, tmp_path) -> None:
    store = SharedBridgeStore(tmp_path)
    captured: dict[str, object] = {}

    class _FakeConnection:
        def __init__(self) -> None:
            self.in_transaction = False
            self.row_factory = None
            self.executed: list[str] = []

        def execute(self, sql, *_args, **_kwargs):  # noqa: ANN001
            self.executed.append(str(sql))
            return self

        def close(self) -> None:
            return None

        def rollback(self) -> None:
            return None

    fake_conn = _FakeConnection()

    def _fake_connect(database, **kwargs):  # noqa: ANN001
        captured["database"] = database
        captured["kwargs"] = kwargs
        return fake_conn

    monkeypatch.setattr(sqlite3, "connect", _fake_connect)

    with store.connect() as conn:
        assert conn is fake_conn

    assert captured["database"] == str(store.db_path)
    assert "PRAGMA journal_mode=DELETE" in fake_conn.executed
    assert "PRAGMA synchronous=NORMAL" in fake_conn.executed


def test_store_alias_paths_share_same_bridge_db(tmp_path) -> None:
    canonical_root = tmp_path / "shared"
    alias_root = tmp_path / "nested" / ".." / "shared"

    writer_store = SharedBridgeStore(canonical_root)
    writer_store.ensure_ready()
    task = writer_store.create_internal_browser_alert_task(
        building="A楼",
        failure_kind="login_failed",
        alert_state="problem",
        status_key="login_failed",
        summary="A楼 登录失败",
        latest_detail="A楼 登录失败",
        first_seen_at="2026-04-10 10:00:00",
        last_seen_at="2026-04-10 10:00:00",
        resolved_at="",
        occurrence_count=1,
        still_unresolved=True,
        created_by_role="internal",
        created_by_node_id="int-01",
    )

    reader_store = SharedBridgeStore(str(alias_root))
    fetched = reader_store.get_task(task["task_id"])

    assert fetched is not None
    assert fetched["task_id"] == task["task_id"]
    assert fetched["feature"] == "internal_browser_alert"
