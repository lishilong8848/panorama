from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.modules.system_screenshot_upload.service import system_screenshot_upload_service as module
from app.modules.scheduler.api import system_screenshot_upload_routes as routes


class FakeInternal:
    def __init__(self, batch_id="batch-current"):
        self.batch_id = batch_id
        self.triggered = []
        self.cancelled = []
        self.files = [
            {"site_building": building, "target_key": target["key"], "file_exists": True,
             "file_name": f"{building}-{target['key']}.png", "capture_batch_id": batch_id,
             "captured_at": "2026-10-06 08:33:20", "modified_at": "2026-10-06 08:33:20"}
            for building in module.DEFAULT_BUILDINGS for target in module.DEFAULT_TARGETS
        ]

    def run_system_screenshot_capture(self, **kwargs):
        self.triggered.append(kwargs)
        return {"status": "running", "batch_id": "batch-current", "started_at": "2026-10-06 08:30:00"}

    def list_system_screenshot_files(self, **kwargs):
        return {"files": self.files}

    def download_system_screenshot_file(self, **kwargs):
        return b"test-png", kwargs["file_name"], "image/png"

    def cancel_system_screenshot_capture(self, **kwargs):
        self.cancelled.append(kwargs)


class FakeBitable:
    def __init__(self):
        self.created = []
        self.deleted = []

    def list_fields(self, table_id):
        return [{"field_name": name, "type": kind} for name, kind in
                [("楼栋", 1), ("日期", 5), ("截图", 17), ("分区", 3)]]

    def list_record_ids(self, table_id, **kwargs):
        return ["old-" + table_id]

    def batch_delete_records(self, table_id, record_ids, **kwargs):
        self.deleted.append(table_id)
        return len(record_ids)

    def upload_attachment_bytes(self, **kwargs):
        return "file-token"

    def batch_create_records(self, table_id, fields, **kwargs):
        self.created.extend((table_id, row) for row in fields)
        return fields


def test_join_current_batch_accepts_first_images_and_uploads_all_thirty():
    internal, bitable = FakeInternal(), FakeBitable()
    service = module.SystemScreenshotUploadService({}, internal_client=internal, bitable_client_factory=lambda _: bitable)
    result = service.run(capture_date="2026-10-06", internal_capture_force=True)
    assert result["uploaded_count"] == 30
    assert len(internal.triggered) == 1
    assert len(bitable.deleted) == 5
    assert len(bitable.created) == 30
    hvac = [fields for table, fields in bitable.created if table == "tblnnbx0tw3sFJoH"]
    assert len(hvac) == 10
    assert {fields["分区"] for fields in hvac} == {"A区", "B区"}


def test_recent_image_from_wrong_batch_is_not_uploaded(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(module.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    internal, bitable = FakeInternal("old-batch"), FakeBitable()
    for item in internal.files:
        item["captured_at"] = "2026-10-06 23:59:59"
    service = module.SystemScreenshotUploadService(
        {"system_screenshot_upload": {"wait_capture_timeout_sec": 1}},
        internal_client=internal, bitable_client_factory=lambda _: bitable,
    )
    with pytest.raises(RuntimeError, match="系统截图文件未更新"):
        service.run(capture_date="2026-10-06", internal_capture_force=True)
    assert not bitable.deleted
    assert not bitable.created
    assert len(internal.triggered) == 1


def test_cancellation_during_wait_cancels_exact_internal_batch():
    internal, bitable = FakeInternal(), FakeBitable()
    cancelled = [False]
    cleanup = []
    def check():
        if cancelled[0]:
            raise RuntimeError("cancelled")
    runtime = SimpleNamespace(raise_if_cancelled=check, is_cancelled=lambda: cancelled[0], register_cleanup_hook=cleanup.append)
    def listing(**kwargs):
        cancelled[0] = True
        return {"files": []}
    internal.list_system_screenshot_files = listing
    service = module.SystemScreenshotUploadService({}, internal_client=internal, bitable_client_factory=lambda _: bitable)
    with pytest.raises(RuntimeError, match="cancelled"):
        service.run(capture_date="2026-10-06", internal_capture_force=True, runtime=runtime)
    for hook in cleanup:
        hook()
    assert internal.cancelled == [{"batch_id": "batch-current"}]
    assert not bitable.deleted


def test_stop_persists_both_automatic_triggers_and_stops_inflight_poller(monkeypatch):
    calls = []
    container = SimpleNamespace(
        config={"features": {"system_screenshot_upload": {"scheduler": {"auto_start_in_gui": True}, "demand_poll": {"enabled": True}}}},
        stop_system_screenshot_demand_poller=lambda **kwargs: calls.append("poller-stopped"),
        stop_system_screenshot_upload_scheduler=lambda: calls.append("scheduler-stopped") or {},
    )
    def persist(current, config, **kwargs):
        calls.append("saved")
        current.config = config
    monkeypatch.setattr(routes, "persist_full_config", persist)
    monkeypatch.setattr(routes, "record_scheduler_config_autostart", Mock())
    monkeypatch.setattr(routes, "_build_payload", lambda *_args, **_kwargs: {})
    routes.system_screenshot_upload_scheduler_stop(SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container))))
    feature = container.config["features"]["system_screenshot_upload"]
    assert feature["scheduler"]["auto_start_in_gui"] is False
    assert feature["demand_poll"]["enabled"] is False
    assert calls == ["poller-stopped", "saved", "scheduler-stopped"]
