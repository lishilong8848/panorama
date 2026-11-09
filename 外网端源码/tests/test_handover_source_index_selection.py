import pytest

from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService


@pytest.mark.parametrize("getter", ["get_handover_by_date_cache_entries", "get_handover_capacity_by_date_cache_entries"])
@pytest.mark.parametrize("require_fresh", [False, True])
def test_handover_index_selects_one_ready_version_per_building_and_shift(monkeypatch, getter, require_fresh):
    runtime = object.__new__(SharedBridgeRuntimeService)
    monkeypatch.setattr(runtime, "_http_bridge_should_try", lambda: True)
    day = "2026-11-08"
    same_shift = {"duty_date": day, "duty_shift": "night", "status": "ready", "file_verified": True}
    rows = [
        {**same_shift, "building": "A楼", "bucket_key": "2026-11-08 22", "file_path": "A-older.json"},
        {**same_shift, "building": "A楼", "bucket_key": "2026-11-09 01", "file_path": "A-newer.json"},
        {**same_shift, "building": "A楼", "bucket_key": "2026-11-09 08", "duty_shift": "day", "file_path": "A-wrong-shift.json"},
        {**same_shift, "building": "A楼", "bucket_key": "2026-11-09 09", "duty_date": "2026-11-09", "file_path": "A-wrong-date.json"},
        {**same_shift, "building": "B楼", "bucket_key": "2026-11-08 22", "file_path": "B.json"},
        {**same_shift, "building": "C楼", "bucket_key": day, "duty_shift": "", "file_path": "C-unknown-shift.json"},
    ]
    def index(**kwargs):
        assert kwargs["force_refresh"] is require_fresh
        assert kwargs["duty_shift"] == "night"
        return rows

    monkeypatch.setattr(runtime, "_http_source_index_entries", index)
    selected = getattr(runtime, getter)(duty_date=day, duty_shift="night", buildings=["B楼", "A楼", "C楼"], require_fresh=require_fresh)
    assert [(row["building"], row["file_path"]) for row in selected] == [("B楼", "B.json"), ("A楼", "A-newer.json")]
