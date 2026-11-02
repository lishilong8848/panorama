from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from app.modules.handover_review.api import routes


@pytest.fixture
def api(tmp_path, monkeypatch):
    config = {"_global_paths": {"runtime_state_root": str(tmp_path / "runtime")}}
    container = SimpleNamespace(add_system_log=Mock())
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))
    session = {"session_id": "A楼|2026-11-02|day", "building": "A楼", "duty_date": "2026-11-02", "duty_shift": "day"}
    monkeypatch.setattr(routes, "_handover_cfg", lambda _: config)
    monkeypatch.setattr(routes, "_build_review_session_service", lambda _: object())
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _, code: code.upper() + "楼")
    monkeypatch.setattr(routes, "_load_target_session_or_404", lambda *_, **__: session)
    lock = Mock()
    monkeypatch.setattr(routes, "_ensure_session_lock_held_or_409", lock)
    payload = {"session_id": session["session_id"], "client_id": "test-client", "mode": "every_shift",
               "values": {"maintenance_item": "机房巡检", "maintenance_party": "自维", "completion": "已完成", "executor": "测试人员"}}
    return request, payload, lock


def test_roundtrip_durable_rules_and_stale_update(api):
    request, payload, lock = api
    first = routes.handover_review_save_maintenance_recurring("a", request, payload)
    record = first["record"]
    lock.assert_called_once()
    assert routes.handover_review_list_maintenance_recurring("a", request)["records"] == [record]
    assert routes.handover_review_list_maintenance_recurring("b", request)["records"] == []
    revised = {**payload, "record_id": record["id"], "revision": record["revision"], "mode": "day_only"}
    new = routes.handover_review_save_maintenance_recurring("a", request, revised)["record"]
    with pytest.raises(HTTPException) as caught:
        routes.handover_review_save_maintenance_recurring("a", request, revised)
    assert caught.value.status_code == 409
    deleted = routes.handover_review_delete_maintenance_recurring("a", record["id"], request, {**payload, "revision": new["revision"]})
    assert deleted["records"] == []
    with pytest.raises(HTTPException) as caught:
        routes.handover_review_save_maintenance_recurring("a", request, {**revised, "revision": new["revision"]})
    assert caught.value.status_code == 404


def test_validation_and_lock_do_not_write(api):
    request, payload, lock = api
    for invalid in [{**payload, "mode": "weekly"}, {**payload, "values": {}}, {**payload, "session_id": ""}]:
        with pytest.raises(HTTPException) as caught:
            routes.handover_review_save_maintenance_recurring("a", request, invalid)
        assert caught.value.status_code == 400
    lock.side_effect = HTTPException(status_code=409, detail="locked")
    with pytest.raises(HTTPException) as caught:
        routes.handover_review_save_maintenance_recurring("a", request, payload)
    assert caught.value.status_code == 409
    assert routes.handover_review_list_maintenance_recurring("a", request)["records"] == []


def test_h_rejected(api):
    request, payload, _ = api
    with pytest.raises(HTTPException) as caught:
        routes.handover_review_save_maintenance_recurring("h", request, payload)
    assert caught.value.status_code == 400
