from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests

from app.config.secret_masking import mask_settings, merge_masked_values
from handover_log_module.service.training_management_payload_builder import (
    BEIJING, TRAINING_SECTION, TrainingManagementPayloadBuilder,
    append_training_rows, attach_training_row_ids, latest_training_notice, training_people,
)


def item(task_id="task-1", building="A楼", time="2026-10-27 09:00", **extra):
    return {"taskId": task_id, "buildingName": building, "taskName": "动环培训",
            "signInNoticeTime": time, "instructors": ["曹毅"], **extra}


def builder(items, total=None):
    response = Mock(status_code=200)
    response.json.return_value = {"items": items, "total": len(items) if total is None else total}
    get = Mock(return_value=response)
    service = TrainingManagementPayloadBuilder({"training_management_section": {"api_key": "test-key"}},
                                               http_get=get, now=lambda: datetime(2026, 10, 27, tzinfo=BEIJING))
    return service, get


def build(service, **extra):
    return service.build(**{"building": "A楼", "duty_date": "2026-10-27",
                            "current_people": "张三、李四", "next_people": "李四 王五", **extra})[TRAINING_SECTION]


def test_mapping_defaults_people_and_bearer_header():
    service, get = builder([item()])
    rows = build(service)
    assert rows == [{"row_id": "training:task-1", "cells": {"B": "动环培训", "D": "17：30-18：00",
                    "E": "理论", "F": "曹毅", "G": "张三、李四、王五"}}]
    kwargs = get.call_args.kwargs
    assert kwargs["headers"] == {"Authorization": "Bearer test-key"}
    assert kwargs["allow_redirects"] is False
    assert "miaoda_oapi_key" not in kwargs["params"]
    get.return_value.close.assert_called_once()


def test_latest_notice_not_first_notice_and_building_match():
    service, _ = builder([
        item("1", signInNoticeTimes=["2026-10-27 07:00", "2026-10-28 09:00"]),
        item("2", time="2026-10-26 12:00", signInNoticeTimes=["2026-10-27 17:00", "bad"]),
        item("3", building="H楼"), item("4", building="B楼"), item("5", time="bad"),
    ])
    assert [r["row_id"] for r in build(service)] == ["training:2"]
    assert [r["row_id"] for r in build(service, building="H楼")] == ["training:3"]


def test_duplicate_id_moved_to_another_date_is_not_added():
    service, _ = builder([item(), item(time="2026-10-28 08:00")])
    assert build(service) == []


def test_timezone_uses_beijing_calendar_date():
    assert latest_training_notice(item(time="2026-10-26T17:30:00+00:00")).day == 27


def test_old_month_does_not_fetch_or_claim_empty_success():
    service, get = builder([])
    with pytest.raises(ValueError, match="当月"):
        build(service, duty_date="2026-09-27")
    get.assert_not_called()


@pytest.mark.parametrize("failure", [requests.Timeout("private-url"), requests.ConnectionError("private-url")])
def test_transient_error_is_sanitized(failure):
    service, get = builder([])
    get.side_effect = failure
    with pytest.raises(ValueError, match="已保留") as caught:
        build(service)
    assert "private-url" not in str(caught.value)


def test_truncated_response_does_not_claim_complete():
    service, _ = builder([item()], total=501)
    with pytest.raises(ValueError, match="不完整"):
        build(service)


def test_refresh_keeps_manual_rows_and_manual_edits_by_task_id():
    service, _ = builder([item()])
    original = build(service)
    original[0]["cells"]["B"] = "用户修改的标题"
    original[0]["cells"]["G"] = "自填人员"
    original.append({"row_id": "manual-1", "cells": {"B": "手工培训"}})
    before = deepcopy(original)
    assert append_training_rows(original, build(service)) == before
    assert original == before
    assert append_training_rows(original, []) == before


def test_generation_import_retains_task_identity_without_modifying_cells():
    service, _ = builder([item()])
    rows = build(service)
    document = {"sections": [{"name": TRAINING_SECTION, "rows": [{"row_id": "excel-row-40", "cells": rows[0]["cells"]}]}]}
    before = deepcopy(document["sections"][0]["rows"][0]["cells"])
    attach_training_row_ids(document, rows)
    assert document["sections"][0]["rows"][0]["row_id"] == "training:task-1"
    assert document["sections"][0]["rows"][0]["cells"] == before


def test_key_masked_and_restored_on_save():
    original = {"training_management_section": {"api_key": "test-training-key"}}
    masked = mask_settings(original)
    assert masked["training_management_section"]["api_key"] != "test-training-key"
    assert merge_masked_values(masked, original) == original


def test_people_are_deduplicated_without_adding_lecturers():
    assert training_people(["张三", "李四"], "李四、王五") == "张三、李四、王五"


def test_configuration_roundtrip_retains_training_settings():
    from app.config.config_adapter import ensure_v3_config, adapt_runtime_config, sync_runtime_back_to_v3
    config = ensure_v3_config({"handover_log": {"training_management_section": {"enabled": True, "api_key": "test-key"}}})
    runtime = adapt_runtime_config(config)
    assert runtime["handover_log"]["training_management_section"]["api_key"] == "test-key"
    restored = sync_runtime_back_to_v3(config, runtime)
    assert restored["features"]["handover_log"]["training_management_section"]["api_key"] == "test-key"


def test_refresh_route_uses_saved_duty_and_takeover_people(monkeypatch):
    from app.modules.handover_review.api import routes
    captured = {}
    container = SimpleNamespace(add_system_log=Mock())
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))
    session = {"session_id": "A楼|2026-10-27|night", "duty_date": "2026-10-27", "duty_shift": "night"}
    monkeypatch.setattr(routes, "_handover_cfg", lambda _c: {})
    monkeypatch.setattr(routes, "_build_review_services", lambda _c: (None, None, None, None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda *_a: "A楼")
    monkeypatch.setattr(routes, "_load_target_session_or_404", lambda *_a, **_k: session)
    monkeypatch.setattr(routes, "_fixed_cell_value_from_review_document", lambda _d, c: {"C3": "甲、乙", "G3": "丙、丁"}[c])
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *_a, **_k: SimpleNamespace(load_document=lambda _: ({}, session)))
    def fake_build(_self, **kwargs):
        captured.update(kwargs)
        return {TRAINING_SECTION: []}
    monkeypatch.setattr(routes.TrainingManagementPayloadBuilder, "build", fake_build)
    result = routes.handover_review_refresh_event_sections("a", request, {"session_id": session["session_id"], "section_name": TRAINING_SECTION})
    assert result["session_id"] == session["session_id"]
    assert captured["duty_date"] == "2026-10-27"
    assert captured["current_people"] == "甲、乙"
    assert captured["next_people"] == "丙、丁"
