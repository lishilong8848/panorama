"""H楼审核页 status 自动拉取培训接口并合并展示（方案A，后端）的回归。

- auto_refresh_training=True（仅 status GET 传）：拉取接口 + 与已保存 KV 合并——手工/已存在记录保留、
  接口新增按 row_id=training:{taskId} 去重补入；同一 taskId 以 KV（可能已被操作者编辑）为准，不被接口覆盖。
- auto_refresh_training=False（保存/签名/关注点刷新等共用 _station_h_status_payload 的调用）：不拉取，仅返回 KV。
- 接口失败：静默回退 KV，不阻断审核页。
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

from app.modules.handover_review.api import routes


def _patch_status(monkeypatch, *, kv_rows, deleted_ids=()):
    selection = {
        "current_people": ["历史值班"], "next_people": ["历史接班"], "long_day_people": [],
        "training_rows": kv_rows, "deleted_training_row_ids": list(deleted_ids), "sections": {},
    }
    selection_service = SimpleNamespace(
        get_selection=lambda *_a: dict(selection),
        resolve_selection=lambda **_kw: dict(selection),
    )
    monkeypatch.setattr(routes, "_handover_cfg", lambda _c: {"training_management_section": {"enabled": True, "api_key": "k"}})
    monkeypatch.setattr(routes, "_STATION_H_TRAINING_PREVIEW", SimpleNamespace(read=Mock(return_value={"mock": None})))
    monkeypatch.setattr(routes, "_is_current_handover_duty_context", lambda **_kw: False)
    monkeypatch.setattr(routes, "_build_station_h_review_selection_service", lambda _c: selection_service)
    monkeypatch.setattr(routes, "_station_h_roster_candidate_people", Mock(side_effect=AssertionError("history must not query roster")))
    monkeypatch.setattr(routes, "_schedule_station_h_roster_refresh", lambda *a, **k: None)
    monkeypatch.setattr(routes, "_build_station_h_duty_focus_service",
                        lambda _c: SimpleNamespace(build_status=lambda **kw: {"rows": [], "image_status": {"available": True}}))


def _manual_row(row_id="manual-training:1", title="手工培训"):
    return {"row_id": row_id, "cells": {"B": title, "D": "", "E": "", "F": "", "G": ""}}


def test_status_auto_refresh_merges_api_training_preserving_manual(monkeypatch):
    _patch_status(monkeypatch, kv_rows=[_manual_row()])
    api_rows = [{"row_id": "training:task-1", "cells": {"B": "接口培训", "D": "17：30-18：00", "E": "理论", "F": "王庆华", "G": "甲、乙"}}]
    calls = []

    def fake_build(self, **kwargs):
        calls.append(kwargs)
        return {routes.TRAINING_SECTION: api_rows}

    monkeypatch.setattr(routes.TrainingManagementPayloadBuilder, "build", fake_build)
    payload = routes._station_h_status_payload(object(), duty_date="2026-03-01", duty_shift="night", auto_refresh_training=True)

    ids = [row["row_id"] for row in payload["training_rows"]]
    assert "manual-training:1" in ids, "手工记录必须保留"
    assert "training:task-1" in ids, "接口新增必须补入"
    assert payload["training_preview_row_ids"] == ["training:task-1"]
    assert len(calls) == 1 and calls[0]["building"] == "H楼"


def test_status_without_auto_refresh_does_not_pull(monkeypatch):
    _patch_status(monkeypatch, kv_rows=[_manual_row()])
    calls = []

    def fake_build(self, **kwargs):
        calls.append(kwargs)
        return {routes.TRAINING_SECTION: []}

    monkeypatch.setattr(routes.TrainingManagementPayloadBuilder, "build", fake_build)
    payload = routes._station_h_status_payload(object(), duty_date="2026-03-01", duty_shift="night")

    assert [row["row_id"] for row in payload["training_rows"]] == ["manual-training:1"]
    assert calls == [], "非 status GET 的共用调用不得拉取接口"
    routes._STATION_H_TRAINING_PREVIEW.read.assert_not_called()


def test_status_auto_refresh_falls_back_to_kv_on_api_error(monkeypatch):
    _patch_status(monkeypatch, kv_rows=[_manual_row()])

    def boom(self, **kwargs):
        raise ValueError("培训接口请求失败或超时，已保留原有内容，请稍后刷新")

    monkeypatch.setattr(routes.TrainingManagementPayloadBuilder, "build", boom)
    payload = routes._station_h_status_payload(object(), duty_date="2026-03-01", duty_shift="night", auto_refresh_training=True)

    assert [row["row_id"] for row in payload["training_rows"]] == ["manual-training:1"], "接口失败应回退 KV，不阻断审核页"


def test_status_auto_refresh_keeps_edited_row_for_same_task_id(monkeypatch):
    # KV 已含某接口行且被操作者改过；再次自动拉取不得重复追加、也不得用接口原值覆盖已编辑内容。
    kv_rows = [{"row_id": "training:task-1", "cells": {"B": "已编辑标题", "D": "", "E": "", "F": "", "G": ""}}]
    _patch_status(monkeypatch, kv_rows=kv_rows)
    api_rows = [{"row_id": "training:task-1", "cells": {"B": "接口原值", "D": "17：30-18：00", "E": "理论", "F": "王庆华", "G": "甲、乙"}}]
    monkeypatch.setattr(routes.TrainingManagementPayloadBuilder, "build", lambda self, **k: {routes.TRAINING_SECTION: api_rows})

    payload = routes._station_h_status_payload(object(), duty_date="2026-03-01", duty_shift="night", auto_refresh_training=True)
    rows = payload["training_rows"]

    assert len(rows) == 1, "同一 taskId 不得重复追加"
    assert rows[0]["cells"]["B"] == "已编辑标题", "已编辑记录以 KV 为准，不被接口覆盖"
    assert payload["training_preview_row_ids"] == []


def test_status_preview_does_not_restore_deleted_training(monkeypatch):
    _patch_status(monkeypatch, kv_rows=[_manual_row()], deleted_ids=["training:task-1"])
    monkeypatch.setattr(
        routes.TrainingManagementPayloadBuilder, "build",
        lambda self, **_: {routes.TRAINING_SECTION: [_manual_row("training:task-1")]},
    )
    payload = routes._station_h_status_payload(object(), duty_date="2026-03-01", duty_shift="night", auto_refresh_training=True)
    assert [row["row_id"] for row in payload["training_rows"]] == ["manual-training:1"]
    assert payload["training_preview_row_ids"] == []
