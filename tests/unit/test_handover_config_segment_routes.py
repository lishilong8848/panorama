from __future__ import annotations

import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.config.config_schema_v3 import DEFAULT_CONFIG_V3
from app.config.settings_loader import load_settings
from app.modules.report_pipeline.api import routes


def _write_default_config(path: Path) -> None:
    payload = copy.deepcopy(DEFAULT_CONFIG_V3)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _make_container(config_path: Path):
    config = load_settings(config_path)
    container = SimpleNamespace()
    container.config = config
    container.runtime_config = config
    container.config_path = config_path
    container.logs = []
    container.reload_calls = []
    container.apply_calls = []
    container.add_system_log = container.logs.append

    def _reload(saved):
        container.reload_calls.append(copy.deepcopy(saved))
        container.config = saved
        container.runtime_config = saved

    def _apply(saved, *, mode="light"):
        container.apply_calls.append(mode)
        container.config = saved
        container.runtime_config = saved

    container.reload_config = _reload
    container.apply_config_snapshot = _apply
    return container


def _make_request(container):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_handover_building_segment_routes_roundtrip(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)

    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None, raising=False)

    current = routes.get_handover_building_config_segment("A", request)
    next_payload = copy.deepcopy(current["data"])
    next_payload["cloud_sheet_sync"]["sheet_names"]["A楼"] = "A楼-路由保存"

    response = routes.put_handover_building_config_segment(
        "A",
        {"base_revision": current["revision"], "data": next_payload},
        request,
    )

    assert response["revision"] == current["revision"] + 1
    assert container.config["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["A楼"] == "A楼-路由保存"
    assert container.apply_calls == ["light"]
    assert container.reload_calls == []
    assert response["apply_mode"] == "business_only"
    assert response["reload_performed"] is False
    assert response["applied_services"] == ["config_snapshot", "runtime_config", "job_service_config"]
    assert any("交接班A楼配置已保存" in item for item in container.logs)


def test_handover_building_segment_persists_review_recipient_enabled_flag(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)

    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None, raising=False)

    current = routes.get_handover_building_config_segment("A", request)
    next_payload = copy.deepcopy(current["data"])
    next_payload.setdefault("review_ui", {})
    next_payload["review_ui"]["review_link_recipients_by_building"] = {
        "A楼": [
            {"note": "值班经理", "open_id": "ou_enabled", "enabled": True},
            {"note": "备用", "open_id": "ou_disabled", "enabled": False},
        ]
    }

    response = routes.put_handover_building_config_segment(
        "A",
        {"base_revision": current["revision"], "data": next_payload},
        request,
    )

    rows = response["data"]["review_ui"]["review_link_recipients_by_building"]["A楼"]
    assert rows == [
        {"note": "值班经理", "open_id": "ou_enabled", "enabled": True},
        {"note": "备用", "open_id": "ou_disabled", "enabled": False},
    ]


def test_handover_building_segment_normalizes_missing_recipient_enabled_flag(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)

    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None, raising=False)

    current = routes.get_handover_building_config_segment("A", request)
    next_payload = copy.deepcopy(current["data"])
    next_payload.setdefault("review_ui", {})
    next_payload["review_ui"]["review_link_recipients_by_building"] = {
        "A楼": [
            {"note": "默认启用", "open_id": "ou_default"},
            {"note": "显式关闭", "open_id": "ou_disabled", "enabled": False},
            {"note": "非布尔也按启用", "open_id": "ou_string", "enabled": "yes"},
        ]
    }

    response = routes.put_handover_building_config_segment(
        "A",
        {"base_revision": current["revision"], "data": next_payload},
        request,
    )

    rows = response["data"]["review_ui"]["review_link_recipients_by_building"]["A楼"]
    assert rows == [
        {"note": "默认启用", "open_id": "ou_default", "enabled": True},
        {"note": "显式关闭", "open_id": "ou_disabled", "enabled": False},
        {"note": "非布尔也按启用", "open_id": "ou_string", "enabled": True},
    ]


def test_handover_building_segment_route_rejects_stale_revision(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)

    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None, raising=False)

    current = routes.get_handover_building_config_segment("A", request)
    next_payload = copy.deepcopy(current["data"])
    next_payload["cloud_sheet_sync"]["sheet_names"]["A楼"] = "A楼-第一次"
    routes.put_handover_building_config_segment(
        "A",
        {"base_revision": current["revision"], "data": next_payload},
        request,
    )

    with pytest.raises(HTTPException) as exc:
        routes.put_handover_building_config_segment(
            "A",
            {"base_revision": current["revision"], "data": current["data"]},
            request,
        )

    assert exc.value.status_code == 409
    assert "当前楼配置已被其他人修改" in str(exc.value.detail)


def test_handover_common_segment_routes_roundtrip(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)

    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None, raising=False)

    current = routes.get_handover_common_config_segment(request)
    next_payload = copy.deepcopy(current["data"])
    next_payload["cloud_sheet_sync"]["root_wiki_url"] = "https://example.com/wiki/common"

    response = routes.put_handover_common_config_segment(
        {"base_revision": current["revision"], "data": next_payload},
        request,
    )

    assert response["revision"] == current["revision"] + 1
    assert container.config["features"]["handover_log"]["cloud_sheet_sync"]["root_wiki_url"] == "https://example.com/wiki/common"
    assert container.apply_calls == ["light"]
    assert container.reload_calls == []
    assert response["apply_mode"] == "business_only"
    assert response["reload_performed"] is False
    assert response["applied_services"] == ["config_snapshot", "runtime_config", "job_service_config"]
    assert any("交接班公共配置已保存" in item for item in container.logs)


def test_handover_common_segment_returns_review_access_snapshot(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)
    request.app.state._health_component_cache = {
        "handover_review_access:2026-04-22:day": {"ready": True, "value": {"review_base_url": "old"}},
        "other_component": {"ready": True, "value": "keep"},
    }

    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None, raising=False)

    current = routes.get_handover_common_config_segment(request)
    next_payload = copy.deepcopy(current["data"])
    next_payload.setdefault("review_ui", {})["public_base_url"] = "https://outer.example"

    response = routes.put_handover_common_config_segment(
        {"base_revision": current["revision"], "data": next_payload},
        request,
    )

    snapshot = response["handover_review_access"]
    assert snapshot["configured"] is True
    assert snapshot["review_base_url"] == "https://outer.example"
    assert snapshot["review_base_url_effective"] == "https://outer.example"
    assert container.config["features"]["handover_log"]["review_ui"]["public_base_url"] == "https://outer.example"
    assert "handover_review_access:2026-04-22:day" not in request.app.state._health_component_cache
    assert "other_component" in request.app.state._health_component_cache


def test_handover_common_segment_route_rejects_stale_revision(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)

    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None, raising=False)

    current = routes.get_handover_common_config_segment(request)
    next_payload = copy.deepcopy(current["data"])
    next_payload["cloud_sheet_sync"]["root_wiki_url"] = "https://example.com/wiki/common-first"
    routes.put_handover_common_config_segment(
        {"base_revision": current["revision"], "data": next_payload},
        request,
    )

    with pytest.raises(HTTPException) as exc:
        routes.put_handover_common_config_segment(
            {"base_revision": current["revision"], "data": current["data"]},
            request,
        )

    assert exc.value.status_code == 409
    assert "公共配置已被其他人修改" in str(exc.value.detail)


def test_put_config_keeps_segment_backed_handover_values(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)

    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None, raising=False)
    monkeypatch.setattr(routes, "_materialize_review_access_snapshot", lambda _container: {"configured": False})
    monkeypatch.setattr(routes, "_persist_manual_review_access_snapshot", lambda _container: {"configured": True})

    current_segment = routes.get_handover_building_config_segment("A", request)
    next_payload = copy.deepcopy(current_segment["data"])
    next_payload["cloud_sheet_sync"]["sheet_names"]["A楼"] = "A楼-最新段值"
    routes.put_handover_building_config_segment(
        "A",
        {"base_revision": current_segment["revision"], "data": next_payload},
        request,
    )

    route_root = tmp_path / "RouteRoot"
    stale_full_payload = copy.deepcopy(container.config)
    stale_full_payload["common"]["paths"]["business_root_dir"] = str(route_root)
    stale_full_payload["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["A楼"] = "A楼-整表旧值"

    response = routes.put_config(stale_full_payload, request)

    assert response["config"]["common"]["paths"]["business_root_dir"] == str(route_root)
    assert response["config"]["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["A楼"] == "A楼-最新段值"


def test_day_metric_config_repair_route_applies_and_reloads(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)

    monkeypatch.setattr(routes, "_materialize_review_access_snapshot", lambda _container: {"configured": False})
    monkeypatch.setattr(routes, "_persist_manual_review_access_snapshot", lambda _container: {"configured": True})

    response = routes.post_repair_day_metric_upload_config(request)

    assert response["ok"] is True
    assert response["repaired"] is False
    assert response["notes"] == ["12项规则已内置，无需修复"]
    assert container.config == load_settings(config_path)
    assert any("无需修复" in item for item in container.logs)


def test_day_metric_config_repair_route_no_change_does_not_save(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)

    monkeypatch.setattr(routes, "_materialize_review_access_snapshot", lambda _container: {"configured": False})
    monkeypatch.setattr(routes, "_persist_manual_review_access_snapshot", lambda _container: {"configured": True})

    def _fail_save_settings(*_args, **_kwargs):
        raise AssertionError("changed=False 时不应调用 save_settings")

    monkeypatch.setattr(routes, "save_settings", _fail_save_settings)

    response = routes.post_repair_day_metric_upload_config(request)

    assert response["ok"] is True
    assert response["repaired"] is False
    assert response["notes"] == ["12项规则已内置，无需修复"]
    assert any("无需修复" in item for item in container.logs)
