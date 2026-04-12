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
    container.add_system_log = container.logs.append

    def _reload(saved):
        container.config = saved
        container.runtime_config = saved

    container.reload_config = _reload
    return container


def _make_request(container):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_handover_building_segment_routes_roundtrip(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)

    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None)

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
    assert any("交接班A楼配置已保存" in item for item in container.logs)


def test_handover_building_segment_route_rejects_stale_revision(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)

    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None)

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

    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None)

    current = routes.get_handover_common_config_segment(request)
    next_payload = copy.deepcopy(current["data"])
    next_payload["cloud_sheet_sync"]["root_wiki_url"] = "https://example.com/wiki/common"

    response = routes.put_handover_common_config_segment(
        {"base_revision": current["revision"], "data": next_payload},
        request,
    )

    assert response["revision"] == current["revision"] + 1
    assert container.config["features"]["handover_log"]["cloud_sheet_sync"]["root_wiki_url"] == "https://example.com/wiki/common"
    assert any("交接班公共配置已保存" in item for item in container.logs)


def test_handover_common_segment_route_rejects_stale_revision(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)

    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None)

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

    monkeypatch.setattr(routes, "_invalidate_review_base_probe_cache", lambda: None)
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

    stale_full_payload = copy.deepcopy(container.config)
    stale_full_payload["common"]["paths"]["business_root_dir"] = r"D:\RouteRoot"
    stale_full_payload["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["A楼"] = "A楼-整表旧值"

    response = routes.put_config(stale_full_payload, request)

    assert response["config"]["common"]["paths"]["business_root_dir"] == r"D:\RouteRoot"
    assert response["config"]["features"]["handover_log"]["cloud_sheet_sync"]["sheet_names"]["A楼"] == "A楼-最新段值"


def test_day_metric_config_repair_route_applies_and_reloads(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)

    monkeypatch.setattr(routes, "_materialize_review_access_snapshot", lambda _container: {"configured": False})
    monkeypatch.setattr(routes, "_persist_manual_review_access_snapshot", lambda _container: {"configured": True})

    repaired_cfg = copy.deepcopy(container.config)
    repaired_cfg["common"]["feishu_auth"]["app_id"] = "cli_test"
    repaired_cfg["common"]["feishu_auth"]["app_secret"] = "sec_test"
    repaired_cfg["features"]["day_metric_upload"]["target"]["source"]["app_token"] = "app_test"
    repaired_cfg["features"]["day_metric_upload"]["target"]["source"]["table_id"] = "tbl_test"

    def _fake_repair(_cfg, _config_path):
        return copy.deepcopy(repaired_cfg), ["12项独立上传配置 <- backup"], True

    monkeypatch.setattr(routes, "repair_day_metric_related_settings", _fake_repair)

    response = routes.post_repair_day_metric_upload_config(request)

    assert response["ok"] is True
    assert response["repaired"] is True
    assert response["notes"] == ["12项独立上传配置 <- backup"]
    assert container.config["common"]["feishu_auth"]["app_id"] == "cli_test"
    assert container.config["features"]["day_metric_upload"]["target"]["source"]["app_token"] == "app_test"
    assert any("12项配置修复完成" in item for item in container.logs)


def test_day_metric_config_repair_route_no_change_does_not_save(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "表格计算配置.json"
    _write_default_config(config_path)
    container = _make_container(config_path)
    request = _make_request(container)

    monkeypatch.setattr(routes, "_materialize_review_access_snapshot", lambda _container: {"configured": False})
    monkeypatch.setattr(routes, "_persist_manual_review_access_snapshot", lambda _container: {"configured": True})
    monkeypatch.setattr(
        routes,
        "repair_day_metric_related_settings",
        lambda cfg, config_path=None: (copy.deepcopy(cfg), [], False),
    )

    def _fail_save_settings(*_args, **_kwargs):
        raise AssertionError("changed=False 时不应调用 save_settings")

    monkeypatch.setattr(routes, "save_settings", _fail_save_settings)

    response = routes.post_repair_day_metric_upload_config(request)

    assert response["ok"] is True
    assert response["repaired"] is False
    assert response["notes"] == []
    assert any("无需修复" in item for item in container.logs)
