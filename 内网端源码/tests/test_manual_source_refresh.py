from types import SimpleNamespace
from unittest.mock import Mock

import openpyxl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.modules.shared_bridge.api import routes
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.modules.shared_bridge.service.shared_source_cache_service import SharedSourceCacheService


@pytest.fixture
def client(monkeypatch):
    service = SimpleNamespace(start_building_latest_source_cache_refresh=Mock())
    app = FastAPI()
    app.state.container = SimpleNamespace(shared_bridge_service=service, add_system_log=Mock())
    app.include_router(routes.router)
    monkeypatch.setattr(routes, "_deployment_role_mode", lambda _: "internal")
    with TestClient(app) as http:
        yield http, service.start_building_latest_source_cache_refresh


@pytest.mark.parametrize("building", ["A楼", "E楼"])
@pytest.mark.parametrize("family", ["monthly_report_family", "branch_power_family", "handover_log_family"])
def test_existing_file_is_success_not_conflict(client, building, family):
    http, refresh = client
    refresh.return_value = {
        "accepted": False, "running": False, "reason": "already_ready",
        "bucket_key": "2026-10-29", "relative_path": "existing.xlsx",
        "recovered_from_existing_file": True,
    }
    response = http.post("/api/bridge/source-cache/refresh-building-latest", params={
        "source_family": family, "building": building,
    })
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] and payload["ready"]
    assert not payload["accepted"] and not payload["running"]
    assert payload["relative_path"] == "existing.xlsx"
    assert "无需重复下载" in payload["message"]
    refresh.assert_called_once_with(source_family=family, building=building)


@pytest.mark.parametrize("result", [
    {"accepted": False, "running": True, "reason": "already_running"},
    {"accepted": True, "running": True},
])
def test_active_or_new_download_is_success(client, result):
    http, refresh = client
    refresh.return_value = result
    response = http.post("/api/bridge/source-cache/refresh-building-latest", params={
        "source_family": "monthly_report_family", "building": "E楼",
    })
    assert response.status_code == 200
    assert response.json()["running"] is True


@pytest.mark.parametrize("reason", ["disabled", "invalid_building", "bucket_unavailable", "unexpected_reason"])
def test_unavailable_does_not_become_false_success(client, reason):
    http, refresh = client
    refresh.return_value = {"accepted": False, "reason": reason}
    response = http.post("/api/bridge/source-cache/refresh-building-latest", params={
        "source_family": "monthly_report_family", "building": "E楼",
    })
    assert response.status_code == 409
    assert "E楼 全景平台月报源文件" in response.json()["detail"]


@pytest.mark.parametrize("family", ["electrical_inspection_family", "hvac_inspection_family"])
def test_inspection_only_accepts_de(client, family):
    http, refresh = client
    response = http.post("/api/bridge/source-cache/refresh-building-latest", params={
        "source_family": family, "building": "A楼",
    })
    assert response.status_code == 400
    refresh.assert_not_called()


def test_legacy_combined_inspection_not_refreshable(client):
    http, refresh = client
    response = http.post("/api/bridge/source-cache/refresh-building-latest", params={
        "source_family": "inspection_data_family", "building": "E楼",
    })
    assert response.status_code == 400
    refresh.assert_not_called()


def test_monthly_ready_requires_actual_file_and_exact_business_date(tmp_path):
    root = tmp_path / "shared"
    root.mkdir()
    store = SharedBridgeStore(root)
    store.ensure_ready()
    cache = SharedSourceCacheService(runtime_config={
        "deployment": {"role_mode": "internal"},
        "shared_bridge": {"enabled": True, "root_dir": str(root)},
        "internal_source_cache": {"enabled": True},
        "internal_source_sites": [{"building": "E楼", "enabled": True}],
    }, store=store, emit_log=lambda _: None)
    source = tmp_path / "monthly.xlsx"
    book = openpyxl.Workbook()
    book.active.append(["monthly", 1])
    book.save(source)
    book.close()
    entry = cache.store_existing_source_file(
        source_family="monthly_report_family", building="E楼", bucket_kind="latest",
        bucket_key="2026-10-29", duty_date="2026-10-29", duty_shift="",
        source_path=source, metadata={"upload_date": "2026-10-29"},
    )
    result = cache.start_building_latest_refresh(
        source_family="monthly_report_family", building="E楼", target_bucket_key="2026-10-29",
    )
    assert result["reason"] == "already_ready"
    assert result["accepted"] is False and result["running"] is False
    assert cache._existing_latest_refresh_result(
        source_family="monthly_report_family", building="E楼", bucket_key="2026-10-28",
        allow_directory_scan=False,
    ) is None
    cache._resolve_entry_file_path(entry).unlink()
    assert cache._existing_latest_refresh_result(
        source_family="monthly_report_family", building="E楼", bucket_key="2026-10-29",
        allow_directory_scan=False,
    ) is None
