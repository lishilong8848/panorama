"""巡检联机实时采集（仅内网端内部改动）隔离测试。

覆盖：
1. 同桶 ready 时，巡检强制补采必须绕过内外两层缓存短路，真正调用采集函数（run_inspection_data_only）；
2. 未授权/错误令牌的巡检强制补采端点不进入 runner，授权令牌才放行；
3. 桥接启用时仅跳过电气/暖通巡检的自动（小时/重试）采集，手动补采与其它族不受影响；
4. 配置默认值、规范化与运行时键往返。

全部用例 mock 采集服务/网络/线程，不访问真实网络、不读写真实配置文件。
"""
from __future__ import annotations

import json
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config.config_adapter import adapt_runtime_config, ensure_v3_config  # noqa: E402
from app.config.inspection_data_config import (  # noqa: E402
    FAMILY_ELECTRICAL_INSPECTION,
    FAMILY_HVAC_INSPECTION,
    INSPECTION_REPORT_TEMPLATES,
)
from app.modules.internal_bridge_http.api import routes  # noqa: E402
from app.modules.internal_bridge_http.service.internal_bridge_http_runner import (  # noqa: E402
    InternalBridgeHttpTaskRunner,
)
from app.modules.shared_bridge.service.shared_bridge_runtime_service import (  # noqa: E402
    SharedBridgeRuntimeService,
)
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore  # noqa: E402
from app.modules.shared_bridge.service.shared_source_cache_service import (  # noqa: E402
    FAMILY_HANDOVER_LOG,
    SharedSourceCacheService,
)
import app.modules.shared_bridge.service.shared_source_cache_service as cache_module  # noqa: E402
import handover_log_module.service.handover_download_service as download_module  # noqa: E402

BUCKET = "2026-10-27 08"
TEST_TOKEN = "inspection-bridge-token-for-tests"
FILL_METHODS = {
    FAMILY_ELECTRICAL_INSPECTION: "fill_electrical_inspection_latest",
    FAMILY_HVAC_INSPECTION: "fill_hvac_inspection_latest",
}


# --------------------------------------------------------------------------- helpers


def bridge_runtime_config(enabled: bool = True, token: str = TEST_TOKEN, root: Path | None = None) -> dict:
    config = {
        "deployment": {"role_mode": "internal"},
        "paths": {"runtime_state_root": str(root or ".runtime")},
        "shared_bridge": {"enabled": True, "root_dir": str(root or "")},
        "internal_source_cache": {"enabled": True},
        "inspection_data": {"enabled": True},
        "inspection_data_upload": {"lighthouse_bridge": {"enabled": enabled, "token": token}},
        "internal_source_sites": [{"building": f"{b}楼", "enabled": True} for b in "ABCDE"],
    }
    return config


@pytest.fixture
def cache(tmp_path):
    store = SharedBridgeStore(tmp_path)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=bridge_runtime_config(root=tmp_path),
        store=store,
        emit_log=lambda _line: None,
    )
    service.shared_root = tmp_path
    yield service
    service.stop()


def dataset(family: str, building: str = "D楼", bucket: str = BUCKET) -> dict:
    end = datetime.strptime(bucket, "%Y-%m-%d %H") + timedelta(minutes=20)
    return {
        "query_start": f"{bucket}:00:00",
        "query_end": end.strftime("%Y-%m-%d %H:%M:%S"),
        "rows": [[f"{building}位置", "设备", "测点", "1"]],
    }


def stored_entry(cache: SharedSourceCacheService, family: str, building: str = "D楼", bucket: str = BUCKET):
    """通过服务自身的登记入口写入同桶 ready 快照（模拟“已就绪”，不接触网络）。"""
    source = cache.shared_root / "_seed" / f"{family}-{building}-{bucket}.json"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(
        json.dumps(
            {
                "schema_version": 1, "building": building, "source_family": family, "bucket_key": bucket,
                "report_name": INSPECTION_REPORT_TEMPLATES[family],
                "query_start": f"{bucket}:00:00", "query_end": f"{bucket}:20:00",
                "rows": [[building, "设备", "测点", "1"]], "point_overrides": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return cache._store_entry(
        source_family=family, building=building, bucket_kind="latest", bucket_key=bucket,
        duty_date=bucket[:10], duty_shift="", source_path=source, status="ready",
        metadata={"family": family, "building": building},
    )


def test_same_bucket_force_refresh_bypasses_both_ready_shortcuts_and_reaches_collector(cache, monkeypatch):
    """同一桶已 ready 时，强制补采必须穿透后台旧缓存短路与采集函数内短路，真正调用采集。"""
    family = FAMILY_ELECTRICAL_INSPECTION
    stored_entry(cache, family)
    assert cache._existing_latest_refresh_result(source_family=family, building="D楼", bucket_key=BUCKET) is not None
    assert cache._get_ready_entry(
        source_family=family, building="D楼", bucket_kind="latest", bucket_key=BUCKET,
    ) is not None

    collector_calls: list[str] = []
    refresh_calls: list[dict] = []
    real_refresh = cache._refresh_family_bucket

    def fake_collector(family_arg, building):
        collector_calls.append(building)
        return dataset(family_arg, building)

    def fake_run_inspection_data_only(self, *, building, source_family, emit_log=None):
        return fake_collector(source_family, building)

    def spy_refresh(**kwargs):
        refresh_calls.append(dict(kwargs))
        return real_refresh(**kwargs)

    monkeypatch.setattr(download_module.HandoverDownloadService, "run_inspection_data_only", fake_run_inspection_data_only)
    monkeypatch.setattr(cache_module, "load_handover_config", lambda _config: {})
    monkeypatch.setattr(cache, "_refresh_family_bucket", spy_refresh)

    started = cache.start_building_latest_refresh(
        source_family=family, building="D楼", target_bucket_key=BUCKET, force_refresh=True,
    )
    assert started["accepted"] is True and started["reason"] == "started"

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with cache._lock:
            alive = cache._building_latest_refresh_threads.get((family, "D楼", "latest", BUCKET))
        if alive is None:
            break
        time.sleep(0.01)

    # 外层缓存短路未生效：后台没有走“现有文件已就绪”而直接进入真实刷新。
    assert refresh_calls and refresh_calls[0]["force_refresh_existing"] is True
    # 内层短路未生效：同桶 ready 仍调用采集服务。
    assert collector_calls == ["D楼"]

    # 非强制调用仍保持原短路行为：不重复采集。
    result = cache.start_building_latest_refresh(source_family=family, building="D楼", target_bucket_key=BUCKET)
    assert result["reason"] == "already_ready"
    assert collector_calls == ["D楼"]


def test_force_refresh_rejected_for_unrelated_family(cache):
    with pytest.raises(ValueError):
        cache.start_building_latest_refresh(
            source_family=FAMILY_HANDOVER_LOG, building="A楼", target_bucket_key=BUCKET, force_refresh=True,
        )


# --------------------------------------------------------------------------- endpoint auth


def make_request(runtime_config: dict, authorization: str | None = None):
    headers = {}
    if authorization is not None:
        headers["authorization"] = authorization
    container = SimpleNamespace(
        runtime_config=runtime_config,
        config={"common": {"internal_bridge_http": {"enabled": True, "allowed_client_ips": []}}},
    )
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(container=container)),
        client=SimpleNamespace(host="127.0.0.1"),
        headers=headers,
    )


class FakeRunner:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def refresh_latest_source_cache(self, **kwargs):
        self.calls.append(dict(kwargs))
        return {"ok": True, "source_family": kwargs.get("source_family")}


def bind_runner(monkeypatch, runner: FakeRunner) -> None:
    monkeypatch.setattr(routes, "_runner", lambda _request: runner)


@pytest.mark.parametrize(
    "peer, allowlist",
    [
        ("8.8.8.8", []),
        ("", []),
        ("192.168.1.5", ["192.168.1.6"]),
    ],
)
def test_unauthorized_inspection_force_refresh_never_reaches_runner(monkeypatch, peer, allowlist):
    runner = FakeRunner()
    bind_runner(monkeypatch, runner)
    request = make_request({})
    request.client.host = peer
    request.app.state.container.config["common"]["internal_bridge_http"]["allowed_client_ips"] = allowlist
    with pytest.raises(routes.HTTPException) as excinfo:
        routes.refresh_internal_latest_source_cache(
            request,
            {"source_family": FAMILY_HVAC_INSPECTION, "buildings": ["D楼"], "target_bucket_key": BUCKET,
             "force_refresh": True},
        )
    assert excinfo.value.status_code == 403
    assert TEST_TOKEN not in str(excinfo.value.detail)
    assert runner.calls == []


def test_authorized_inspection_force_refresh_reaches_runner_with_force_flag(monkeypatch):
    runner = FakeRunner()
    bind_runner(monkeypatch, runner)
    for family in INSPECTION_REPORT_TEMPLATES:
        request = make_request({})
        request.client.host = "192.168.1.5"
        result = routes.refresh_internal_latest_source_cache(
            request,
            {"source_family": family, "buildings": ["D楼"], "target_bucket_key": BUCKET, "force_refresh": True},
        )
        assert result["ok"] is True
    assert len(runner.calls) == len(INSPECTION_REPORT_TEMPLATES)
    for call in runner.calls:
        assert call["force_refresh"] is True
        assert call["inspection_force_authorized"] is True


def test_non_force_and_other_family_calls_keep_legacy_tokenless_behavior(monkeypatch):
    runner = FakeRunner()
    bind_runner(monkeypatch, runner)
    request = make_request(bridge_runtime_config(enabled=False), None)
    # 巡检非强制：不带令牌照常放行。
    routes.refresh_internal_latest_source_cache(
        request, {"source_family": FAMILY_ELECTRICAL_INSPECTION, "buildings": ["D楼"]},
    )
    # 其它族：不带令牌照常放行。
    routes.refresh_internal_latest_source_cache(
        request, {"source_family": FAMILY_HANDOVER_LOG, "buildings": ["A楼"]},
    )
    assert [call["source_family"] for call in runner.calls] == [FAMILY_ELECTRICAL_INSPECTION, FAMILY_HANDOVER_LOG]
    assert all("force_refresh" not in call for call in runner.calls)
    assert all("inspection_force_authorized" not in call for call in runner.calls)


def test_runner_backstop_refuses_unverified_inspection_force(tmp_path, monkeypatch):
    runtime = SharedBridgeRuntimeService(runtime_config=bridge_runtime_config(root=tmp_path), app_version="test")
    cache_service = Mock()
    runtime._source_cache_service = cache_service
    runner = InternalBridgeHttpTaskRunner(runtime_service=runtime, emit_log=lambda _line: None)

    with pytest.raises(PermissionError):
        runner.refresh_latest_source_cache(
            source_family=FAMILY_ELECTRICAL_INSPECTION, buildings=["D楼"], target_bucket_key=BUCKET, force_refresh=True,
        )
    cache_service.start_building_latest_refresh.assert_not_called()

    result = runner.refresh_latest_source_cache(
        source_family=FAMILY_ELECTRICAL_INSPECTION, buildings=["D楼"], target_bucket_key=BUCKET,
        force_refresh=True, inspection_force_authorized=True,
    )
    assert result["ok"] is True
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not cache_service.start_building_latest_refresh.called:
        time.sleep(0.01)
    kwargs = cache_service.start_building_latest_refresh.call_args.kwargs
    assert kwargs["source_family"] == FAMILY_ELECTRICAL_INSPECTION
    assert kwargs["force_refresh"] is True
    assert "inspection_force_authorized" not in kwargs


# --------------------------------------------------------------------------- automatic skip


def test_bridge_enabled_skips_only_automatic_inspection_in_hourly_and_retry(cache, monkeypatch):
    calls: list[list[str]] = []
    refreshed: list[str] = []

    def spy_pipeline(*, steps, **_kwargs):
        calls.append([step[0] for step in steps])
        return {"failed_units": [], "blocked_units": [], "running_units": [], "completed_units": []}

    def spy_refresh_family(*, source_family, **_kwargs):
        refreshed.append(source_family)
        return {}

    monkeypatch.setattr(cache, "_ensure_dirs", lambda: None)
    monkeypatch.setattr(cache, "_run_latest_source_steps_by_building", spy_pipeline)
    monkeypatch.setattr(cache, "_refresh_family_bucket", spy_refresh_family)

    # 桥接启用：自动小时轮次不含巡检，其它族照常。
    cache._run_current_bucket_once(BUCKET)
    assert calls and all(family not in INSPECTION_REPORT_TEMPLATES for family in calls[-1])
    assert FAMILY_HANDOVER_LOG in calls[-1]

    # 重试路径同样不发巡检自动补采。
    with cache._lock:
        for family in INSPECTION_REPORT_TEMPLATES:
            cache._family_status[family] = {"failed_buildings": ["D楼"], "blocked_buildings": ["E楼"]}
    cache._retry_inspection_data_if_needed()
    assert refreshed == []

    # 手动补采（配置保存/按钮触发）仍然可用。
    manual = cache.start_building_latest_refresh(
        source_family=FAMILY_ELECTRICAL_INSPECTION, building="D楼", target_bucket_key=BUCKET,
    )
    assert manual["accepted"] is True

    # 关闭桥接后自动流程恢复巡检。
    cache.runtime_config["inspection_data_upload"]["lighthouse_bridge"]["enabled"] = False
    cache._run_current_bucket_once(BUCKET)
    assert all(family in calls[-1] for family in INSPECTION_REPORT_TEMPLATES)

    # 启用但令牌为空 → 不视为联机模式，自动流程不跳过。
    cache.runtime_config["inspection_data_upload"]["lighthouse_bridge"] = {
        "enabled": True, "token": "   ",
    }
    cache._run_current_bucket_once(BUCKET)
    assert all(family in calls[-1] for family in INSPECTION_REPORT_TEMPLATES)


def test_inspection_data_feature_itself_not_disabled_by_bridge(cache):
    assert cache._inspection_live_bridge_active() is True
    assert cache._family_buildings(FAMILY_ELECTRICAL_INSPECTION) == ["D楼", "E楼"]
    assert cache._family_buildings(FAMILY_HVAC_INSPECTION) == ["D楼", "E楼"]
    assert cache.runtime_config["inspection_data"]["enabled"] is True


# --------------------------------------------------------------------------- config roundtrip


def test_config_defaults_normalization_and_runtime_key():
    from app.config.config_adapter import sync_runtime_back_to_v3
    from app.config.secret_masking import mask_settings, merge_masked_values
    import json
    default = ensure_v3_config({})["features"]["inspection_data_upload"]
    assert default == {"lighthouse_bridge": {"enabled": False, "token": ""}}
    raw = {"version": 3, "common": {}, "features": {"inspection_data": {"enabled": True},
           "inspection_data_upload": {"lighthouse_bridge": {"enabled": True, "token": TEST_TOKEN}}}}
    normalized = ensure_v3_config(raw)
    runtime = adapt_runtime_config(normalized)
    assert runtime["inspection_data_upload"] == raw["features"]["inspection_data_upload"]
    assert sync_runtime_back_to_v3(normalized, runtime)["features"]["inspection_data_upload"] == runtime["inspection_data_upload"]
    service = SharedSourceCacheService(runtime_config=runtime, store=None, emit_log=lambda _: None)
    assert service._inspection_live_bridge_active()
    masked = mask_settings(normalized)
    assert TEST_TOKEN not in json.dumps(masked)
    assert merge_masked_values(masked, normalized) == normalized
    raw["features"]["inspection_data_upload"]["lighthouse_bridge"]["token"] = ""
    with pytest.raises(ValueError): ensure_v3_config(raw)
