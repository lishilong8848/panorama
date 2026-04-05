from __future__ import annotations

from datetime import datetime

import pytest

import handover_log_module.service.wet_bulb_collection_service as wet_bulb_collection_module
from handover_log_module.core.models import MetricHit
from handover_log_module.service.wet_bulb_collection_service import WetBulbCollectionService


def _build_cfg() -> dict:
    return {
        "fields": {
            "date": "日期",
            "building": "楼栋",
            "wet_bulb_temp": "天气湿球温度",
            "cooling_mode": "冷源运行模式",
            "sequence": "序号",
        },
        "cooling_mode": {
            "priority_order": ["1", "2", "3", "4"],
            "source_value_map": {"1": "制冷", "2": "预冷", "3": "板换", "4": "停机"},
            "upload_value_map": {
                "制冷": "机械制冷",
                "预冷": "预冷模式",
                "板换": "自然冷模式",
            },
            "skip_modes": ["停机"],
        },
        "target": {
            "app_token": "configured_token",
            "table_id": "table_demo",
        },
    }


def _build_success_target(*, target_kind: str = "base_token_pair") -> dict:
    configured_token = "configured_token"
    operation_token = "operation_token" if target_kind == "wiki_token_pair" else configured_token
    display_url = (
        f"https://vnet.feishu.cn/wiki/{configured_token}?table=table_demo"
        if target_kind == "wiki_token_pair"
        else f"https://vnet.feishu.cn/base/{configured_token}?table=table_demo"
    )
    return {
        "configured_app_token": configured_token,
        "operation_app_token": operation_token,
        "app_token": operation_token,
        "table_id": "table_demo",
        "target_kind": target_kind,
        "display_url": display_url,
        "bitable_url": display_url,
        "wiki_node_token": configured_token if target_kind == "wiki_token_pair" else "",
        "message": "",
        "resolved_at": "2026-03-27 16:00:00",
    }


def _build_runtime_config_for_run(*, enable_auto_switch_wifi: bool) -> dict:
    return {
        "network": {"enable_auto_switch_wifi": enable_auto_switch_wifi},
        "handover_log": {"download": {"switch_to_internal_before_download": False}},
        "wet_bulb_collection": {"enabled": True, "target": {"app_token": "configured_token", "table_id": "table_demo"}},
    }


def test_build_target_descriptor_uses_token_pair_preview(monkeypatch: pytest.MonkeyPatch) -> None:
    service = WetBulbCollectionService({"wet_bulb_collection": {"target": {"app_token": "configured_token", "table_id": "table_demo"}}})

    class _FakeResolver:
        def resolve_token_pair_preview(self, **kwargs):
            assert kwargs["configured_app_token"] == "configured_token"
            assert kwargs["table_id"] == "table_demo"
            assert kwargs["force_refresh"] is False
            return _build_success_target(target_kind="wiki_token_pair")

    monkeypatch.setattr(service, "_new_target_resolver", lambda: _FakeResolver())  # noqa: SLF001

    descriptor = service.build_target_descriptor()

    assert descriptor["target_kind"] == "wiki_token_pair"
    assert descriptor["configured_app_token"] == "configured_token"
    assert descriptor["operation_app_token"] == "operation_token"
    assert descriptor["display_url"] == "https://vnet.feishu.cn/wiki/configured_token?table=table_demo"


def test_new_client_uses_operation_app_token(monkeypatch: pytest.MonkeyPatch) -> None:
    service = WetBulbCollectionService(
        {
            "feishu": {"app_id": "app_id", "app_secret": "app_secret"},
            "wet_bulb_collection": {"target": {"app_token": "configured_token", "table_id": "table_demo"}},
        }
    )
    captured: dict[str, object] = {}

    class _FakeClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(wet_bulb_collection_module, "FeishuBitableClient", _FakeClient)

    service._new_client(_build_cfg(), target_descriptor=_build_success_target(target_kind="wiki_token_pair"))  # noqa: SLF001

    assert captured["app_token"] == "operation_token"
    assert captured["calc_table_id"] == "table_demo"
    assert captured["attachment_table_id"] == "table_demo"


def test_resolve_wet_bulb_value_accepts_metric_text_with_unit() -> None:
    service = WetBulbCollectionService({})
    hits = {
        "wet_bulb": MetricHit("wet_bulb", 1, "室外湿球温度", "26.4℃", "", ""),
    }

    value = service._resolve_wet_bulb_value(hits=hits, effective_config={})  # noqa: SLF001

    assert value == pytest.approx(26.4)


@pytest.mark.parametrize(
    ("building", "expected"),
    [("A楼", "1"), ("B楼", "2"), ("C楼", "3"), ("D楼", "4"), ("E楼", "5")],
)
def test_building_sequence_text_maps_building_to_text_number(building: str, expected: str) -> None:
    assert WetBulbCollectionService._building_sequence_text(building) == expected  # noqa: SLF001


def test_resolve_cooling_mode_prefers_code_1_when_multiple_modes_active() -> None:
    service = WetBulbCollectionService({})
    hits = {
        "chiller_mode_1": MetricHit("chiller_mode_1", 1, "1号冷机模式", 2, "", ""),
        "chiller_mode_2": MetricHit("chiller_mode_2", 2, "2号冷机模式", 1, "", ""),
        "chiller_mode_3": MetricHit("chiller_mode_3", 3, "3号冷机模式", 4, "", ""),
        "chiller_mode_4": MetricHit("chiller_mode_4", 4, "4号冷机模式", 4, "", ""),
        "chiller_mode_5": MetricHit("chiller_mode_5", 5, "5号冷机模式", 4, "", ""),
        "chiller_mode_6": MetricHit("chiller_mode_6", 6, "6号冷机模式", 4, "", ""),
    }
    effective_config = {
        "chiller_mode": {
            "west_keys": ["chiller_mode_1", "chiller_mode_2", "chiller_mode_3"],
            "east_keys": ["chiller_mode_4", "chiller_mode_5", "chiller_mode_6"],
        }
    }

    result = service._resolve_cooling_mode_value(hits=hits, effective_config=effective_config, cfg=_build_cfg())  # noqa: SLF001

    assert result["source_code"] == "1"
    assert result["source_text"] == "制冷"
    assert result["upload_text"] == "机械制冷"


class _FakeDownloadService:
    should_mark_switched = False
    switched_external_calls = 0
    last_config = None
    success_building = "A楼"

    def __init__(self, config, download_browser_pool=None):  # noqa: ARG002
        self.config = config
        self.did_switch_internal_this_run = False
        type(self).last_config = config

    def run(self, buildings=None, reuse_cached=True, emit_log=print):  # noqa: ARG002
        self.did_switch_internal_this_run = bool(type(self).should_mark_switched)
        return {
            "success_files": [{"building": type(self).success_building, "file_path": "fake.xlsx"}],
            "failed": [],
        }

    def switch_external_after_download(self, emit_log=print):  # noqa: ARG002
        type(self).switched_external_calls += 1


class _FakeExtractService:
    def __init__(self, config):  # noqa: ARG002
        pass

    def extract(self, building, data_file):  # noqa: ARG002
        return {
            "hits": {
                "wet_bulb": MetricHit("wet_bulb", 1, "室外湿球温度", 26.4, "", ""),
                "chiller_mode_1": MetricHit("chiller_mode_1", 1, "1号冷机模式", 1, "", ""),
                "chiller_mode_2": MetricHit("chiller_mode_2", 2, "2号冷机模式", 4, "", ""),
                "chiller_mode_3": MetricHit("chiller_mode_3", 3, "3号冷机模式", 4, "", ""),
                "chiller_mode_4": MetricHit("chiller_mode_4", 4, "4号冷机模式", 4, "", ""),
                "chiller_mode_5": MetricHit("chiller_mode_5", 5, "5号冷机模式", 4, "", ""),
                "chiller_mode_6": MetricHit("chiller_mode_6", 6, "6号冷机模式", 4, "", ""),
            },
            "effective_config": {
                "chiller_mode": {
                    "west_keys": ["chiller_mode_1", "chiller_mode_2", "chiller_mode_3"],
                    "east_keys": ["chiller_mode_4", "chiller_mode_5", "chiller_mode_6"],
                    "value_map": {"1": "制冷", "2": "预冷", "3": "板换", "4": "停机"},
                }
            },
        }


class _FakeClient:
    last_fields_list = None
    clear_calls = 0
    clear_deleted_count = 0

    def clear_table(self, **kwargs):  # noqa: ARG002
        type(self).clear_calls += 1
        return type(self).clear_deleted_count

    def batch_create_records(self, **kwargs):
        type(self).last_fields_list = kwargs.get("fields_list")
        return [{"record_id": "rec_new"}]


def test_run_uses_datetime_timestamp_and_target_descriptor(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeDownloadService.should_mark_switched = False
    _FakeDownloadService.switched_external_calls = 0
    _FakeDownloadService.last_config = None
    _FakeDownloadService.success_building = "A楼"
    _FakeClient.last_fields_list = None
    _FakeClient.clear_calls = 0
    _FakeClient.clear_deleted_count = 4

    monkeypatch.setattr(wet_bulb_collection_module, "load_handover_config", lambda runtime: runtime.get("handover_log", {}))
    monkeypatch.setattr(wet_bulb_collection_module, "HandoverDownloadService", _FakeDownloadService)
    monkeypatch.setattr(wet_bulb_collection_module, "HandoverExtractService", _FakeExtractService)

    fixed_ts = int(datetime(2026, 3, 27, 10, 11, 12).timestamp() * 1000)
    monkeypatch.setattr(WetBulbCollectionService, "_current_timestamp_ms", staticmethod(lambda: fixed_ts))

    service = WetBulbCollectionService(_build_runtime_config_for_run(enable_auto_switch_wifi=False))
    target_descriptor = _build_success_target(target_kind="wiki_token_pair")
    monkeypatch.setattr(service, "build_target_descriptor", lambda cfg=None, force_refresh=False: dict(target_descriptor))
    monkeypatch.setattr(service, "_new_client", lambda cfg, target_descriptor=None: _FakeClient())  # noqa: SLF001

    result = service.run(buildings=["A楼"], emit_log=lambda *_: None)

    assert result["status"] == "ok"
    assert result["deleted_count"] == 4
    assert result["created_count"] == 1
    assert result["target"] == target_descriptor
    assert _FakeDownloadService.last_config["download"]["switch_to_internal_before_download"] is False
    assert _FakeDownloadService.switched_external_calls == 0
    assert _FakeClient.clear_calls == 1
    assert _FakeClient.last_fields_list == [
        {
            "日期": fixed_ts,
            "楼栋": "A楼",
            "天气湿球温度": 26.4,
            "冷源运行模式": "机械制冷",
            "序号": "1",
        }
    ]


def test_run_fails_early_when_target_preview_is_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    service = WetBulbCollectionService(_build_runtime_config_for_run(enable_auto_switch_wifi=False))
    monkeypatch.setattr(
        service,
        "build_target_descriptor",
        lambda cfg=None, force_refresh=False: {
            "configured_app_token": "bad_token",
            "operation_app_token": "",
            "table_id": "table_demo",
            "target_kind": "invalid",
            "display_url": "",
            "bitable_url": "",
            "message": "目标既不是可访问的 Base，也不是可解析的 Wiki 多维表",
            "resolved_at": "2026-03-27 16:00:00",
        },
    )

    result = service.run(buildings=["A楼"], emit_log=lambda *_: None)

    assert result["status"] == "failed"
    assert result["uploaded_buildings"] == []
    assert result["failed_buildings"] == [
        {
            "building": "-",
            "error": "目标既不是可访问的 Base，也不是可解析的 Wiki 多维表",
            "code": "target_invalid",
        }
    ]


def test_continue_from_source_units_logs_runtime_error_failure_in_chinese(monkeypatch: pytest.MonkeyPatch) -> None:
    service = WetBulbCollectionService(_build_runtime_config_for_run(enable_auto_switch_wifi=False))
    logs: list[str] = []

    monkeypatch.setattr(
        service,
        "build_target_descriptor",
        lambda cfg=None, force_refresh=False: _build_success_target(target_kind="wiki_token_pair"),
    )
    monkeypatch.setattr(wet_bulb_collection_module, "load_handover_config", lambda runtime: runtime.get("handover_log", {}))

    class _ExplodingExtractService:
        def __init__(self, config):  # noqa: ARG002
            pass

        def extract(self, building, data_file):  # noqa: ARG002
            raise RuntimeError("page_timeout")

    monkeypatch.setattr(wet_bulb_collection_module, "HandoverExtractService", _ExplodingExtractService)

    result = service.continue_from_source_units(
        source_units=[{"building": "E楼", "file_path": "fake.xlsx"}],
        emit_log=logs.append,
        cfg=_build_cfg(),
        target_descriptor=_build_success_target(target_kind="wiki_token_pair"),
    )

    assert result["status"] == "failed"
    assert result["failed_buildings"] == [{"building": "E楼", "error": "页面超时", "code": "page_timeout"}]
    assert any("[湿球温度定时采集][E楼] 提取失败: 页面超时" in line for line in logs)
