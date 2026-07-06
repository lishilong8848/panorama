from __future__ import annotations

import threading
import time
from datetime import datetime

from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService
from handover_log_module.repository.event_followup_cache_store import EventFollowupCacheStore
from handover_log_module.repository.event_sections_repository import EventSectionQueryResult
from handover_log_module.service.day_metric_bitable_export_service import DayMetricBitableExportService
from handover_log_module.service.event_category_payload_builder import EventCategoryPayloadBuilder


def test_event_cache_record_not_found_is_carried_once_then_removed(tmp_path):
    store = EventFollowupCacheStore(
        cache_state_file=str(tmp_path / "handover_shared_cache.json"),
        global_paths={"runtime_state_root": str(tmp_path)},
    )
    store.save_state(
        {
            "pending_by_id": {
                "A楼::rec_missing": {
                    "record_id": "rec_missing",
                    "event_level": "三级",
                    "event_time": "2026-06-09 09:30:00",
                    "description": "测试历史事件",
                    "building_text": "A楼",
                    "final_status_text": "未完成",
                    "progress_text": "未完成",
                    "work_window_text": "/",
                }
            },
            "last_query_record_ids": ["rec_missing"],
        }
    )

    class Repo:
        def get_record_by_id(self, *, record_id: str):
            raise RuntimeError(
                "飞书接口调用失败: {'code': 1254043, 'msg': 'RecordIdNotFound'}"
            )

    builder = EventCategoryPayloadBuilder(
        {"template": {"source_path": str(tmp_path / "missing_template.xlsx")}},
        repository=Repo(),
        cache_store=store,
    )
    logs: list[str] = []
    result = builder.build(
        building="A楼",
        duty_date="2026-06-09",
        duty_shift="day",
        follower_text="",
        is_current_duty_context=True,
        preloaded_query_result_by_building={
            "A楼": EventSectionQueryResult(
                current_rows=[],
                outside_shift_ongoing_rows=[],
                historical_open_rows=[],
                shift_start=datetime(2026, 6, 9, 9, 0, 0),
                shift_end=datetime(2026, 6, 9, 18, 0, 0),
                cfg={
                    "enabled": True,
                    "cache": {"enabled": True},
                    "sections": {"new_event": "新事件处理", "history_followup": "历史事件跟进"},
                    "progress_text": {"done": "已完成", "todo": "未完成"},
                },
            )
        },
        emit_log=logs.append,
    )

    assert len(result["历史事件跟进"]) == 1
    assert store.list_pending_for_building("A楼") == []
    assert any("已清理失效历史缓存" in item for item in logs)
    assert not any("回查record失败" in item for item in logs)


def test_day_metric_create_failure_keeps_old_records():
    service = DayMetricBitableExportService({})
    cfg = service._defaults()

    class Client:
        def __init__(self):
            self.deleted: list[str] = []
            self.created = 0

        def list_records(self, **_kwargs):
            return [
                {
                    "record_id": "old_1",
                    "fields": {
                        "类型": "总负荷（KW）",
                        "楼栋": "A楼",
                        "日期": service._midnight_timestamp_ms("2026-06-09"),
                    },
                }
            ]

        def batch_create_records(self, **_kwargs):
            raise RuntimeError("飞书接口调用失败: {'code': 1254002, 'msg': 'Fail'}")

        def batch_delete_records(self, *, record_ids, **_kwargs):
            self.deleted.extend(record_ids)
            return len(record_ids)

    client = Client()
    service._new_client = lambda *_args, **_kwargs: client  # type: ignore[method-assign]
    service._prepare_records = lambda **_kwargs: (  # type: ignore[method-assign]
        [{"类型": "总负荷（KW）", "楼栋": "A楼", "日期": service._midnight_timestamp_ms("2026-06-09"), "数值": 1}],
        [],
    )

    logs: list[str] = []
    result = service._run_with_values(
        cfg=cfg,
        building="A楼",
        duty_date="2026-06-09",
        duty_shift="day",
        cell_values={},
        resolved_values_by_id={},
        metric_origin_context={},
        emit_log=logs.append,
    )

    assert result["status"] == "failed"
    assert client.deleted == []
    assert any("旧记录未删除" in item for item in logs)


def test_day_metric_create_then_delete_old_records():
    service = DayMetricBitableExportService({})
    cfg = service._defaults()

    class Client:
        def __init__(self):
            self.deleted: list[str] = []
            self.created_payloads: list[list[dict]] = []

        def list_records(self, **_kwargs):
            return [
                {
                    "record_id": "old_1",
                    "fields": {
                        "类型": "总负荷（KW）",
                        "楼栋": "A楼",
                        "日期": service._midnight_timestamp_ms("2026-06-09"),
                    },
                }
            ]

        def batch_create_records(self, *, fields_list, **_kwargs):
            self.created_payloads.append(list(fields_list))
            return [{"code": 0}]

        def batch_delete_records(self, *, record_ids, **_kwargs):
            self.deleted.extend(record_ids)
            return len(record_ids)

    client = Client()
    service._new_client = lambda *_args, **_kwargs: client  # type: ignore[method-assign]
    service._prepare_records = lambda **_kwargs: (  # type: ignore[method-assign]
        [{"类型": "总负荷（KW）", "楼栋": "A楼", "日期": service._midnight_timestamp_ms("2026-06-09"), "数值": 1}],
        [],
    )

    result = service._run_with_values(
        cfg=cfg,
        building="A楼",
        duty_date="2026-06-09",
        duty_shift="day",
        cell_values={},
        resolved_values_by_id={},
        metric_origin_context={},
        emit_log=lambda _text: None,
    )

    assert result["status"] == "ok"
    assert len(client.created_payloads) == 1
    assert client.deleted == ["old_1"]


def test_http_source_index_uses_short_cache_and_coalesces_requests():
    service = SharedBridgeRuntimeService(
        runtime_config={
            "deployment": {"role_mode": "external"},
            "shared_bridge": {"enabled": True, "root_dir": r"D:\share"},
            "internal_bridge_http": {"enabled": True, "base_url": "http://internal", "read_timeout_sec": 1},
        },
        app_version="test",
        emit_log=lambda _text: None,
    )

    class Client:
        read_timeout_sec = 1

        def __init__(self):
            self.calls = 0
            self.lock = threading.Lock()

        def source_index_batch(self, queries, *, default_limit=50):
            with self.lock:
                self.calls += 1
            time.sleep(0.05)
            return [
                {
                    "index": index,
                    "ok": True,
                    "entries": [
                        {
                            "entry_id": f"entry-{index}",
                            "source_family": query["source_family"],
                            "building": query["building"],
                            "bucket_kind": "daily",
                            "bucket_key": query["bucket_or_date"],
                            "relative_path": f"支路功率源文件/202606/20260609--整日/{query['building']}.xlsx",
                            "status": "ready",
                            "file_verified": True,
                        }
                    ],
                }
                for index, query in enumerate(queries)
            ]

    client = Client()
    service._internal_bridge_http_client = client  # type: ignore[assignment]

    results: list[list[dict]] = []
    threads = [
        threading.Thread(
            target=lambda: results.append(
                service._http_source_index_entries(
                    source_family="branch_power_family",
                    buildings=["A楼"],
                    bucket_key="2026-06-09",
                )
                or []
            )
        )
        for _ in range(5)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert client.calls == 1
    assert len(results) == 5
    assert all(len(rows) == 1 for rows in results)
    assert service._http_source_index_entries(
        source_family="branch_power_family",
        buildings=["A楼"],
        bucket_key="2026-06-09",
    )
    assert client.calls == 1


def test_http_source_index_accepts_unc_ready_entry_without_request_path_probe():
    service = SharedBridgeRuntimeService(
        runtime_config={
            "deployment": {"role_mode": "external"},
            "shared_bridge": {"enabled": True, "root_dir": r"\\172.16.1.2\share"},
            "internal_bridge_http": {"enabled": True, "base_url": "http://internal", "read_timeout_sec": 1},
        },
        app_version="test",
        emit_log=lambda _text: None,
    )

    class Client:
        read_timeout_sec = 1

        def source_index_batch(self, queries, *, default_limit=50):
            return [
                {
                    "index": 0,
                    "ok": True,
                    "entries": [
                        {
                            "entry_id": "entry-unc",
                            "source_family": "branch_power_family",
                            "building": "A楼",
                            "bucket_kind": "daily",
                            "bucket_key": "2026-06-09",
                            "relative_path": r"支路功率源文件\202606\20260609--整日\A楼.xlsx",
                            "status": "ready",
                        }
                    ],
                }
            ]

    service._internal_bridge_http_client = Client()  # type: ignore[assignment]

    rows = service._http_source_index_entries(
        source_family="branch_power_family",
        buildings=["A楼"],
        bucket_key="2026-06-09",
    )

    assert rows and len(rows) == 1
    assert rows[0]["file_verified"] is True
    assert rows[0]["file_verified_by"] == "external_http_index_unc_no_request_path_probe"


def test_http_source_index_queue_busy_uses_mirror_without_global_cooldown():
    service = SharedBridgeRuntimeService(
        runtime_config={
            "deployment": {"role_mode": "external"},
            "shared_bridge": {"enabled": True, "root_dir": r"\\172.16.1.2\share"},
            "internal_bridge_http": {"enabled": True, "base_url": "http://internal", "read_timeout_sec": 1},
        },
        app_version="test",
        emit_log=lambda _text: None,
    )
    service._http_bridge_retry_after_sec = lambda *_args, **_kwargs: 0  # type: ignore[method-assign]

    class Client:
        read_timeout_sec = 1

        def source_index_batch(self, queries, *, default_limit=50):
            raise RuntimeError("内网端 source-index/batch 正在处理其他请求，请约 60 秒后重试; retry_after_sec=60")

    class Repo:
        def list_bridge_source_index_entries(self, **_kwargs):
            return [
                {
                    "entry_id": "mirror-1",
                    "source_family": "branch_power_family",
                    "building": "A楼",
                    "bucket_kind": "daily",
                    "bucket_key": "2026-06-09",
                    "relative_path": r"支路功率源文件\202606\20260609--整日\A楼.xlsx",
                    "status": "ready",
                }
            ]

    service._internal_bridge_http_client = Client()  # type: ignore[assignment]
    service._app_state_repository = Repo()

    rows = service._http_source_index_entries(
        source_family="branch_power_family",
        buildings=["A楼"],
        bucket_key="2026-06-09",
    )

    assert rows and rows[0]["entry_id"] == "mirror-1"
    assert service._http_bridge_unavailable_until_monotonic == 0.0
