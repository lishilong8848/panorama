from datetime import datetime
from unittest.mock import Mock

import pytest
import requests

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient, FeishuBitableDataNotReady
from app.modules.report_pipeline.service.job_service import JobService
from handover_log_module.repository.event_sections_repository import EventSectionQueryResult
from handover_log_module.service.event_category_payload_builder import EventCategoryPayloadBuilder
from handover_log_module.service.hvac_bitable_sync_service import _HvacFeishuAppClient


@pytest.mark.parametrize("status", [200, 400])
def test_bitable_exhausted_retry_reports_data_not_ready(status):
    client = object.__new__(FeishuBitableClient)
    response = Mock(status_code=status, url="https://test.invalid/records")
    response.json.return_value = {"code": 1254607, "msg": "Data not ready"}
    if status == 400:
        response.raise_for_status.side_effect = requests.HTTPError("test")
    client._api_retry_attempts = lambda: 2
    client._api_retry_sleep = Mock()
    client.refresh_token = Mock(return_value="mock")
    client._request_with_retry = Mock(return_value=response)
    with pytest.raises(FeishuBitableDataNotReady, match="1254607"):
        client._get_json(response.url)
    assert client._request_with_retry.call_count == 2


@pytest.mark.parametrize("view_error,expected_views", [
    (FeishuBitableDataNotReady("1254607"), ["view"]),
    (RuntimeError("view not found"), ["view", ""]),
])
def test_hvac_does_not_expand_request_for_data_not_ready(view_error, expected_views):
    reader = object.__new__(_HvacFeishuAppClient)
    reader._client = Mock()
    reader._client.list_records.side_effect = [view_error, [{"record_id": "record"}]]
    reader._existing_field_names = lambda _: {"字段"}
    reader.page_size = 500
    reader.emit_log = Mock()
    if isinstance(view_error, FeishuBitableDataNotReady):
        with pytest.raises(FeishuBitableDataNotReady):
            reader.record_list("table", "view", ["字段"])
    else:
        assert reader.record_list("table", "view", ["字段"]) == [{"record_id": "record"}]
    assert [call.kwargs["view_id"] for call in reader._client.list_records.call_args_list] == expected_views


def test_event_data_not_ready_keeps_all_cached_records_without_more_lookups(tmp_path):
    repo = Mock()
    repo.get_record_by_id.side_effect = FeishuBitableDataNotReady("1254607")
    cache = Mock()
    cache.list_pending_for_building.return_value = [
        {"record_id": key, "description": key, "event_time": "2026-11-08 16:00:00", "progress_text": "未完成"}
        for key in ("record-a", "record-b")
    ]
    builder = EventCategoryPayloadBuilder({"template": {"source_path": str(tmp_path / "missing.xlsx")}}, repository=repo, cache_store=cache)
    prefetched = EventSectionQueryResult(
        current_rows=[], outside_shift_ongoing_rows=[], historical_open_rows=[],
        shift_start=datetime(2026, 11, 9, 9), shift_end=datetime(2026, 11, 9, 18),
        cfg={"enabled": True, "cache": {"enabled": True}, "sections": {"history_followup": "历史事件跟进"}},
    )
    result = builder.build(building="A楼", duty_date="2026-11-09", duty_shift="day", follower_text="",
                           is_current_duty_context=True, preloaded_query_result_by_building={"A楼": prefetched},
                           emit_log=Mock())
    assert len(result["历史事件跟进"]) == 2
    repo.get_record_by_id.assert_called_once_with(record_id="record-a")
    assert {row["record_id"] for row in cache.update_building_pending.call_args.kwargs["pending_rows"]} == {"record-a", "record-b"}


def test_worker_failure_summary_reports_exit_and_ignores_openpyxl_style_warning():
    warning = "Workbook contains no default style, apply openpyxl's default"
    stderr = [f"openpyxl/styles/stylesheet.py:237: UserWarning: {warning}", f'warn("{warning}")']
    assert JobService._worker_failure_without_result(137, stderr) == "worker 无明确错误结果（退出码 137）"
    stderr.append("MemoryError: test exception")
    detail = JobService._worker_failure_without_result(1, stderr)
    assert "退出码 1" in detail and "MemoryError" in detail and warning not in detail
