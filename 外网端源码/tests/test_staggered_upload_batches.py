import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.config.config_adapter import adapt_runtime_config
from app.modules.shared_bridge.service import shared_bridge_runtime_service as bridge


def test_schedules_use_new_offsets_without_moving_business_times():
    raw = json.loads((Path(__file__).resolve().parents[1] / "表格计算配置.json").read_text(encoding="utf-8-sig"))
    cfg = adapt_runtime_config(raw)
    assert cfg["scheduler"]["run_time"] == "02:30:00"
    assert cfg["temperature_humidity_upload"]["scheduler"]["run_time"] == "01:10:00"
    assert cfg["day_metric_upload"]["scheduler"]["minute_offset"] == 15
    assert cfg["chiller_mode_upload"]["scheduler"]["minute_offset"] == 7
    assert cfg["wet_bulb_collection"]["scheduler"]["minute_offset"] == 35
    assert cfg["branch_power_upload"]["scheduler"]["minute_offset"] == 240
    assert cfg["alarm_export"]["scheduler"]["run_time"] == "08:25:00"
    assert cfg["system_screenshot_upload"]["scheduler"]["run_time"] == "06:37:00"
    assert cfg["inspection_data_upload"]["scheduler"]["run_times"] == [
        f"{hour:02d}:20:00" for hour in range(0, 24, 2)
    ]


def test_scheduled_alarm_requires_exact_daily_bucket_and_keeps_it_on_resume():
    bucket = "2026-11-13 08"
    lookup = Mock(return_value=[
        {"building": "A", "bucket_key": bucket, "bucket_kind": "latest"},
        {"building": "B", "bucket_key": "2026-11-12 08", "bucket_kind": "latest"},
        {"building": "B", "bucket_key": bucket, "bucket_kind": "manual"},
    ])
    runtime = SimpleNamespace(
        _http_source_cache_buildings=lambda _: ["A", "B"],
        get_source_cache_buildings=lambda: ["A", "B"],
        get_latest_source_cache_entries=lookup,
    )
    runtime.get_alarm_event_upload_selection = (
        lambda **kwargs: bridge.SharedBridgeRuntimeService.get_alarm_event_upload_selection(runtime, **kwargs)
    )
    task = {"requested_by": "alarm_upload_scheduler", "request": {"target_bucket_key": bucket}}
    bind = bridge.SharedBridgeRuntimeService._build_alarm_event_upload_resume_binding
    with pytest.raises(RuntimeError, match="B"):
        bind(runtime, task)
    assert lookup.call_args.kwargs["bucket_key"] == bucket
    lookup.return_value.append({"building": "B", "bucket_key": bucket, "bucket_kind": "latest"})
    assert bind(runtime, task)["worker_payload"]["required_alarm_bucket"] == bucket


def test_scheduled_day_metric_cannot_resume_with_only_one_building():
    runtime = SimpleNamespace(
        _require_accessible_cached_file=lambda path, **_: path,
        _normalize_text_list=lambda values: list(values),
        get_source_cache_buildings=lambda: ["A", "B"],
    )
    task = {
        "requested_by": "scheduler",
        "request": {"selected_dates": ["2026-11-13"]},
        "result": {"internal": {"source_units": [
            {"building": "A", "duty_date": "2026-11-13", "file_path": "a.json"},
        ]}},
    }
    with pytest.raises(RuntimeError, match="未齐全"):
        bridge.SharedBridgeRuntimeService._build_day_metric_resume_binding_from_artifacts(runtime, task)


def test_old_alarm_wait_does_not_block_new_daily_batch(monkeypatch):
    monkeypatch.setattr(bridge, "daily_scheduled_bucket_for_time", lambda: "2026-11-14 08")
    jobs = SimpleNamespace(
        list_jobs=lambda **_: [
            {"job_id": "old", "feature": "alarm_event_upload", "wait_reason": "waiting:shared_bridge"},
        ],
        load_job_stage_payload=lambda _: {"required_alarm_bucket": "2026-11-13 08"},
        cancel_job=Mock(),
    )
    runtime = SimpleNamespace(_job_service=jobs, _emit_system_log=Mock(), _reissue_unbound_waiting_job=Mock())
    bridge.SharedBridgeRuntimeService._reconcile_waiting_jobs(runtime)
    jobs.cancel_job.assert_called_once_with("old")
    runtime._reissue_unbound_waiting_job.assert_not_called()
