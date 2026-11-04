from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import pytest

from app.modules.shared_bridge.service.report_api_query import (
    _encode_parameters,
    _point_parameters,
    _report_session_id,
    _validate_daily_hour_columns,
)
from app.modules.shared_bridge.service.report_api_session import ReportApiError
from app.modules.shared_bridge.service import report_api_query


def test_report_session_id_requires_one_explicit_content_pane_registration():
    assert _report_session_id("FR.SessionMgr.register('12345', contentPane);") == "12345"
    with pytest.raises(ReportApiError):
        _report_session_id("<html>login</html>")
    with pytest.raises(ReportApiError):
        _report_session_id("FR.SessionMgr.register('1', contentPane); FR.SessionMgr.register('2', contentPane);")


def test_selected_points_use_only_first_tree_segment():
    params = [{"key": "测点列表", "type": "normalTree", "value": "0_1_2,0_1_3;parent1,parent2;meta;normalTree"}]
    assert _point_parameters(params) == ["0_1_2", "0_1_3"]
    with pytest.raises(ReportApiError):
        _point_parameters([{"key": "测点列表", "value": "0_1_2,0_1_2;meta"}])
    with pytest.raises(ReportApiError):
        _point_parameters([{"key": "测点列表", "value": "0_1_2,bad;meta"}])


def test_fr_parameter_encoding_round_trips_chinese_and_arrays():
    import re

    payload = {"开始时间": "2026-11-04 09:00:00", "测点列表": ["0_1_2"], "查询刻度": "five"}
    encoded = _encode_parameters(payload)
    assert "[5b]" in encoded and "[5d]" in encoded
    decoded = re.sub(r"\[([0-9a-fA-F]{2,6})\]", lambda m: chr(int(m.group(1), 16)), encoded)
    assert json.loads(decoded) == payload


@pytest.mark.parametrize("family", ["monthly_report_family", "branch_power_family", "branch_current_family", "branch_switch_family", "building_full_cabinet_power_family"])
def test_daily_query_rejects_missing_business_hour(family):
    day = datetime(2026, 11, 3)
    hours = [(day + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S") for i in range(25)]
    hours.pop(15)
    with pytest.raises(ReportApiError, match="2026-11-03 15:00"):
        _validate_daily_hour_columns([[], hours], family=family, business_date="2026-11-03",
                                     start=day-timedelta(minutes=10), end=day+timedelta(days=1, hours=1))


def test_api_queries_for_same_building_start_concurrently(monkeypatch):
    barrier = threading.Barrier(4, timeout=3)
    class Session:
        def __init__(self, site):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *_):
            pass
        def login(self):
            pass
    def query(*args, **kwargs):
        barrier.wait()
        return [[], ["2026-11-04 08:00:00"], [], [1]], {}, None
    monkeypatch.setattr(report_api_query, "BmsReportApiSession", Session)
    monkeypatch.setattr(report_api_query, "_query_once", query)
    def run(_):
        return report_api_query.query_report_json(
            {"building": "A楼", "host": "local"}, source_family="handover_log_family",
            report_name="test", query_start="2026-11-04 08:00:00",
            query_end="2026-11-04 08:05:00", scale_label="5分钟",
        )
    with ThreadPoolExecutor(max_workers=4) as executor:
        assert len(list(executor.map(run, range(4)))) == 4
