from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest

from app.modules.shared_bridge.service.report_api_query import (
    _encode_parameters,
    _point_parameters,
    _report_session_id,
    _validate_daily_hour_columns,
)
from app.modules.shared_bridge.service.report_api_session import ReportApiError
from app.modules.shared_bridge.service import report_api_query
from app.shared.utils.report_source_json import load_report_source_json


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
        return [[], ["2026-11-04 08:00:00"], [], [1]], {}, None, [], []
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


START = "2026-11-04 08:00:00"
END = "2026-11-04 08:05:00"
UPS_ID = "25_0_220_1_601_0"
BRANCH_SHEETS = {"branch_power_family": "支路功率", "branch_current_family": "支路电流", "branch_switch_family": "开关"}


def _offline_query(monkeypatch, *, family, points=True, sample="0", query_start=START, query_end=END):
    branch = family in BRANCH_SHEETS
    labels = ["房间", "机列", "支路"] if branch else ["", "C楼", "实际设备", "实际测点"]
    header = [""] * len(labels) + [START, END, "平均值"]
    live = labels + [sample, "--", "17"]
    def tr(values, hidden=""):
        return "<tr>" + hidden + "".join(f"<td>{v}</td>" for v in values) + "</tr>"
    html = '<table class="x-table">' + tr(header) + tr(live, '<td hidden id="E2-0">1_2</td>') + "</table>"
    content = Mock(headers={})
    content.iter_content.return_value = [html.encode()]
    session = Mock(_session_id="offline")
    session.find_report.return_value = {
        "tpl_name": "offline.cpt", "dcstruc_id": "1", "plan_id": 1,
        "templateParam": [{"key": "测点列表", "value": f"{UPS_ID},1_2,9_9;parents;meta"}] if points else [],
    }
    session.request.side_effect = [
        Mock(content=b"FR.SessionMgr.register('offline', contentPane)"), Mock(),
        TimeoutError("offline timeout") if sample == "timeout" else content,
    ]
    monkeypatch.setattr(report_api_query, "BmsReportApiSession", lambda site: nullcontext(session))
    payload = report_api_query.query_report_json(
        {"building": "C楼"}, source_family=family, report_name="测试", scale_label="5分钟",
        sheet_name=BRANCH_SHEETS.get(family, ""), business_date="2026-11-04",
        query_start=query_start, query_end=query_end,
    )
    posted = session.request.call_args_list[1].kwargs["data"]["__parameters__"]
    expected = {"开始时间": query_start, "结束时间": query_end, "查询刻度": "five"}
    if points:
        expected.update({"测点列表": [UPS_ID, "1_2", "9_9"], "排列方式": "B", "报表名称": ""})
    else:
        expected["配置文件SHEET名"] = BRANCH_SHEETS[family]
    assert posted == _encode_parameters(expected)
    content.close.assert_called_once()
    return payload, live


@pytest.mark.parametrize("family", sorted(report_api_query._SOURCE_FAMILIES.difference(BRANCH_SHEETS)))
def test_only_requested_missing_ids_are_zero_filled_in_delivered_json(monkeypatch, tmp_path, family):
    payload, live = _offline_query(monkeypatch, family=family)
    path = tmp_path / "report.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    payload = load_report_source_json(path, expected_family=family)
    assert payload["requested_point_count"] == payload["returned_point_count"] == 3
    assert payload["api_returned_point_count"] == 1
    assert payload["zero_filled_point_ids"] == [UPS_ID, "9_9"]
    sheets = payload["sheets"]
    reading = None if family in {"monthly_report_family", "air_conditioner_temperature_humidity_family"} else 0
    assert sheets[0]["rows"][3] == [*live[:-2], reading, live[-1]]
    assert payload["missing_sample_points"][0]["count"] == 1
    assert payload["missing_sample_points"][0]["point_id"] == "1_2"
    assert len(sheets) == 1
    filled = sheets[0]["rows"][4:]
    assert len(filled) == 2 and all(row[-3:] == [0, 0, None] for row in filled)
    if family == "handover_capacity_report_family":
        assert filled[0][2:4] == ["C-218-UPS-101", "电池_正极电压_V"]
    else:
        assert [row[0] for row in filled] == [UPS_ID, "9_9"]
        assert all(all(v is None for v in row[1:-3]) for row in filled)


@pytest.mark.parametrize("family", BRANCH_SHEETS)
def test_branch_sheet_keeps_exact_business_rows_and_explicit_ids_cannot_fake_a_join(monkeypatch, family):
    payload, live = _offline_query(monkeypatch, family=family, points=False)
    assert payload["requested_point_count"] is None and payload["returned_point_count"] == 1
    assert "zero_filled_point_ids" not in payload and "api_returned_point_count" not in payload
    assert len(payload["sheets"]) == 1 and payload["sheets"][0]["rows"][3:] == [[*live[:-2], 0, live[-1]]]
    assert payload["missing_sample_points"][0]["zero_filled"] is True
    with pytest.raises(ValueError, match="支路报表缺少测点"):
        _offline_query(monkeypatch, family=family)


def test_partial_points_cannot_mask_timeout_or_missing_daily_hours(monkeypatch):
    with pytest.raises(TimeoutError, match="offline timeout"):
        _offline_query(monkeypatch, family="handover_log_family", sample="timeout")
    with pytest.raises(ReportApiError, match="缺少业务日小时列"):
        _offline_query(monkeypatch, family="monthly_report_family",
                       query_start="2026-11-04 00:00:00", query_end="2026-11-05 00:00:00")
