import json
import re
from datetime import datetime
from types import SimpleNamespace

import pytest

from app.config.inspection_data_config import INSPECTION_REPORT_TEMPLATES
from app.modules.shared_bridge.service.report_api_session import ReportApiUnauthorized
from handover_log_module.repository import inspection_report_api as api
from handover_log_module.service.handover_download_service import HandoverDownloadService


FAMILY = next(iter(INSPECTION_REPORT_TEMPLATES))
START = datetime(2026, 11, 4, 9, 40)
END = datetime(2026, 11, 4, 10)


def table(ids=("1_2", "1_3"), stamp="2026-11-04 09:45:00"):
    header = f"<tr><td></td><td></td><td></td><td></td><td>{stamp}</td><td>2026-11-04 09:50:00</td></tr>"
    rows = "".join(
        f'<tr><td></td><td>位置</td><td>设备</td><td>测点{i}</td><td hidden id="E1-{i}">{point}</td><td>{i}</td><td>99</td></tr>'
        for i, point in enumerate(ids)
    )
    return f'<table class="x-table">{header}{rows}</table>'.encode()


class Client:
    _session_id = "session"

    def __init__(self, html, family=FAMILY):
        self.html = html
        self.family = family
        self.calls = []
        self.closed = False

    def find_report(self, name):
        assert name == INSPECTION_REPORT_TEMPLATES[self.family]
        return {"tpl_name": "test.cpt", "plan_id": 1, "dcstruc_id": 2,
                "templateParam": json.dumps([{"key": "测点列表", "type": "normalTree", "value": "1_2,1_3;parents;meta"}])}

    def request(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        op = kwargs["params"]["op"]
        if op == "write":
            return SimpleNamespace(content=b"FR.SessionMgr.register('abc', contentPane)")
        if op == "fr_dialog":
            return SimpleNamespace()
        assert op == "fr_write"
        return SimpleNamespace(headers={"content-type": "text/html; charset=utf-8"},
                               iter_content=lambda **_: iter([self.html]), close=self.close)

    def close(self):
        self.closed = True


def query(client, rules=None):
    return api._query_once(client, building="D楼", source_family=FAMILY, rules=rules or {}, start=START, end=END)


@pytest.mark.parametrize("building", ["D楼", "E楼"])
@pytest.mark.parametrize("family", list(INSPECTION_REPORT_TEMPLATES))
def test_direct_protocol_default_points_and_first_sample(building, family):
    client = Client(table(), family)
    result = api._query_once(client, building=building, source_family=family, rules={}, start=START, end=END)
    assert result == {"schema_version": 1, "building": building, "source_family": family,
                      "report_name": INSPECTION_REPORT_TEMPLATES[family],
                      "query_start": "2026-11-04 09:40:00", "query_end": "2026-11-04 10:00:00",
                      "value_time": "2026-11-04 09:45:00", "rows": [["位置", "设备", "测点0", "0"], ["位置", "设备", "测点1", "1"]],
                      "point_ids": ["1_2", "1_3"], "requested_point_ids": ["1_2", "1_3"], "point_overrides": {}}
    assert client.closed
    assert [(method, args["params"]["op"]) for method, _, args in client.calls] == [("GET", "write"), ("POST", "fr_dialog"), ("GET", "fr_write")]
    encoded = client.calls[1][2]["data"]["__parameters__"]
    body = json.loads(re.sub(r"\[([0-9a-fA-F]{2,6})\]", lambda match: chr(int(match.group(1), 16)), encoded))
    assert body["查询刻度"] == "five" and body["测点列表"] == ["1_2", "1_3"]


@pytest.mark.parametrize("rules,expected", [
    ({"add": ["1_4"], "remove": ["1_2"]}, ["1_3", "1_4"]),
    ({"baseline": ["1_5", "1_2"], "add": ["1_5", "1_4"], "remove": ["1_2"]}, ["1_5", "1_4"]),
])
def test_override_rules_are_submitted_and_preserved(rules, expected):
    client = Client(table(expected))
    result = query(client, rules)
    assert result["requested_point_ids"] == expected and result["point_overrides"] == rules
    encoded = client.calls[1][2]["data"]["__parameters__"]
    body = json.loads(re.sub(r"\[([0-9a-fA-F]{2,6})\]", lambda match: chr(int(match.group(1), 16)), encoded))
    assert body["测点列表"] == expected


@pytest.mark.parametrize("ids", [("1_2",), ("1_2", "1_4"), ("1_2", "1_2"), ("1_2", "")])
def test_rejects_missing_extra_duplicate_or_absent_returned_ids(ids):
    client = Client(table(ids))
    with pytest.raises(ValueError, match="不完整或不符"):
        query(client)
    assert client.closed


def test_rejects_out_of_window_and_invalid_overrides():
    with pytest.raises(ValueError, match="查询窗口"):
        query(Client(table(stamp="2026-11-04 09:35:00")))
    for rules in ({"baseline": []}, {"remove": ["1_2", "1_3"]}, {"add": ["bad"]}):
        with pytest.raises(ValueError):
            query(Client(table()), rules)


def test_first_sample_does_not_fall_back_when_entire_first_column_is_blank():
    html = table().replace(b">0</td>", b"></td>").replace(b">1</td>", b"></td>")
    with pytest.raises(ValueError, match="未返回有效测点"):
        query(Client(html))


def test_session_retries_unauthorized_with_same_query_window(monkeypatch):
    sessions = []
    class Session:
        def __init__(self, site):
            sessions.append(self)
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            self.closed = True
        def login(self):
            if len(sessions) == 1:
                raise ReportApiUnauthorized("expired")
    monkeypatch.setattr(api, "BmsReportApiSession", Session)
    monkeypatch.setattr(api, "_query_once", lambda client, **kwargs: kwargs)
    result = api.query_inspection_report({}, building="D楼", source_family=FAMILY, config={})
    assert len(sessions) == 2 and all(session.closed for session in sessions)
    assert (result["end"] - result["start"]).total_seconds() == 1200


def test_service_guards_do_not_call_direct_api(monkeypatch):
    monkeypatch.setattr("handover_log_module.service.handover_download_service.query_inspection_report",
                        lambda *args, **kwargs: pytest.fail("unexpected API call"))
    for config, building, family in [
        ({}, "D楼", FAMILY), ({"inspection_data": {"enabled": True}}, "A楼", FAMILY),
        ({"inspection_data": {"enabled": True}}, "D楼", "unknown"),
        ({"inspection_data": {"enabled": True}, "sites": [{"building": "D楼", "enabled": False}]}, "D楼", FAMILY),
    ]:
        with pytest.raises(ValueError):
            HandoverDownloadService(config).run_inspection_data_only(building=building, source_family=family)
