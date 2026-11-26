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


@pytest.mark.parametrize("family", list(INSPECTION_REPORT_TEMPLATES))
@pytest.mark.parametrize("latest", [False, True])
def test_partial_response_preserves_real_reading_and_appends_unlabelled_missing_id(family, latest):
    client = Client(table(("1_3",)), family)
    result = api._query_once(client, building="D楼", source_family=family, rules={}, start=START, end=END,
                             latest_sample=latest)
    assert result["point_ids"] == ["1_3", "1_2"]
    assert result["zero_filled_point_ids"] == ["1_2"]
    assert result["rows"] == [["位置", "设备", "测点0", "99" if latest else "0"], ["", "", "", "0"]]


@pytest.mark.parametrize("cells", [b"<td></td><td>99</td>", b"<td>  </td><td>99</td>", b"<td>null</td>", b""])
def test_missing_sample_is_filled_separately_without_using_later_columns(cells):
    client = Client(table().replace(b"<td>0</td><td>99</td>", cells).replace(b">1</td>", b">0</td>"))
    result = query(client, {"add": ["1_4"]})
    assert result["point_ids"] == ["1_2", "1_3", "1_4"]
    assert result["rows"] == [["位置", "设备", "测点0", "0"], ["位置", "设备", "测点1", "0"], ["", "", "", "0"]]
    assert result["zero_filled_point_ids"] == ["1_4"]
    assert result["zero_filled_sample_point_ids"] == ["1_2"]
    assert result["missing_sample_points"] == [{"point_id": "1_2", "device": "设备", "metric": "测点0", "count": 1}]


@pytest.mark.parametrize("literal", ["--", "timed out", "NaN", "停机", "未运行"])
def test_nonmissing_literals_and_returned_states_are_not_padded(literal):
    result = query(Client(table().replace(b">0</td>", f">{literal}</td>".encode())))
    assert result["rows"][0][3] == literal
    assert "zero_filled_sample_point_ids" not in result
    assert "missing_sample_points" not in result


@pytest.mark.parametrize("ids", [("1_2", "1_4"), ("1_2", "1_2"), ("1_2", "")])
def test_rejects_extra_duplicate_or_absent_returned_ids(ids):
    client = Client(table(ids))
    with pytest.raises(ValueError, match="不完整或不符"):
        query(client)
    assert client.closed


@pytest.mark.parametrize("html", [
    table(()), table(("1_3",)).replace(b">0</td>", b">--</td>"),
    table(("1_3",)).replace(b">0</td>", b">timed out</td>"),
    table(("1_2", "1_3")).replace(b">0</td>", b">Infinity</td>").replace(b">1</td>", b">NaN</td>"),
    table(("1_3",)).replace("测点0".encode(), b""),
    b"<html>permission denied</html>", b'<table class="x-table"><tr>',
])
def test_partial_without_valid_sample_or_structure_never_fills(html):
    with pytest.raises(ValueError):
        query(Client(html))


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


def test_hvac_queries_state_and_other_points_separately_in_the_same_window():
    ids = ["55_0_444_1_602_0", "55_0_444_2_600_0", "55_0_444_1_600_0"]
    class GroupsClient(Client):
        def request(self, method, path, **kwargs):
            if kwargs["params"]["op"] == "fr_dialog":
                decoded = re.sub(r"\[([0-9a-fA-F]{2,6})\]", lambda m: chr(int(m[1], 16)), kwargs["data"]["__parameters__"])
                self.html = table(json.loads(decoded)["测点列表"])
            return super().request(method, path, **kwargs)
    c = GroupsClient(b"", "hvac_inspection_family")
    result = api._query_once(c, building="E楼", source_family=c.family, rules={"baseline":ids}, start=START, end=END)
    params = [args["params"] for _,_,args in c.calls if args["params"]["op"] == "write"]
    assert len(params) == 2 and params[0]["starttime"] == params[1]["starttime"] and params[0]["endtime"] == params[1]["endtime"]
    assert result["requested_point_ids"] == ids and result["point_ids"] == [ids[1],ids[0],ids[2]]
    assert len(result["rows"]) == 3


@pytest.mark.parametrize("latest", [False, True])
@pytest.mark.parametrize("empty_group", [0, 1])
def test_hvac_header_only_group_does_not_discard_sibling_or_use_other_time(latest, empty_group):
    family = "hvac_inspection_family"
    ids = ["5_0_1_2_600_0", "5_0_1_1_600_0"]
    class GroupClient(Client):
        def request(self, method, path, **kwargs):
            if kwargs["params"]["op"] == "fr_dialog":
                self.html = next(self.pages)
            return super().request(method, path, **kwargs)

    def read(pages):
        client = GroupClient(b"", family)
        client.pages = iter(pages)
        return api._query_once(client, building="D楼", source_family=family, rules={"baseline": ids},
                               start=START, end=END, latest_sample=latest)

    pages = [table((i,)) for i in ids]
    pages[empty_group] = table(())
    result = read(pages)
    assert result["point_ids"] == [ids[1 - empty_group], ids[empty_group]]
    assert result["zero_filled_point_ids"] == [ids[empty_group]]
    with pytest.raises(ValueError, match="未返回有效测点"):
        read([table(()), table(())])
    pages[empty_group] = table(()).replace(b"09:45", b"09:44").replace(b"09:50", b"09:49")
    with pytest.raises(ValueError, match="时刻不一致"):
        read(pages)
    pages[empty_group] = b"<html>permission denied</html>"
    with pytest.raises(api.ReportGridParseError):
        read(pages)
    pages[empty_group] = table(()).replace(b"</table>", b"<tr><td>permission denied</td></tr></table>")
    with pytest.raises(ValueError, match="有效空表"):
        read(pages)


@pytest.mark.parametrize("mismatch", [False, True])
def test_oversized_hvac_group_splits_only_itself_and_never_combines_different_samples(monkeypatch, mismatch):
    calls = []
    def read(client, init, params, points, start, end, latest, split):
        calls.append(points)
        if len(points) > 1:
            raise api.ReportGridParseError("HTML body exceeds max_body_bytes")
        return ("09:50" if mismatch and len(calls) == 4 else "09:45"), points, [["位置","设备","信号","0"]]
    monkeypatch.setattr(api, "_read_point_chunk", read)
    ids = ["5_0_1_2_600_0","5_0_1_1_600_0","5_0_1_1_601_0"]
    opts = dict(building="D楼", source_family="hvac_inspection_family", rules={"baseline":ids}, start=START, end=END)
    c = Client(b"",opts["source_family"])
    if mismatch:
        with pytest.raises(ValueError,match="时刻不一致"):
            api._query_once(c, **opts)
    else:
        assert api._query_once(c, **opts)["point_ids"] == ids
    assert calls == [ids[:1], ids[1:], ids[1:2], ids[2:]]
