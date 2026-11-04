from __future__ import annotations

import re
from datetime import datetime, timedelta

from app.config.inspection_data_config import INSPECTION_REPORT_TEMPLATES
from app.modules.shared_bridge.service.report_api_query import (
    _body_parameters, _encode_parameters, _init_parameters, _point_parameters,
    _report_session_id, _source_parameters,
)
from app.modules.shared_bridge.service.report_api_session import BmsReportApiSession, ReportApiError, ReportApiUnauthorized
from app.modules.shared_bridge.service.report_grid_parser import parse_report_grid


def _first_sample(grid: list, start: datetime, end: datetime, *, ids=None, sample_ids=None) -> tuple[str, list]:
    for header_index, row in enumerate(grid[:40]):
        for column, value in enumerate(row[4:], 4):
            try:
                stamp = datetime.strptime(value or "", "%Y-%m-%d %H:%M:%S")
            except (TypeError, ValueError):
                continue
            if not start <= stamp <= end:
                raise ValueError("巡检接口返回的首个时刻不在本次查询窗口内")
            rows = []
            for i, line in enumerate(grid[header_index + 1:], header_index + 1):
                if len(line) <= 3 or not line[3]:
                    continue
                if len(line) <= column:
                    raise ValueError("巡检接口测点行缺少首时刻数据列")
                rows.append([*line[1:4], line[column]])
                if sample_ids is not None:
                    sample_ids.append((ids or {}).get(i, (ids or {}).get(str(i), "")))
            if rows and any(sample[3] not in (None, "") for sample in rows):
                return stamp.strftime("%Y-%m-%d %H:%M:%S"), rows
            raise ValueError("巡检接口未返回有效测点")
    raise ValueError("巡检接口缺少时间表头")


def _select_points(current: list[str], rules: dict) -> list[str]:
    if not isinstance(rules, dict) or set(rules) - {"baseline", "add", "remove"}:
        raise ValueError("巡检测点配置无效")
    for ids in rules.values():
        if not isinstance(ids, list) or any(
            not isinstance(point, str) or not re.fullmatch(r"[0-9]+(?:_[0-9]+)+", point)
            for point in ids
        ):
            raise ValueError("巡检测点配置无效")
    baseline = rules.get("baseline", current)
    removed = set(rules.get("remove", []))
    selected = [point for point in dict.fromkeys([*baseline, *rules.get("add", [])]) if point not in removed]
    if not 1 <= len(selected) <= 2000:
        raise ValueError("巡检查询需要选择 1–2000 个测点")
    return selected


def _query_once(client, *, building: str, source_family: str, rules: dict, start: datetime, end: datetime) -> dict:
    template = INSPECTION_REPORT_TEMPLATES[source_family]
    start_text, end_text = (stamp.strftime("%Y-%m-%d %H:%M:%S") for stamp in (start, end))
    report = client.find_report(template)
    parameters = _source_parameters(report)
    selected_ids = _select_points(_point_parameters(parameters), rules)
    init = _init_parameters(report, parameters, session_id=client._session_id, query_start=start_text, query_end=end_text)
    response = client.request("GET", "/xbreport/ReportServer", params=init, timeout=(5, 120))
    try:
        report_session = _report_session_id(response.content.decode("gb18030"))
    except UnicodeDecodeError:
        raise ReportApiError("动环报表初始化响应编码无效") from None
    body, _ = _body_parameters(parameters, query_start=start_text, query_end=end_text, scale_label="5分钟", sheet_name="")
    body["测点列表"] = selected_ids
    client.request(
        "POST", "/xbreport/ReportServer",
        params={"op": "fr_dialog", "cmd": "parameters_d", "sessionID": report_session},
        data={"__parameters__": _encode_parameters(body)}, timeout=(5, 180),
    )
    response = client.request(
        "GET", "/xbreport/ReportServer",
        params={"op": "fr_write", "cmd": "read_w_content", "sessionID": report_session, "reportIndex": 0, "pn": 0},
        stream=True, timeout=(5, 180),
    )
    try:
        content_type = str(response.headers.get("content-type", "")).lower()
        encoding = "gb18030" if any(value in content_type for value in ("gbk", "gb2312", "gb18030")) else "utf-8"
        parsed = parse_report_grid(response.iter_content(chunk_size=65536), encoding=encoding)
    finally:
        response.close()
    sample_ids: list[str] = []
    value_time, rows = _first_sample(parsed["grid"], start, end, ids=parsed["row_ids"], sample_ids=sample_ids)
    if set(sample_ids) != set(selected_ids) or "" in sample_ids or len(sample_ids) != len(set(sample_ids)):
        missing = sorted(set(selected_ids) - set(sample_ids))
        extra = sorted(set(sample_ids) - set(selected_ids))
        raise ValueError(f"巡检测点返回不完整或不符: 缺少={','.join(missing[:10]) or '-'}; 多出={','.join(extra[:10]) or '-'}")
    return {
        "schema_version": 1, "building": building, "source_family": source_family,
        "report_name": template, "query_start": start_text, "query_end": end_text,
        "value_time": value_time, "rows": rows, "point_ids": sample_ids,
        "requested_point_ids": selected_ids, "point_overrides": rules,
    }


def query_inspection_report(site: dict, *, building: str, source_family: str, config: dict) -> dict:
    from app.modules.shared_bridge.service.api_request_capacity import api_request_slot

    rules = config.get("inspection_data", {}).get("point_overrides", {}).get(building, {}).get(source_family, {})
    with api_request_slot():
        end = datetime.now().replace(microsecond=0)
        start = end - timedelta(minutes=20)
        for attempt in range(2):
            try:
                with BmsReportApiSession(site) as client:
                    client.login()
                    return _query_once(client, building=building, source_family=source_family, rules=rules, start=start, end=end)
            except ReportApiUnauthorized:
                if attempt:
                    raise
    raise ReportApiError("动环报表获取失败")  # pragma: no cover
