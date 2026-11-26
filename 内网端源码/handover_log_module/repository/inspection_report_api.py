from __future__ import annotations

import math
import re
from datetime import datetime, timedelta

from app.config.inspection_data_config import INSPECTION_REPORT_TEMPLATES
from app.modules.shared_bridge.service.report_api_query import (
    _body_parameters, _encode_parameters, _init_parameters, _point_parameters,
    _report_session_id, _source_parameters,
)
from app.modules.shared_bridge.service.report_api_session import BmsReportApiSession, ReportApiError, ReportApiUnauthorized
from app.modules.shared_bridge.service.report_grid_parser import ReportGridParseError, parse_report_grid


def _usable_reading(value) -> bool:
    text = str(value if value is not None else "").strip().lower()
    if text in {"", "-", "--", "null", "none", "n/a", "无数据", "无权限", "暂无数据", "读取失败",
                "超时", "timeout", "timed out", "permission denied"}:
        return False
    try:
        return math.isfinite(float(text))
    except ValueError:
        return True


def _first_sample(grid: list, start: datetime, end: datetime, *, ids=None, sample_ids=None, latest=False, allow_empty=False) -> tuple[str, list]:
    for header_index, row in enumerate(grid[:40]):
        candidates = []
        for column, value in enumerate(row[4:], 4):
            try:
                stamp = datetime.strptime(value or "", "%Y-%m-%d %H:%M:%S")
            except (TypeError, ValueError):
                continue
            if not start <= stamp <= end:
                if latest:
                    continue
                raise ValueError("巡检接口返回的首个时刻不在本次查询窗口内")
            candidates.append((stamp, column))
            if not latest:
                break
        if not candidates:
            continue
        stamp, column = max(candidates)
        rows = []
        body = grid[header_index + 1:]
        for i, line in enumerate(body, header_index + 1):
            if len(line) <= 3 or not line[3]:
                if ids is not None and (i in ids or str(i) in ids or any(line[4:])):
                    raise ValueError("巡检接口测点行缺少名称")
                continue
            rows.append([*line[1:4], line[column] if len(line) > column else None])
            if sample_ids is not None:
                sample_ids.append((ids or {}).get(i, (ids or {}).get(str(i), "")))
        if not rows and allow_empty and any(str(cell or "").strip() not in ("", "无数据", "暂无数据")
                                            for line in body for cell in line):
            raise ValueError("巡检分组不是有效空表")
        if allow_empty or any(sample[3] not in (None, "") for sample in rows):
            return stamp.strftime("%Y-%m-%d %H:%M:%S"), rows
        raise ValueError("巡检接口未返回有效测点")
    raise ValueError("巡检接口缺少窗口内真实采样时间" if latest else "巡检接口缺少时间表头")


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


def _query_once(client, *, building: str, source_family: str, rules: dict, start: datetime, end: datetime, latest_sample=False) -> dict:
    template = INSPECTION_REPORT_TEMPLATES[source_family]
    start_text, end_text = (stamp.strftime("%Y-%m-%d %H:%M:%S") for stamp in (start, end))
    report = client.find_report(template)
    parameters = _source_parameters(report)
    selected_ids = _select_points(_point_parameters(parameters), rules)
    init = _init_parameters(report, parameters, session_id=client._session_id, query_start=start_text, query_end=end_text)
    groups = [selected_ids]
    hvac = source_family == "hvac_inspection_family"
    if hvac:
        # CMDB IDs end in <spot_type>_<signal>_<source>; type 2 is discrete feedback.
        states, measurements = [], []
        for point in selected_ids:
            parts = point.rsplit("_", 3)
            (states if len(parts) == 4 and parts[1] == "2" else measurements).append(point)
        groups = [group for group in (states, measurements) if group]
    pending = list(reversed(groups))
    sample_ids, rows = [], []
    value_time = None
    while pending:
        ids_to_read = pending.pop()
        try:
            when, ids, samples = _read_point_chunk(
                client, init, parameters, ids_to_read, start, end, latest_sample, len(ids_to_read) != len(selected_ids),
            )
        except ReportGridParseError as exc:
            if not hvac or str(exc) != "HTML body exceeds max_body_bytes" or len(ids_to_read) < 2:
                raise
            half = len(ids_to_read) // 2
            pending.extend((ids_to_read[half:], ids_to_read[:half]))
            continue
        if value_time is not None and when != value_time:
            raise ValueError("巡检分组采样时刻不一致，未合并本批次")
        value_time = when
        sample_ids.extend(ids)
        rows.extend(samples)
    if not any(_usable_reading(sample[3]) for sample in rows):
        raise ValueError("巡检接口未返回有效测点")
    missing_samples = []
    position = device = ""
    for point_id, row in zip(sample_ids, rows):
        place, name, metric, value = row
        if place:
            place_key = re.sub(r"\s+", "", place).replace("（", "(").replace("）", ")")
            if place_key != position:
                position, device = place_key, ""
        if name:
            device = name.strip()
        if value is None or str(value).strip().lower() in {"", "null"}:
            row[3] = "0"
            missing_samples.append({"point_id": point_id, "device": device, "metric": metric.strip(), "count": 1})
    returned = set(sample_ids)
    missing = [point for point in selected_ids if point not in returned]
    # Keep real rows in source order; a missing ID has no source labels to inherit.
    rows.extend(["", "", "", "0"] for _ in missing)
    sample_ids.extend(missing)
    return {
        "schema_version": 1, "building": building, "source_family": source_family,
        "report_name": template, "query_start": start_text, "query_end": end_text,
        "value_time": value_time, "rows": rows, "point_ids": sample_ids,
        "requested_point_ids": selected_ids, "point_overrides": rules,
        **({"zero_filled_point_ids": missing} if missing else {}),
        **({"zero_filled_sample_point_ids": [item["point_id"] for item in missing_samples],
            "missing_sample_points": missing_samples} if missing_samples else {}),
    }


def _read_point_chunk(client, init, parameters, selected_ids, start, end, latest_sample, split):
    response = client.request("GET", "/xbreport/ReportServer", params=init, timeout=(5, 120))
    try:
        report_session = _report_session_id(response.content.decode("gb18030"))
    except UnicodeDecodeError:
        raise ReportApiError("动环报表初始化响应编码无效") from None
    body, _ = _body_parameters(parameters, query_start=init["starttime"], query_end=init["endtime"], scale_label="5分钟", sheet_name="")
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
    value_time, rows = _first_sample(parsed["grid"], start, end, ids=parsed["row_ids"], sample_ids=sample_ids,
                                     latest=latest_sample, allow_empty=split)
    if (not set(sample_ids) <= set(selected_ids) or len(sample_ids) != len(set(sample_ids))
            or len(sample_ids) != len(parsed["row_ids"])):
        missing = sorted(set(selected_ids) - set(sample_ids))
        extra = sorted(set(sample_ids) - set(selected_ids))
        raise ValueError(f"巡检测点返回不完整或不符: 缺少={','.join(missing[:10]) or '-'}; 多出={','.join(extra[:10]) or '-'}")
    return value_time, sample_ids, rows


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
                    return _query_once(client, building=building, source_family=source_family, rules=rules, start=start, end=end,
                                       **({"latest_sample": True} if config.get("inspection_data", {}).get("live_latest_sample") is True else {}))
            except ReportApiUnauthorized:
                if attempt:
                    raise
    raise ReportApiError("动环报表获取失败")  # pragma: no cover
