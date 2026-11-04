from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any

from app.modules.shared_bridge.service.report_api_session import (
    BmsReportApiSession,
    ReportApiError,
    ReportApiUnauthorized,
)
from app.modules.shared_bridge.service.report_api_transform import normalize_report_rows
from app.modules.shared_bridge.service.report_grid_parser import parse_report_grid


_SOURCE_FAMILIES = frozenset({
    "handover_log_family", "handover_capacity_report_family", "monthly_report_family",
    "branch_power_family", "branch_current_family", "branch_switch_family",
    "building_full_cabinet_power_family", "air_conditioner_temperature_humidity_family",
    "chiller_mode_switch_family",
})
_POINT_ID = re.compile(r"^\d+(?:_\d+)+$")
_SESSION_REGISTRATION = re.compile(
    r"FR\.SessionMgr\.register\(\s*['\"]([A-Za-z0-9_-]{1,128})['\"]\s*,\s*contentPane\s*\)"
)
_SCALE_CODES = {"5分钟": "five", "小时": "hour"}
_SCALE_SECONDS = {"5分钟": 300, "小时": 3600}
def _report_session_id(html: str) -> str:
    matches = set(_SESSION_REGISTRATION.findall(html))
    if len(matches) != 1:
        raise ReportApiError("动环报表会话未就绪或响应异常")
    return next(iter(matches))


def _point_parameters(parameters: list[dict[str, Any]]) -> list[str]:
    items = [item for item in parameters if item.get("key") == "测点列表"]
    if len(items) != 1 or not isinstance(items[0].get("value"), str):
        raise ReportApiError("动环报表测点配置无效")
    first_segment = items[0]["value"].split(";", 1)[0]
    points = [part.strip() for part in first_segment.split(",") if part.strip()]
    if not points or len(points) != len(set(points)) or any(not _POINT_ID.fullmatch(point) for point in points):
        raise ReportApiError("动环报表测点配置无效")
    return points


def _encode_parameters(payload: dict[str, Any]) -> str:
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return "".join(f"[{ord(char):x}]" if ord(char) > 127 or char in "[]" else char for char in text)


def _source_parameters(row: dict[str, Any]) -> list[dict[str, Any]]:
    raw = row.get("templateParam")
    try:
        parameters = json.loads(raw) if isinstance(raw, str) else raw
    except ValueError:
        raise ReportApiError("动环报表参数配置无效") from None
    if not isinstance(parameters, list) or any(not isinstance(item, dict) for item in parameters):
        raise ReportApiError("动环报表参数配置无效")
    return parameters


def _init_parameters(
    row: dict[str, Any], parameters: list[dict[str, Any]], *, session_id: str,
    query_start: str, query_end: str,
) -> dict[str, Any]:
    template = str(row.get("tpl_name") or "").strip()
    if not template.lower().endswith(".cpt") or not row.get("plan_id") or row.get("dcstruc_id") is None:
        raise ReportApiError("动环报表模板信息不完整")
    result: dict[str, Any] = {
        "reportlet": template,
        "resourceid": row["dcstruc_id"],
        "starttime": query_start,
        "endtime": query_end,
        "token": session_id,
        "__filename__": template.rsplit(".", 1)[0],
        "op": "write",
    }
    for item in parameters:
        key = str(item.get("key") or "").strip()
        if not key or key in {"op", "token", "reportlet", "resourceid", "starttime", "endtime"}:
            continue
        value = item.get("value", "")
        if not isinstance(value, (str, int, float, bool)):
            raise ReportApiError("动环报表参数配置无效")
        if "tree" in str(item.get("type") or "").lower() and len(str(value)) > 4000:
            value = f"redirect:{row['plan_id']}"
        result[key] = value
    return result


def _body_parameters(
    parameters: list[dict[str, Any]], *, query_start: str, query_end: str,
    scale_label: str, sheet_name: str,
) -> tuple[dict[str, Any], list[str] | None]:
    scale = _SCALE_CODES.get(scale_label)
    if not scale:
        raise ReportApiError("动环报表查询刻度不支持")
    payload: dict[str, Any] = {
        "开始时间": query_start, "结束时间": query_end, "查询刻度": scale,
    }
    if any(item.get("key") == "测点列表" for item in parameters):
        points = _point_parameters(parameters)
        layout = next((item.get("value") for item in parameters if item.get("key") == "排列方式"), "B")
        payload.update({"测点列表": points, "排列方式": str(layout or "B"), "报表名称": ""})
        return payload, points
    if not sheet_name:
        raise ReportApiError("动环支路报表缺少配置表名称")
    payload["配置文件SHEET名"] = sheet_name
    return payload, None


def _query_once(
    client: BmsReportApiSession, *, report_name: str, query_start: str,
    query_end: str, scale_label: str, sheet_name: str,
) -> tuple[list[list[Any]], dict[int, str], int | None]:
    row = client.find_report(report_name)
    parameters = _source_parameters(row)
    init = _init_parameters(
        row, parameters, session_id=client._session_id, query_start=query_start, query_end=query_end,
    )
    response = client.request("GET", "/xbreport/ReportServer", params=init, timeout=(5, 120))
    try:
        html = response.content.decode("gb18030")
    except UnicodeDecodeError:
        raise ReportApiError("动环报表初始化响应编码无效") from None
    report_session = _report_session_id(html)
    body, points = _body_parameters(
        parameters, query_start=query_start, query_end=query_end,
        scale_label=scale_label, sheet_name=sheet_name,
    )
    client.request(
        "POST", "/xbreport/ReportServer",
        params={"op": "fr_dialog", "cmd": "parameters_d", "sessionID": report_session},
        data={"__parameters__": _encode_parameters(body)}, timeout=(5, 180),
    )
    response = client.request(
        "GET", "/xbreport/ReportServer",
        params={"op": "fr_write", "cmd": "read_w_content", "sessionID": report_session,
                "reportIndex": 0, "pn": 0},
        stream=True, timeout=(5, 180),
    )
    try:
        content_type = str(response.headers.get("content-type", "")).lower()
        encoding = "gb18030" if any(value in content_type for value in ("gbk", "gb2312", "gb18030")) else "utf-8"
        result = parse_report_grid(
            response.iter_content(chunk_size=65536), encoding=encoding,
            max_rows=50000, max_columns=4096, max_body_bytes=256 * 1024 * 1024,
            max_cells=4_000_000,
        )
    finally:
        response.close()
    rows = normalize_report_rows(
        result["grid"], result["row_ids"], expected_point_ids=points,
        query_start=query_start, query_end=query_end,
        time_tolerance_sec=_SCALE_SECONDS[scale_label],
    )
    return rows, result["row_ids"], len(points) if points is not None else None


def _validate_daily_hour_columns(rows: list[list[Any]], *, family: str, business_date: str,
                                 start: datetime, end: datetime) -> None:
    daily = {"monthly_report_family", "branch_power_family", "branch_current_family",
             "branch_switch_family", "building_full_cabinet_power_family"}
    if family not in daily or not business_date:
        return
    day = datetime.strptime(business_date, "%Y-%m-%d")
    if start > day or end < day + timedelta(days=1):
        return
    available = set()
    for value in rows[1]:
        try:
            available.add(datetime.fromisoformat(str(value or "").strip().replace("/", "-")))
        except ValueError:
            continue
    count = 25 if family == "monthly_report_family" else 24
    missing = [day + timedelta(hours=i) for i in range(count) if day + timedelta(hours=i) not in available]
    if missing:
        raise ReportApiError("动环报表缺少业务日小时列: " + ",".join(x.strftime("%Y-%m-%d %H:%M") for x in missing))


def query_report_json(
    site: dict[str, Any], *, source_family: str, report_name: str,
    query_start: str, query_end: str, scale_label: str,
    sheet_name: str = "", business_date: str = "",
) -> dict[str, Any]:
    building = str(site.get("building") or "").strip()
    if building not in {"A楼", "B楼", "C楼", "D楼", "E楼"} or source_family not in _SOURCE_FAMILIES:
        raise ReportApiError("动环报表楼栋或来源无效")
    try:
        start = datetime.strptime(query_start, "%Y-%m-%d %H:%M:%S")
        end = datetime.strptime(query_end, "%Y-%m-%d %H:%M:%S")
        if business_date:
            parsed_date = datetime.strptime(business_date, "%Y-%m-%d")
            if parsed_date.strftime("%Y-%m-%d") != business_date:
                raise ValueError
    except ValueError:
        raise ReportApiError("动环报表查询日期无效") from None
    if end < start or scale_label not in _SCALE_CODES or not report_name:
        raise ReportApiError("动环报表查询参数无效")
    from app.modules.shared_bridge.service.api_request_capacity import api_request_slot

    with api_request_slot():
        for attempt in range(2):
            try:
                with BmsReportApiSession(site) as client:
                    client.login()
                    rows, row_ids, requested_count = _query_once(
                        client, report_name=report_name, query_start=query_start, query_end=query_end,
                        scale_label=scale_label, sheet_name=sheet_name,
                    )
                    _validate_daily_hour_columns(rows, family=source_family, business_date=business_date, start=start, end=end)
                break
            except ReportApiUnauthorized:
                if attempt:
                    raise
        else:  # pragma: no cover
            raise ReportApiError("动环报表获取失败")
    return {
        "schema_version": 1,
        "source_family": source_family,
        "building": building,
        "business_date": business_date,
        "report_name": report_name,
        "query_start": query_start,
        "query_end": query_end,
        "scale_label": scale_label,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "requested_point_count": requested_count,
        "returned_point_count": len(row_ids),
        "sheets": [{"name": "sheet1", "rows": rows}],
    }
