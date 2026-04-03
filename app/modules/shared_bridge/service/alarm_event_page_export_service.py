from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence
from urllib.parse import urlparse

from playwright.async_api import APIRequestContext, Page

from app.shared.utils.atomic_file import atomic_write_text


ALARM_EVENT_JSON_SCHEMA_VERSION = 1
ALARM_EVENT_EVENT_PAGE_SIZE = 50
ALARM_EVENT_COUNT_PAGE_SIZE = 20
ALARM_EVENT_WINDOW_DAYS = 60
ALARM_EVENT_EXPORT_COLUMNS: list[tuple[str, str]] = [
    ("level", "级别"),
    ("content", "内容"),
    ("position", "位置"),
    ("object", "对象"),
    ("event_time", "告警时间"),
    ("accept_time", "接警时间"),
    ("is_accept", "处理状态"),
    ("accept_by", "处理人"),
    ("accept_content", "处理内容"),
    ("recover_time", "恢复时间"),
    ("is_recover", "恢复状态"),
    ("event_snapshot", "告警快照"),
    ("event_type", "事件类型"),
    ("confirm_type", "确认类型"),
    ("event_suggest", "建议"),
    ("confirm_time", "确认时间"),
    ("confirm_by", "确认人"),
    ("confirm_description", "确认说明"),
    ("event_snapshot", "实时值"),
    ("alarm_threshold", "阈值"),
]

_LEVEL_LABELS = {
    "1": "紧急",
    "2": "严重",
    "3": "重要",
    "4": "次要",
    "5": "预警",
}
_PROCESS_STATUS_LABELS = {
    "0": "未处理",
    "1": "处理中",
    "2": "已处理",
}
_EVENT_TYPE_TEXT_KEYS: Sequence[str] = (
    "event_type_name",
    "event_type_text",
    "event_type_label",
    "event_type_desc",
    "alarm_type",
    "alarm_type_name",
    "alarm_type_text",
    "alarm_type_label",
)
_EVENT_TYPE_LABELS = {
    "0": "通信中断",
    "2": "过高报警",
    "3": "不正常值",
    "4": "过低报警",
}


def query_window_start(when: datetime | None = None) -> datetime:
    now = when or datetime.now()
    return now - timedelta(days=ALARM_EVENT_WINDOW_DAYS)


def query_window_start_text(when: datetime | None = None) -> str:
    return query_window_start(when).strftime("%Y-%m-%d %H:%M:%S")


def scheduled_bucket_for_time(when: datetime | None = None) -> str:
    now = when or datetime.now()
    if now.hour >= 16:
        return now.strftime("%Y-%m-%d 16")
    if now.hour >= 8:
        return now.strftime("%Y-%m-%d 08")
    previous_day = now - timedelta(days=1)
    return previous_day.strftime("%Y-%m-%d 16")


def _seconds_to_text(value: Any) -> str:
    try:
        parsed = int(float(value or 0))
    except Exception:  # noqa: BLE001
        return ""
    if parsed <= 0:
        return ""
    return datetime.fromtimestamp(parsed).strftime("%Y-%m-%d %H:%M:%S")


def _normalize_numeric_key(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(int(float(value)))
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return str(int(float(text)))
    except Exception:  # noqa: BLE001
        return text


def _map_level(value: Any) -> str:
    key = _normalize_numeric_key(value)
    return _LEVEL_LABELS.get(key, key)


def _map_process_status(value: Any) -> str:
    key = _normalize_numeric_key(value)
    return _PROCESS_STATUS_LABELS.get(key, key or "")


def _map_recover_status(value: Any) -> str:
    key = _normalize_numeric_key(value)
    return "已恢复" if key == "1" else "未恢复"


def _coerce_text(value: Any) -> str:
    return str(value or "").strip()


def _map_event_type(item: Dict[str, Any]) -> str:
    for key in _EVENT_TYPE_TEXT_KEYS:
        text = _coerce_text(item.get(key))
        if text:
            return text
    raw_key = _normalize_numeric_key(item.get("event_type"))
    return _EVENT_TYPE_LABELS.get(raw_key, raw_key)


def _split_event_source(value: Any, *, fallback_object: str = "") -> tuple[str, str]:
    parts = [part.strip() for part in str(value or "").split("/") if str(part or "").strip()]
    if not parts:
        return "", str(fallback_object or "").strip()
    if len(parts) == 1:
        return parts[0], str(fallback_object or "").strip() or parts[0]
    return "/".join(parts[:-1]), parts[-1]


def _resolve_alarm_api_base_url(raw_url: str) -> str:
    parsed = urlparse(str(raw_url or "").strip())
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    raise RuntimeError("告警 API 基地址无法解析")


def _summarize_alarm_api_error(error: Any) -> str:
    text = str(error or "").replace("\r", "\n").strip()
    if "Call log:" in text:
        text = text.split("Call log:", 1)[0].strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) > 240:
        text = f"{text[:237]}..."
    return text or "未知错误"


def _build_alarm_query_filter(*, start_ts: int, end_ts: int) -> Dict[str, Any]:
    return {
        "and": [
            {
                "and": [
                    {"field": "event_type", "operator": "notin", "value": 7},
                    {"field": "masked", "operator": "eq", "value": 0},
                    {"field": "cep_processed", "operator": "eq", "value": 0},
                    {"field": "event_level", "operator": "in", "value": [1, 2, 3, 4, 5]},
                    {"field": "event_time", "operator": "gte", "value": start_ts},
                    {"field": "event_time", "operator": "lte", "value": end_ts},
                ]
            }
        ]
    }


async def _post_json(request_context: APIRequestContext, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    response = await request_context.post(
        url,
        headers={
            "Content-Type": "application/json;charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
        },
        data=payload,
        fail_on_status_code=False,
    )
    if not response.ok:
        try:
            error_text = await response.text()
        except Exception:  # noqa: BLE001
            error_text = ""
        raise RuntimeError(str(error_text or f"告警 API 请求失败: {url}").strip())
    data = await response.json()
    if not isinstance(data, dict):
        raise RuntimeError("告警 API 响应不是 JSON 对象")
    if str(data.get("error_code", "") or "").strip() != "00":
        raise RuntimeError(str(data.get("error_msg", "") or "告警 API 返回失败").strip())
    return data


def _extract_count_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = payload.get("data")
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        if isinstance(data.get("list"), list):
            items = data.get("list")
        elif isinstance(data.get("rows"), list):
            items = data.get("rows")
        elif isinstance(data.get("items"), list):
            items = data.get("items")
        elif isinstance(data.get("result"), list):
            items = data.get("result")
        else:
            items = []
    else:
        items = []

    level_counts: Dict[str, int] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        raw_level = item.get("event_level", item.get("group", item.get("value", "")))
        raw_count = item.get("count", item.get("total", item.get("value_count", 0)))
        label = _map_level(raw_level)
        if not label:
            continue
        try:
            level_counts[label] = int(float(raw_count or 0))
        except Exception:  # noqa: BLE001
            continue
    return {"level_counts": level_counts} if level_counts else {}


def _normalize_alarm_event_row(item: Dict[str, Any]) -> Dict[str, Any]:
    resource_id = str(item.get("resource_id", "") or "").strip()
    position_text, object_text = _split_event_source(item.get("event_source"), fallback_object=resource_id)
    return {
        "level": _map_level(item.get("event_level")),
        "content": str(item.get("content", "") or "").strip(),
        "position": position_text,
        "object": object_text or resource_id,
        "event_time": _seconds_to_text(item.get("event_time")),
        "accept_time": _seconds_to_text(item.get("accept_time")),
        "is_accept": _map_process_status(item.get("is_accept")),
        "accept_by": str(item.get("accept_by", "") or "").strip(),
        "accept_content": str(item.get("accept_description", "") or "").strip(),
        "recover_time": _seconds_to_text(item.get("recover_time")),
        "is_recover": _map_recover_status(item.get("is_recover")),
        "event_snapshot": str(item.get("event_snapshot", "") or "").strip(),
        "event_type": _map_event_type(item),
        "confirm_type": "真实告警",
        "event_suggest": str(item.get("event_suggest", "") or "").strip(),
        "confirm_time": _seconds_to_text(item.get("confirm_time")),
        "confirm_by": str(item.get("confirm_by", "") or "").strip(),
        "confirm_description": str(item.get("confirm_description", "") or "").strip(),
        "alarm_threshold": "",
    }


async def collect_alarm_event_rows(
    request_context: APIRequestContext,
    *,
    base_url: str,
    now: datetime | None = None,
    emit_log: Callable[[str], None] | None = None,
    log_prefix: str = "",
) -> Dict[str, Any]:
    target_now = now or datetime.now()
    query_start_dt = query_window_start(target_now)
    query_start_text_value = query_window_start_text(target_now)
    query_end_text = target_now.strftime("%Y-%m-%d %H:%M:%S")
    start_ts = int(query_start_dt.timestamp())
    end_ts = int(target_now.timestamp())
    normalized_base_url = _resolve_alarm_api_base_url(base_url)
    query_filter = _build_alarm_query_filter(start_ts=start_ts, end_ts=end_ts)
    event_url = f"{normalized_base_url}/api/v2/tsdb/status/event"
    count_url = f"{normalized_base_url}/api/v2/tsdb/status/event/count"

    rows: List[Dict[str, Any]] = []
    seen_page_signatures: set[str] = set()
    page_number = 1
    pages_fetched = 0
    while True:
        payload = {
            "where": query_filter,
            "sorts": [{"field": "event_time", "type": "DESC"}],
            "extra": True,
            "page": {"number": page_number, "size": ALARM_EVENT_EVENT_PAGE_SIZE},
        }
        try:
            response = await _post_json(request_context, event_url, payload)
            data = response.get("data")
            if not isinstance(data, dict):
                raise RuntimeError("告警事件 API data 字段缺失")
            page_info = data.get("page", {})
            event_list = data.get("event_list")
            if not isinstance(event_list, list):
                raise RuntimeError("告警事件 API event_list 字段缺失")
        except Exception as exc:  # noqa: BLE001
            if emit_log is not None:
                emit_log(
                    f"{log_prefix}告警 API 拉取失败: page={page_number}, accumulated_rows={len(rows)}, "
                    f"query_start={query_start_text_value}, query_end={query_end_text}, url={event_url}, "
                    f"error={_summarize_alarm_api_error(exc)}"
                )
            raise
        if not event_list:
            break

        total_pages = 0
        if isinstance(page_info, dict):
            for key in ("total_page", "total_pages", "page_count", "pages", "total"):
                try:
                    parsed = int(float((page_info or {}).get(key, 0) or 0))
                except Exception:  # noqa: BLE001
                    parsed = 0
                if parsed > 0:
                    total_pages = parsed
                    break

        page_signature = json.dumps(
            {
                "count": len(event_list),
                "first": event_list[0],
                "last": event_list[-1],
            },
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
        if page_signature in seen_page_signatures:
            break
        seen_page_signatures.add(page_signature)

        for item in event_list:
            if isinstance(item, dict):
                rows.append(_normalize_alarm_event_row(item))
        pages_fetched = page_number

        if total_pages > 0 and page_number < total_pages:
            page_number += 1
            continue
        if len(event_list) >= ALARM_EVENT_EVENT_PAGE_SIZE:
            page_number += 1
            continue
        break

    count_summary: Dict[str, Any] = {}
    try:
        count_payload = {
            "group": "event_level",
            "where": query_filter,
            "sorts": [{"field": "event_time", "type": "DESC"}],
            "page": {"number": "1", "size": str(ALARM_EVENT_COUNT_PAGE_SIZE)},
            "extra": True,
        }
        count_response = await _post_json(request_context, count_url, count_payload)
        count_summary = _extract_count_summary(count_response)
    except Exception as exc:  # noqa: BLE001
        if emit_log is not None:
            emit_log(
                f"{log_prefix}告警统计 API 获取失败: query_start={query_start_text_value}, query_end={query_end_text}, "
                f"url={count_url}, error={_summarize_alarm_api_error(exc)}"
            )
        count_summary = {}

    return {
        "query_start": query_start_text_value,
        "query_end": query_end_text,
        "row_count": len(rows),
        "pages_fetched": pages_fetched,
        "count_summary": count_summary,
        "rows": rows,
    }


async def stream_alarm_event_json_document(
    request_context: APIRequestContext,
    *,
    base_url: str,
    output_path: Path,
    source_family: str,
    building: str,
    bucket_kind: str,
    bucket_key: str,
    now: datetime | None = None,
    emit_log: Callable[[str], None] | None = None,
    log_prefix: str = "",
) -> Dict[str, Any]:
    target_now = now or datetime.now()
    generated_text = target_now.strftime("%Y-%m-%d %H:%M:%S")
    query_start_dt = query_window_start(target_now)
    query_start_text_value = query_window_start_text(target_now)
    query_end_text = target_now.strftime("%Y-%m-%d %H:%M:%S")
    start_ts = int(query_start_dt.timestamp())
    end_ts = int(target_now.timestamp())
    normalized_base_url = _resolve_alarm_api_base_url(base_url)
    query_filter = _build_alarm_query_filter(start_ts=start_ts, end_ts=end_ts)
    event_url = f"{normalized_base_url}/api/v2/tsdb/status/event"
    count_url = f"{normalized_base_url}/api/v2/tsdb/status/event/count"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_name(f"{output_path.name}.tmp")
    seen_page_signatures: set[str] = set()
    page_number = 1
    row_count = 0
    pages_fetched = 0
    first_row = True
    count_summary: Dict[str, Any] = {}
    with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write("{\n")
        handle.write(f'  "schema_version": {ALARM_EVENT_JSON_SCHEMA_VERSION},\n')
        handle.write(f'  "source_family": {json.dumps(str(source_family or "").strip(), ensure_ascii=False)},\n')
        handle.write(f'  "building": {json.dumps(str(building or "").strip(), ensure_ascii=False)},\n')
        handle.write(f'  "bucket_kind": {json.dumps(str(bucket_kind or "").strip(), ensure_ascii=False)},\n')
        handle.write(f'  "bucket_key": {json.dumps(str(bucket_key or "").strip(), ensure_ascii=False)},\n')
        handle.write(f'  "generated_at": {json.dumps(generated_text, ensure_ascii=False)},\n')
        handle.write(f'  "query_start": {json.dumps(query_start_text_value, ensure_ascii=False)},\n')
        handle.write(f'  "query_end": {json.dumps(query_end_text, ensure_ascii=False)},\n')
        handle.write('  "rows": [\n')
        while True:
            payload = {
                "where": query_filter,
                "sorts": [{"field": "event_time", "type": "DESC"}],
                "extra": True,
                "page": {"number": page_number, "size": ALARM_EVENT_EVENT_PAGE_SIZE},
            }
            try:
                response = await _post_json(request_context, event_url, payload)
                data = response.get("data")
                if not isinstance(data, dict):
                    raise RuntimeError("告警事件 API data 字段缺失")
                page_info = data.get("page", {})
                event_list = data.get("event_list")
                if not isinstance(event_list, list):
                    raise RuntimeError("告警事件 API event_list 字段缺失")
            except Exception as exc:  # noqa: BLE001
                if emit_log is not None:
                    emit_log(
                        f"{log_prefix}告警 API 拉取失败: page={page_number}, accumulated_rows={row_count}, "
                        f"query_start={query_start_text_value}, query_end={query_end_text}, url={event_url}, "
                        f"error={_summarize_alarm_api_error(exc)}"
                    )
                raise
            if not event_list:
                break

            total_pages = 0
            if isinstance(page_info, dict):
                for key in ("total_page", "total_pages", "page_count", "pages", "total"):
                    try:
                        parsed = int(float((page_info or {}).get(key, 0) or 0))
                    except Exception:  # noqa: BLE001
                        parsed = 0
                    if parsed > 0:
                        total_pages = parsed
                        break

            page_signature = json.dumps(
                {"count": len(event_list), "first": event_list[0], "last": event_list[-1]},
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            )
            if page_signature in seen_page_signatures:
                break
            seen_page_signatures.add(page_signature)

            page_rows = [_normalize_alarm_event_row(item) for item in event_list if isinstance(item, dict)]
            for row in page_rows:
                if not first_row:
                    handle.write(",\n")
                handle.write("    ")
                handle.write(json.dumps(row, ensure_ascii=False))
                first_row = False
            row_count += len(page_rows)
            pages_fetched = page_number
            handle.flush()

            if total_pages > 0 and page_number < total_pages:
                page_number += 1
                continue
            if len(event_list) >= ALARM_EVENT_EVENT_PAGE_SIZE:
                page_number += 1
                continue
            break

        handle.write("\n  ],\n")
        try:
            count_payload = {
                "group": "event_level",
                "where": query_filter,
                "sorts": [{"field": "event_time", "type": "DESC"}],
                "page": {"number": "1", "size": str(ALARM_EVENT_COUNT_PAGE_SIZE)},
                "extra": True,
            }
            count_response = await _post_json(request_context, count_url, count_payload)
            count_summary = _extract_count_summary(count_response)
        except Exception as exc:  # noqa: BLE001
            if emit_log is not None:
                emit_log(
                    f"{log_prefix}告警统计 API 获取失败: query_start={query_start_text_value}, query_end={query_end_text}, "
                    f"url={count_url}, error={_summarize_alarm_api_error(exc)}"
                )
            count_summary = {}
        handle.write(f'  "row_count": {row_count},\n')
        handle.write(f'  "count_summary": {json.dumps(count_summary, ensure_ascii=False, indent=2)}\n')
        handle.write("}\n")
    temp_path.replace(output_path)
    if emit_log is not None:
        emit_log(
            f"{log_prefix}告警 API 拉取完成: file={output_path}, row_count={row_count}, pages={pages_fetched}, "
            f"query_start={query_start_text_value}, query_end={query_end_text}"
        )
    return {
        "query_start": query_start_text_value,
        "query_end": query_end_text,
        "row_count": row_count,
        "pages_fetched": pages_fetched,
        "count_summary": count_summary,
    }


class _PageBackedAPIResponse:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload

    @property
    def ok(self) -> bool:
        return bool(self._payload.get("ok"))

    @property
    def status(self) -> int:
        return int(self._payload.get("status", 0) or 0)

    async def json(self) -> Any:
        return self._payload.get("json")

    async def text(self) -> str:
        return str(self._payload.get("text", "") or self._payload.get("fetch_error", "") or "").strip()


class _PageBackedAPIRequestContext:
    def __init__(self, page: Page) -> None:
        self._page = page

    async def post(
        self,
        url: str,
        *,
        headers: Dict[str, str] | None = None,
        data: Dict[str, Any] | None = None,
        fail_on_status_code: bool | None = None,  # noqa: ARG002
    ) -> _PageBackedAPIResponse:
        payload = await self._page.evaluate(
            """
            async ({ url, headers, payload }) => {
              try {
                const resp = await fetch(url, {
                  method: 'POST',
                  credentials: 'include',
                  headers,
                  body: JSON.stringify(payload),
                });
                const text = await resp.text();
                let json = null;
                try {
                  json = text ? JSON.parse(text) : null;
                } catch (error) {
                  json = null;
                }
                return {
                  ok: !!resp.ok,
                  status: Number(resp.status || 0),
                  text,
                  json,
                };
              } catch (error) {
                return {
                  ok: false,
                  status: 0,
                  text: '',
                  json: null,
                  fetch_error: String(error && error.message ? error.message : error || ''),
                };
              }
            }
            """,
            {
                "url": url,
                "headers": headers or {
                    "Content-Type": "application/json;charset=UTF-8",
                    "X-Requested-With": "XMLHttpRequest",
                },
                "payload": data or {},
            },
        )
        if not isinstance(payload, dict):
            payload = {"ok": False, "status": 0, "text": "告警调试请求返回格式异常", "json": None}
        return _PageBackedAPIResponse(payload)


# Legacy debug helpers retained so standalone scripts can still import them.
async def _open_custom_columns_panel(page: Page) -> None:  # pragma: no cover
    raise RuntimeError("告警页面调试入口已退役，当前版本仅支持 API 拉取")


async def _select_all_columns(page: Page) -> None:  # pragma: no cover
    raise RuntimeError("告警页面调试入口已退役，当前版本仅支持 API 拉取")


async def _expand_time_filter(page: Page) -> None:  # pragma: no cover
    raise RuntimeError("告警页面调试入口已退役，当前版本仅支持 API 拉取")


async def _set_start_time(page: Page, start_text: str) -> None:  # pragma: no cover
    raise RuntimeError("告警页面调试入口已退役，当前版本仅支持 API 拉取")


async def _wait_for_table_change(
    page: Page,
    *,
    previous_signature: dict[str, Any] | None = None,
    previous_page: int | None = None,
    timeout_ms: int = 15000,
) -> None:  # pragma: no cover
    raise RuntimeError("告警页面调试入口已退役，当前版本仅支持 API 拉取")


async def _select_page_size(page: Page, value: str) -> None:  # pragma: no cover
    raise RuntimeError("告警页面调试入口已退役，当前版本仅支持 API 拉取")


async def _extract_current_rows(page: Page) -> List[Dict[str, str]]:  # pragma: no cover
    raise RuntimeError("告警页面调试入口已退役，当前版本仅支持 API 拉取")


async def _table_signature(page: Page) -> dict[str, Any]:  # pragma: no cover
    raise RuntimeError("告警页面调试入口已退役，当前版本仅支持 API 拉取")


async def _current_page_number(page: Page) -> int:  # pragma: no cover
    raise RuntimeError("告警页面调试入口已退役，当前版本仅支持 API 拉取")


async def exercise_alarm_event_page_actions(page: Page, *, now: datetime | None = None) -> Dict[str, Any]:
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=5000)
    except Exception:  # noqa: BLE001
        pass
    page_url = str(getattr(page, "url", "") or "").strip()
    base_url = _resolve_alarm_api_base_url(page_url)
    request_context = _PageBackedAPIRequestContext(page)
    payload = await collect_alarm_event_rows(request_context, base_url=base_url, now=now)
    return {
        "query_start": str(payload.get("query_start", "") or "").strip(),
        "query_end": str(payload.get("query_end", "") or "").strip(),
        "row_count": int(payload.get("row_count", 0) or 0),
        "current_page": 1,
        "content_column_visible": False,
        "start_time_ready": True,
        "page_url": page_url,
        "count_summary": payload.get("count_summary", {}),
    }


def build_alarm_event_json_document(
    *,
    source_family: str,
    building: str,
    bucket_kind: str,
    bucket_key: str,
    payload: Dict[str, Any],
    generated_at: datetime | None = None,
) -> Dict[str, Any]:
    rows = payload.get("rows", []) if isinstance(payload.get("rows", []), list) else []
    count_summary = payload.get("count_summary", {}) if isinstance(payload.get("count_summary", {}), dict) else {}
    generated_text = (generated_at or datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    return {
        "schema_version": ALARM_EVENT_JSON_SCHEMA_VERSION,
        "source_family": str(source_family or "").strip(),
        "building": str(building or "").strip(),
        "bucket_kind": str(bucket_kind or "").strip(),
        "bucket_key": str(bucket_key or "").strip(),
        "generated_at": generated_text,
        "query_start": str(payload.get("query_start", "") or "").strip(),
        "query_end": str(payload.get("query_end", "") or "").strip(),
        "row_count": len(rows),
        "count_summary": count_summary,
        "rows": rows,
    }


def write_alarm_event_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    atomic_write_text(path, text, encoding="utf-8")


def load_alarm_event_json(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    data = json.loads(text)
    if not isinstance(data, dict):
        raise RuntimeError("告警 JSON 文件顶层必须是对象")
    schema_version = data.get("schema_version")
    try:
        schema_version_int = int(schema_version)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"告警 JSON schema_version 非法: {schema_version!r}") from exc
    if schema_version_int != ALARM_EVENT_JSON_SCHEMA_VERSION:
        raise RuntimeError(f"告警 JSON schema_version 非法: {schema_version!r}")
    source_family = str(data.get("source_family", "") or "").strip()
    if source_family != "alarm_event_family":
        raise RuntimeError(f"告警 JSON source_family 非法: {source_family or '<empty>'}")
    building = str(data.get("building", "") or "").strip()
    if not building:
        raise RuntimeError("告警 JSON 缺少 building")
    bucket_kind = str(data.get("bucket_kind", "") or "").strip().lower()
    if bucket_kind not in {"latest", "manual"}:
        raise RuntimeError(f"告警 JSON bucket_kind 非法: {bucket_kind or '<empty>'}")
    rows = data.get("rows")
    if not isinstance(rows, list):
        raise RuntimeError("告警 JSON 缺少 rows 数组")
    row_count = data.get("row_count")
    if not isinstance(row_count, int) or row_count < 0:
        raise RuntimeError(f"告警 JSON row_count 非法: {row_count!r}")
    if row_count != len(rows):
        raise RuntimeError(f"告警 JSON row_count 与 rows 数量不一致: row_count={row_count}, rows={len(rows)}")
    return data
