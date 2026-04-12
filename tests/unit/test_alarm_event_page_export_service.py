from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

import app.modules.shared_bridge.service.alarm_event_page_export_service as alarm_module


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    @property
    def ok(self) -> bool:
        return bool(self._payload.get("ok", True))

    @property
    def status(self) -> int:
        return int(self._payload.get("status", 200) or 200)

    async def json(self) -> dict:
        return self._payload["json"]

    async def text(self) -> str:
        return str(self._payload.get("text", "") or "").strip()


class _FakeAPIRequestContext:
    def __init__(self, *, event_pages: list[dict], count_payload: dict | None = None) -> None:
        self._event_pages = list(event_pages)
        self._count_payload = count_payload or {"error_code": "00", "error_msg": "Succeed", "data": []}
        self.requests: list[dict] = []

    async def post(self, url: str, *, headers: dict | None = None, data: dict | None = None, fail_on_status_code: bool | None = None):  # noqa: ANN001, ARG002
        self.requests.append({"url": url, "headers": headers or {}, "payload": data or {}})
        if url.endswith("/api/v2/tsdb/status/event"):
            page_number = int((data or {}).get("page", {}).get("number", 1) or 1)
            response_payload = self._event_pages[page_number - 1]
        elif url.endswith("/api/v2/tsdb/status/event/count"):
            response_payload = self._count_payload
        else:  # pragma: no cover
            raise AssertionError(f"unexpected url: {url}")
        return _FakeResponse({"ok": True, "status": 200, "json": response_payload})


def test_collect_alarm_event_rows_fetches_all_pages_and_normalizes_rows() -> None:
    now = datetime(2026, 4, 2, 10, 15, 30)
    event_pages = [
        {
            "error_code": "00",
            "error_msg": "Succeed",
            "data": {
                "page": {"total": 2, "number": 1, "size": 50},
                "event_list": [
                    {
                        "resource_id": "20_0_139_1_826_0",
                        "event_time": 1775110814,
                        "content": "107_电池内阻_u值: 过低报警",
                        "event_level": 4,
                        "event_type": 4,
                        "event_type_name": "不正常值",
                        "event_snapshot": "166.00",
                        "is_recover": 1,
                        "recover_time": 1775119605,
                        "confirm_time": 1775113373,
                        "confirm_by": "系统管理员",
                        "confirm_description": "确认说明",
                        "event_source": "南通阿里保税A区E楼/E楼/三层/电池室M3 E-346/E-346-HVDC-152-2组",
                        "event_suggest": "",
                        "is_accept": 2,
                        "accept_time": 1775113367,
                        "accept_by": "系统管理员",
                        "accept_description": "测试受理",
                    }
                ],
            },
        },
        {
            "error_code": "00",
            "error_msg": "Succeed",
            "data": {
                "page": {"total": 2, "number": 2, "size": 50},
                "event_list": [
                    {
                        "resource_id": "20_0_133_1_767_0",
                        "event_time": 1775110224,
                        "content": "48_电池内阻_u值: 过高报警",
                        "event_level": 2,
                        "event_type": 2,
                        "alarm_type": "联动告警",
                        "event_snapshot": "542.00",
                        "is_recover": 0,
                        "recover_time": 0,
                        "confirm_time": 1775113373,
                        "confirm_by": "系统管理员",
                        "confirm_description": "确认说明2",
                        "event_source": "南通阿里保税A区E楼/E楼/三层/电池室M3 E-346/E-346-HVDC-222-2组",
                        "event_suggest": "建议2",
                        "is_accept": 1,
                        "accept_time": 1775113367,
                        "accept_by": "系统管理员",
                        "accept_description": "测试受理2",
                    }
                ],
            },
        },
    ]
    count_payload = {
        "error_code": "00",
        "error_msg": "Succeed",
        "data": [
            {"event_level": 4, "count": 1},
            {"event_level": 2, "count": 1},
        ],
    }
    request_context = _FakeAPIRequestContext(event_pages=event_pages, count_payload=count_payload)

    result = asyncio.run(
        alarm_module.collect_alarm_event_rows(
            request_context,
            base_url="http://192.168.233.57",
            now=now,
        )
    )

    assert result["query_start"] == "2026-02-01 10:15:30"
    assert result["query_end"] == "2026-04-02 10:15:30"
    assert result["row_count"] == 2
    assert result["pages_fetched"] == 2
    assert result["count_summary"] == {"level_counts": {"次要": 1, "严重": 1}}
    assert len(request_context.requests) == 3

    first = result["rows"][0]
    second = result["rows"][1]
    assert first["level"] == "次要"
    assert first["position"] == "南通阿里保税A区E楼/E楼/三层/电池室M3 E-346"
    assert first["object"] == "E-346-HVDC-152-2组"
    assert first["is_accept"] == "已处理"
    assert first["is_recover"] == "已恢复"
    assert first["confirm_type"] == "真实告警"
    assert first["event_snapshot"] == "166.00"
    assert first["event_type"] == "不正常值"
    assert first["alarm_threshold"] == ""
    assert second["level"] == "严重"
    assert second["is_accept"] == "处理中"
    assert second["is_recover"] == "未恢复"
    assert second["event_type"] == "联动告警"


def test_build_write_and_load_alarm_event_json_document(tmp_path: Path) -> None:
    payload = {
        "query_start": "2026-02-01 10:15:30",
        "query_end": "2026-04-01 12:05:00",
        "count_summary": {"level_counts": {"次要": 2}},
        "rows": [
            {
                "level": "次要",
                "content": "风机状态: 告警",
                "position": "E楼/三层",
                "object": "E-311-CRAH-10",
                "event_time": "2026-04-01 14:19:36",
                "accept_time": "2026-04-01 14:20:40",
                "is_accept": "已处理",
                "accept_by": "系统管理员",
                "accept_content": "测试受理",
                "recover_time": "",
                "is_recover": "未恢复",
                "event_snapshot": "12.5",
                "event_type": "不正常值",
                "confirm_type": "真实告警",
                "event_suggest": "",
                "confirm_time": "2026-04-01 14:20:49",
                "confirm_by": "系统管理员",
                "confirm_description": "测试确认",
                "alarm_threshold": "",
            }
        ],
    }
    document = alarm_module.build_alarm_event_json_document(
        source_family="alarm_event_family",
        building="A楼",
        bucket_kind="latest",
        bucket_key="2026-04-01 08",
        payload=payload,
        generated_at=datetime(2026, 4, 1, 12, 5, 0),
    )
    path = tmp_path / "A楼.json"

    alarm_module.write_alarm_event_json(path, document)
    loaded = alarm_module.load_alarm_event_json(path)

    assert path.exists()
    assert loaded["schema_version"] == 1
    assert loaded["building"] == "A楼"
    assert loaded["bucket_kind"] == "latest"
    assert loaded["bucket_key"] == "2026-04-01 08"
    assert loaded["generated_at"] == "2026-04-01 12:05:00"
    assert loaded["row_count"] == 1
    assert loaded["count_summary"] == {"level_counts": {"次要": 2}}
    assert loaded["rows"][0]["event_snapshot"] == "12.5"

def test_collect_alarm_event_rows_treats_page_total_as_page_count_when_needed() -> None:
    event_pages = [
        {
            "error_code": "00",
            "error_msg": "Succeed",
            "data": {
                "page": {"total": 3, "number": 1, "size": 50},
                "event_list": [{"content": f"p1-{idx}", "event_level": 4, "event_time": 1775110000 + idx} for idx in range(50)],
            },
        },
        {
            "error_code": "00",
            "error_msg": "Succeed",
            "data": {
                "page": {"total": 3, "number": 2, "size": 50},
                "event_list": [{"content": f"p2-{idx}", "event_level": 4, "event_time": 1775120000 + idx} for idx in range(50)],
            },
        },
        {
            "error_code": "00",
            "error_msg": "Succeed",
            "data": {
                "page": {"total": 3, "number": 3, "size": 50},
                "event_list": [{"content": "p3-0", "event_level": 4, "event_time": 1775130000}],
            },
        },
    ]
    request_context = _FakeAPIRequestContext(event_pages=event_pages)

    result = asyncio.run(
        alarm_module.collect_alarm_event_rows(
            request_context,
            base_url="http://192.168.233.57",
            now=datetime(2026, 4, 2, 10, 15, 30),
        )
    )

    assert result["row_count"] == 101
    assert result["pages_fetched"] == 3
    assert len(result["rows"]) == 101
    event_requests = [item for item in request_context.requests if item["url"].endswith("/api/v2/tsdb/status/event")]
    assert len(event_requests) == 3

def test_collect_alarm_event_rows_ignores_misleading_page_total_and_fetches_until_short_page() -> None:
    event_pages = []
    base_time = 1775110000
    for page_no in range(1, 51):
        event_pages.append(
            {
                "error_code": "00",
                "error_msg": "Succeed",
                "data": {
                    "page": {"total": 50, "number": page_no, "size": 50},
                    "event_list": [
                        {"content": f"p{page_no}-{idx}", "event_level": 4, "event_time": base_time + (page_no * 100) + idx}
                        for idx in range(50)
                    ],
                },
            }
        )
    event_pages.append(
        {
            "error_code": "00",
            "error_msg": "Succeed",
            "data": {
                "page": {"total": 50, "number": 51, "size": 50},
                "event_list": [
                    {"content": f"p51-{idx}", "event_level": 4, "event_time": base_time + 5100 + idx}
                    for idx in range(7)
                ],
            },
        }
    )
    request_context = _FakeAPIRequestContext(event_pages=event_pages)

    result = asyncio.run(
        alarm_module.collect_alarm_event_rows(
            request_context,
            base_url="http://192.168.233.57",
            now=datetime(2026, 4, 2, 10, 15, 30),
        )
    )

    assert result["row_count"] == 2507
    assert result["pages_fetched"] == 51
    event_requests = [item for item in request_context.requests if item["url"].endswith("/api/v2/tsdb/status/event")]
    assert len(event_requests) == 51


def test_stream_alarm_event_json_document_writes_json_incrementally_and_logs(tmp_path: Path) -> None:
    event_pages = [
        {
            "error_code": "00",
            "error_msg": "Succeed",
            "data": {
                "page": {"total": 2, "number": 1, "size": 50},
                "event_list": [{"content": f"p1-{idx}", "event_level": 4, "event_time": 1775110000 + idx} for idx in range(50)],
            },
        },
        {
            "error_code": "00",
            "error_msg": "Succeed",
            "data": {
                "page": {"total": 2, "number": 2, "size": 50},
                "event_list": [{"content": f"p2-{idx}", "event_level": 4, "event_time": 1775120000 + idx} for idx in range(3)],
            },
        },
    ]
    request_context = _FakeAPIRequestContext(event_pages=event_pages)
    output_path = tmp_path / "A楼.json"
    logs: list[str] = []

    result = asyncio.run(
        alarm_module.stream_alarm_event_json_document(
            request_context,
            base_url="http://192.168.233.57",
            output_path=output_path,
            source_family="alarm_event_family",
            building="A楼",
            bucket_kind="manual",
            bucket_key="2026-04-03 00:19:53",
            now=datetime(2026, 4, 3, 0, 20, 18),
            emit_log=logs.append,
            log_prefix="[测试][A楼] ",
        )
    )

    payload = alarm_module.load_alarm_event_json(output_path)
    assert result["row_count"] == 53
    assert result["pages_fetched"] == 2
    assert payload["row_count"] == 53
    assert len(payload["rows"]) == 53
    assert any("告警 API 拉取完成" in item for item in logs)
    assert all("告警 API 请求:" not in item for item in logs)
    assert all("告警 API 响应:" not in item for item in logs)


def test_stream_alarm_event_json_document_logs_single_failure_summary(tmp_path: Path) -> None:
    event_pages = [
        {
            "error_code": "00",
            "error_msg": "Succeed",
            "data": {
                "page": {"total": 2, "number": 1, "size": 50},
                "event_list": [{"content": f"p1-{idx}", "event_level": 4, "event_time": 1775110000 + idx} for idx in range(50)],
            },
        },
    ]

    class _FailingAPIRequestContext(_FakeAPIRequestContext):
        async def post(self, url: str, *, headers: dict | None = None, data: dict | None = None, fail_on_status_code: bool | None = None):  # noqa: ANN001, ARG002
            payload = data or {}
            if url.endswith("/api/v2/tsdb/status/event") and int(payload.get("page", {}).get("number", 1) or 1) == 2:
                raise RuntimeError("APIRequestContext.post: connect ETIMEDOUT 192.168.232.53:80 Call log: cookie=abc")
            return await super().post(url, headers=headers, data=data, fail_on_status_code=fail_on_status_code)

    request_context = _FailingAPIRequestContext(event_pages=event_pages)
    output_path = tmp_path / "A楼.json"
    logs: list[str] = []

    try:
        asyncio.run(
            alarm_module.stream_alarm_event_json_document(
                request_context,
                base_url="http://192.168.233.57",
                output_path=output_path,
                source_family="alarm_event_family",
                building="A楼",
                bucket_kind="manual",
                bucket_key="2026-04-03 00:19:53",
                now=datetime(2026, 4, 3, 0, 20, 18),
                emit_log=logs.append,
                log_prefix="[测试][A楼] ",
            )
        )
    except RuntimeError as exc:
        assert "ETIMEDOUT" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected timeout")

    assert any("告警 API 拉取失败: page=2" in item for item in logs)
    assert any("accumulated_rows=50" in item for item in logs)
    assert all("cookie=abc" not in item for item in logs)


def test_collect_alarm_event_rows_maps_latest_level_and_event_type_labels() -> None:
    request_context = _FakeAPIRequestContext(
        event_pages=[
            {
                "error_code": "00",
                "error_msg": "Succeed",
                "data": {
                    "page": {"total": 1, "number": 1, "size": 50},
                    "event_list": [
                        {"content": "comm", "event_level": 5, "event_type": 0, "event_time": 1775110000},
                        {"content": "low", "event_level": 4, "event_type": 4, "event_time": 1775110001},
                        {"content": "high", "event_level": 3, "event_type": 2, "event_time": 1775110002},
                        {"content": "abnormal", "event_level": 2, "event_type": 3, "event_time": 1775110003},
                        {"content": "raw", "event_level": 1, "event_type": 1, "event_time": 1775110004},
                    ],
                },
            }
        ],
        count_payload={
            "error_code": "00",
            "error_msg": "Succeed",
            "data": [
                {"event_level": 5, "count": 1},
                {"event_level": 4, "count": 1},
                {"event_level": 3, "count": 1},
                {"event_level": 2, "count": 1},
                {"event_level": 1, "count": 1},
            ],
        },
    )

    result = asyncio.run(
        alarm_module.collect_alarm_event_rows(
            request_context,
            base_url="http://192.168.233.57",
            now=datetime(2026, 4, 3, 2, 30, 0),
        )
    )

    assert [row["level"] for row in result["rows"]] == ["\u9884\u8b66", "\u6b21\u8981", "\u91cd\u8981", "\u4e25\u91cd", "\u7d27\u6025"]
    assert [row["event_type"] for row in result["rows"]] == ["\u901a\u4fe1\u4e2d\u65ad", "\u8fc7\u4f4e\u62a5\u8b66", "\u8fc7\u9ad8\u62a5\u8b66", "\u4e0d\u6b63\u5e38\u503c", "1"]
    assert result["count_summary"] == {"level_counts": {"\u9884\u8b66": 1, "\u6b21\u8981": 1, "\u91cd\u8981": 1, "\u4e25\u91cd": 1, "\u7d27\u6025": 1}}
