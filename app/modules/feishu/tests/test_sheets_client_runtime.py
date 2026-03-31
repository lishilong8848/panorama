from __future__ import annotations

import pytest

from app.modules.feishu.service.sheets_client_runtime import FeishuSheetsClientRuntime


def test_extract_node_token_from_wiki_url() -> None:
    token = FeishuSheetsClientRuntime.extract_node_token_from_url(
        "https://vnet.feishu.cn/wiki/WlpWwkhQGi46pEkYbMTcNnOzntb"
    )

    assert token == "WlpWwkhQGi46pEkYbMTcNnOzntb"


def test_extract_node_token_from_invalid_url_raises() -> None:
    with pytest.raises(ValueError, match="无法从 URL 中提取 wiki node token"):
        FeishuSheetsClientRuntime.extract_node_token_from_url("https://vnet.feishu.cn/docx/abc")


def test_update_dimension_range_uses_dimension_properties(monkeypatch) -> None:
    runtime = FeishuSheetsClientRuntime(app_id="app_id", app_secret="app_secret")
    captured = {}

    def fake_request(method, url, *, payload=None, params=None, timeout=None):  # noqa: ANN001
        captured["method"] = method
        captured["url"] = url
        captured["payload"] = payload
        return {"code": 0, "data": {}}

    monkeypatch.setattr(runtime, "_request_json_with_auth_retry", fake_request)

    runtime.update_dimension_range(
        "spreadsheet_token",
        sheet_id="sheet_1",
        major_dimension="ROWS",
        start_index=0,
        end_index=1,
        pixel_size=42,
    )

    assert captured["method"] == "PUT"
    assert captured["payload"]["dimension"]["sheetId"] == "sheet_1"
    assert captured["payload"]["dimension"]["startIndex"] == 1
    assert captured["payload"]["dimension"]["endIndex"] == 1
    assert captured["payload"]["dimensionProperties"]["fixedSize"] == 42
    assert "fixedSize" not in captured["payload"]


def test_add_dimension_uses_length_payload(monkeypatch) -> None:
    runtime = FeishuSheetsClientRuntime(app_id="app_id", app_secret="app_secret")
    captured = {}

    def fake_request(method, url, *, payload=None, params=None, timeout=None):  # noqa: ANN001
        captured["method"] = method
        captured["url"] = url
        captured["payload"] = payload
        return {"code": 0, "data": {"addCount": 3}}

    monkeypatch.setattr(runtime, "_request_json_with_auth_retry", fake_request)

    runtime.add_dimension(
        "spreadsheet_token",
        sheet_id="sheet_1",
        major_dimension="COLUMNS",
        length=3,
    )

    assert captured["method"] == "POST"
    assert captured["payload"] == {
        "dimension": {
            "sheetId": "sheet_1",
            "majorDimension": "COLUMNS",
            "length": 3,
        }
    }


def test_batch_merge_cells_uses_merge_cells_endpoint(monkeypatch) -> None:
    runtime = FeishuSheetsClientRuntime(app_id="app_id", app_secret="app_secret")
    calls = []

    def fake_request(method, url, *, payload=None, params=None, timeout=None):  # noqa: ANN001
        calls.append({"method": method, "url": url, "payload": payload})
        return {"code": 0, "data": {}}

    monkeypatch.setattr(runtime, "_request_json_with_auth_retry", fake_request)

    runtime.batch_merge_cells(
        "spreadsheet_token",
        "sheet_1",
        [
            {
                "start_row_index": 0,
                "end_row_index": 2,
                "start_column_index": 0,
                "end_column_index": 2,
            }
        ],
    )

    assert calls[0]["method"] == "POST"
    assert calls[0]["url"].endswith("/merge_cells")
    assert calls[0]["payload"] == {"range": "sheet_1!A1:B2", "mergeType": "MERGE_ALL"}


def test_batch_unmerge_cells_uses_unmerge_cells_endpoint(monkeypatch) -> None:
    runtime = FeishuSheetsClientRuntime(app_id="app_id", app_secret="app_secret")
    calls = []

    def fake_request(method, url, *, payload=None, params=None, timeout=None):  # noqa: ANN001
        calls.append({"method": method, "url": url, "payload": payload})
        return {"code": 0, "data": {}}

    monkeypatch.setattr(runtime, "_request_json_with_auth_retry", fake_request)

    runtime.batch_unmerge_cells(
        "spreadsheet_token",
        "sheet_1",
        [
            {
                "start_row_index": 2,
                "end_row_index": 3,
                "start_column_index": 0,
                "end_column_index": 2,
            }
        ],
    )

    assert calls[0]["method"] == "POST"
    assert calls[0]["url"].endswith("/unmerge_cells")
    assert calls[0]["payload"] == {"range": "sheet_1!A3:B3"}


def test_query_sheets_uses_cache_until_force_refresh(monkeypatch) -> None:
    runtime = FeishuSheetsClientRuntime(app_id="app_id", app_secret="app_secret")
    calls = []

    def fake_request(method, url, *, payload=None, params=None, timeout=None):  # noqa: ANN001
        calls.append({"method": method, "url": url, "payload": payload})
        return {
            "code": 0,
            "data": {
                "sheets": [
                    {
                        "sheet_id": "sheet_1",
                        "title": "A楼",
                        "index": 0,
                        "grid_properties": {"row_count": 10, "column_count": 8},
                    }
                ]
            },
        }

    monkeypatch.setattr(runtime, "_request_json_with_auth_retry", fake_request)

    cache = {}
    first = runtime.query_sheets("spreadsheet_token", sheet_cache=cache)
    second = runtime.query_sheets("spreadsheet_token", sheet_cache=cache)
    refreshed = runtime.query_sheets("spreadsheet_token", sheet_cache=cache, force_refresh=True)

    assert len(calls) == 2
    assert first == second == refreshed
    assert cache["spreadsheet_token"][0]["sheet_id"] == "sheet_1"
