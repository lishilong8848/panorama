from __future__ import annotations

import json
import threading
from typing import Any

import pytest

from app.modules.internal_bridge_http.service.client import InternalBridgeHttpClient
from app.modules.internal_bridge_http.service.client import InternalBridgeHttpError
from app.modules.report_pipeline.service.system_alert_log_upload_service import (
    SystemAlertLogUploadService,
)


def test_internal_bridge_client_does_not_send_token(monkeypatch):
    captured_headers: dict[str, str] = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps({"ok": True}).encode("utf-8")

    def fake_urlopen(req, timeout):  # noqa: ANN001
        nonlocal captured_headers
        captured_headers = dict(req.headers)
        return Response()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    client = InternalBridgeHttpClient(
        base_url="http://127.0.0.1:18765",
        auth_token="legacy-token-must-be-ignored",
    )
    assert client.health() == {"ok": True}
    assert "x-bridge-token" not in {key.lower() for key in captured_headers}


def test_internal_bridge_client_treats_busy_source_index_as_retryable(monkeypatch):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "ok": False,
                    "status": "busy",
                    "queued": True,
                    "retry_after_sec": 60,
                    "message": "内网端 source-index 正在处理其他请求，请约 60 秒后重试",
                },
                ensure_ascii=False,
            ).encode("utf-8")

    monkeypatch.setattr("urllib.request.urlopen", lambda req, timeout: Response())

    client = InternalBridgeHttpClient(base_url="http://127.0.0.1:18765")
    with pytest.raises(InternalBridgeHttpError, match="retry_after_sec=60"):
        client.source_index(source_family="branch_power_family", building="A楼")


def test_internal_bridge_client_coalesces_concurrent_source_index_batches(monkeypatch):
    captured_payloads: list[dict[str, Any]] = []

    class Response:
        def __init__(self, body: dict[str, Any]):
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(self.body, ensure_ascii=False).encode("utf-8")

    def fake_urlopen(req, timeout):  # noqa: ANN001
        payload = json.loads(req.data.decode("utf-8"))
        captured_payloads.append(payload)
        queries = payload.get("queries", [])
        return Response(
            {
                "ok": True,
                "results": [
                    {
                        "index": index,
                        "ok": True,
                        "entries": [{"building": query.get("building", "")}],
                    }
                    for index, query in enumerate(queries)
                ],
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    client = InternalBridgeHttpClient(base_url="http://127.0.0.1:18765")
    client._source_index_batch_window_sec = 0.1
    barrier = threading.Barrier(3)
    results: list[list[dict[str, Any]]] = []
    errors: list[BaseException] = []

    def worker(building: str) -> None:
        try:
            barrier.wait()
            results.append(client.source_index_batch([{"source_family": "branch_power_family", "building": building}]))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(building,)) for building in ("A楼", "B楼")]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(captured_payloads) == 1
    assert len(captured_payloads[0]["queries"]) == 2
    returned_buildings = sorted(item[0]["entries"][0]["building"] for item in results)
    assert returned_buildings == ["A楼", "B楼"]


def test_internal_bridge_client_bad_query_does_not_block_coalesced_batch(monkeypatch):
    captured_payloads: list[dict[str, Any]] = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "ok": True,
                    "results": [
                        {
                            "index": 0,
                            "ok": True,
                            "entries": [{"building": "A楼"}],
                        }
                    ],
                },
                ensure_ascii=False,
            ).encode("utf-8")

    def fake_urlopen(req, timeout):  # noqa: ANN001
        captured_payloads.append(json.loads(req.data.decode("utf-8")))
        return Response()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    client = InternalBridgeHttpClient(base_url="http://127.0.0.1:18765")
    results = client.source_index_batch(
        [
            {"source_family": "branch_power_family", "building": "A楼"},
            {"source_family": "branch_power_family", "building": "B楼", "limit": "bad"},
        ]
    )

    assert len(captured_payloads) == 1
    assert len(captured_payloads[0]["queries"]) == 1
    assert results[0]["ok"] is True
    assert results[1]["ok"] is False
    assert "invalid source-index query" in results[1]["error"]


def test_internal_bridge_client_dedupes_queries_but_returns_each_result(monkeypatch):
    captured_payloads: list[dict[str, Any]] = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "ok": True,
                    "results": [
                        {
                            "index": 0,
                            "ok": True,
                            "entries": [{"building": "A楼", "entry_id": "same"}],
                        }
                    ],
                },
                ensure_ascii=False,
            ).encode("utf-8")

    def fake_urlopen(req, timeout):  # noqa: ANN001
        captured_payloads.append(json.loads(req.data.decode("utf-8")))
        return Response()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    client = InternalBridgeHttpClient(base_url="http://127.0.0.1:18765")
    query = {"source_family": "branch_power_family", "building": "A楼", "bucket_or_date": "2026-06-09"}
    results = client.source_index_batch([query, dict(query)])

    assert len(captured_payloads) == 1
    assert len(captured_payloads[0]["queries"]) == 1
    assert len(results) == 2
    assert [item["index"] for item in results] == [0, 1]
    assert all(item["ok"] is True for item in results)
    assert all(item["entries"][0]["entry_id"] == "same" for item in results)


def test_system_alert_upload_adds_creator_fields_and_dedupes(tmp_path):
    class FakeClient:
        def __init__(self):
            self.created: list[dict[str, Any]] = []

        def list_fields(self, table_id: str, page_size: int = 200):
            return [
                {"field_name": "日志信息"},
                {"field_name": "创建者电脑名"},
                {"field_name": "创建者IP地址"},
            ]

        def batch_create_records(self, *, table_id: str, fields_list, batch_size: int):
            self.created.extend(fields_list)

    service = SystemAlertLogUploadService(
        config_getter=lambda: {},
        active_job_id_getter=lambda: "",
        emit_log=lambda _text: None,
        runtime_state_root=str(tmp_path),
    )
    fake_client = FakeClient()
    service._build_client = lambda: fake_client  # type: ignore[method-assign]
    service._host_name = "HOST-X"
    service._host_ip = "10.1.2.3"

    assert service._upload_entries([{"line": "same"}, {"line": "same"}, {"line": "other"}]) == 2
    assert fake_client.created == [
        {"日志信息": "same", "创建者电脑名": "HOST-X", "创建者IP地址": "10.1.2.3"},
        {"日志信息": "other", "创建者电脑名": "HOST-X", "创建者IP地址": "10.1.2.3"},
    ]
    assert service._upload_entries([{"line": "same"}]) == 0


def test_system_alert_upload_discards_stale_queue_on_start(tmp_path):
    service = SystemAlertLogUploadService(
        config_getter=lambda: {},
        active_job_id_getter=lambda: "",
        emit_log=lambda _text: None,
        runtime_state_root=str(tmp_path),
    )
    service._queue_path.write_text(
        json.dumps({"line": "old", "level": "error", "session_id": "old-session"}, ensure_ascii=False)
        + "\n",
        encoding="utf-8",
    )

    service._discard_stale_queue_entries()

    assert service._queue_path.read_text(encoding="utf-8") == ""
