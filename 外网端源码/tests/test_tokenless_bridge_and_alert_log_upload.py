from __future__ import annotations

import json
from typing import Any

from app.modules.internal_bridge_http.service.client import InternalBridgeHttpClient
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
