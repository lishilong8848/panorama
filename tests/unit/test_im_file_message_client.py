from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from app.modules.feishu.service.im_file_message_client import FeishuImFileMessageClient


class _RequestClient(FeishuImFileMessageClient):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.requests: List[Dict[str, Any]] = []

    def _request_json_with_auth_retry(self, method: str, url: str, **kwargs: Any) -> Dict[str, Any]:  # type: ignore[override]
        self.requests.append({"method": method, "url": url, "kwargs": kwargs})
        if url == self.UPLOAD_IMAGE_URL:
            return {"code": 0, "data": {"image_key": "img_v2_abc"}}
        return {"code": 0, "data": {"message_id": "om_msg"}}


def _new_client() -> _RequestClient:
    return _RequestClient(
        app_id="app_id",
        app_secret="app_secret",
        timeout=30,
        request_retry_count=0,
        request_retry_interval_sec=0,
    )


def test_upload_image_uses_feishu_image_endpoint(tmp_path: Path) -> None:
    image = tmp_path / "capacity.png"
    image.write_bytes(b"\x89PNG\r\n\x1a\n")
    client = _new_client()

    result = client.upload_image(str(image))

    assert result["image_key"] == "img_v2_abc"
    request = client.requests[0]
    assert request["method"] == "POST"
    assert request["url"] == client.UPLOAD_IMAGE_URL
    assert request["kwargs"]["data"] == {"image_type": "message"}
    uploaded = request["kwargs"]["files"]["image"]
    assert uploaded[0] == "capacity.png"
    assert uploaded[2] == "image/png"


def test_send_image_message_sends_image_key_content() -> None:
    client = _new_client()

    result = client.send_image_message(
        receive_id="ou_abc",
        receive_id_type="open_id",
        image_key="img_v2_abc",
    )

    assert result["message_id"] == "om_msg"
    request = client.requests[0]
    assert request["method"] == "POST"
    assert request["url"] == client.SEND_MESSAGE_URL
    assert request["kwargs"]["params"] == {"receive_id_type": "open_id"}
    assert request["kwargs"]["payload"]["receive_id"] == "ou_abc"
    assert request["kwargs"]["payload"]["msg_type"] == "image"
    assert json.loads(request["kwargs"]["payload"]["content"]) == {"image_key": "img_v2_abc"}
    assert request["kwargs"]["content_type_json"] is True
