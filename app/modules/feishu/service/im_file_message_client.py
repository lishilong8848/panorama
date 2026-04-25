from __future__ import annotations

import json
import mimetypes
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from app.modules.feishu.service.feishu_auth_resolver import resolve_feishu_auth_settings


class FeishuImFileMessageClient:
    AUTH_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    UPLOAD_FILE_URL = "https://open.feishu.cn/open-apis/im/v1/files"
    UPLOAD_IMAGE_URL = "https://open.feishu.cn/open-apis/im/v1/images"
    SEND_MESSAGE_URL = "https://open.feishu.cn/open-apis/im/v1/messages"

    def __init__(
        self,
        *,
        app_id: str,
        app_secret: str,
        timeout: int = 30,
        request_retry_count: int = 3,
        request_retry_interval_sec: float = 1.0,
    ) -> None:
        auth = resolve_feishu_auth_settings(
            {
                "app_id": app_id,
                "app_secret": app_secret,
                "timeout": timeout,
                "request_retry_count": request_retry_count,
                "request_retry_interval_sec": request_retry_interval_sec,
            }
        )
        self.app_id = str(auth.get("app_id", "") or "").strip()
        self.app_secret = str(auth.get("app_secret", "") or "").strip()
        self.timeout = max(1, int(auth.get("timeout", 30) or 30))
        self.request_retry_count = max(0, int(auth.get("request_retry_count", 0) or 0))
        self.request_retry_interval_sec = max(0.0, float(auth.get("request_retry_interval_sec", 0.0) or 0.0))
        self._tenant_access_token: Optional[str] = None
        if not self.app_id or not self.app_secret:
            raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")

    @staticmethod
    def _is_retryable_exception(exc: Exception) -> bool:
        return isinstance(
            exc,
            (
                requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ),
        )

    @staticmethod
    def _is_token_invalid_code(code: Any) -> bool:
        text = str(code or "").strip()
        return text in {"99991661", "99991663", "99991668"}

    @staticmethod
    def _extract_http_error_detail(response: requests.Response) -> str:
        try:
            payload = response.json()
        except Exception:  # noqa: BLE001
            payload = None
        if isinstance(payload, dict):
            return json.dumps(payload, ensure_ascii=False)
        text = str(getattr(response, "text", "") or "").strip()
        return text

    def _request_with_retry(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        total_attempts = self.request_retry_count + 1
        last_exc: Optional[Exception] = None
        timeout = kwargs.pop("timeout", self.timeout)
        for attempt in range(1, total_attempts + 1):
            try:
                response = requests.request(method=method, url=url, timeout=timeout, **kwargs)
                if response.status_code >= 500 and attempt < total_attempts:
                    if self.request_retry_interval_sec > 0:
                        time.sleep(self.request_retry_interval_sec * attempt)
                    continue
                return response
            except Exception as exc:  # noqa: BLE001
                if not self._is_retryable_exception(exc) or attempt >= total_attempts:
                    raise
                last_exc = exc
                if self.request_retry_interval_sec > 0:
                    time.sleep(self.request_retry_interval_sec * attempt)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("飞书请求失败: 未知错误")

    def refresh_token(self, force: bool = False) -> str:
        if self._tenant_access_token and not force:
            return self._tenant_access_token
        response = self._request_with_retry(
            "POST",
            self.AUTH_URL,
            json={"app_id": self.app_id, "app_secret": self.app_secret},
            headers={"Content-Type": "application/json; charset=utf-8", "Connection": "close"},
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0:
            raise RuntimeError(f"飞书获取 token 失败: {payload}")
        token = str(payload.get("tenant_access_token", "") or "").strip()
        if not token:
            raise RuntimeError("飞书获取 token 失败: tenant_access_token 为空")
        self._tenant_access_token = token
        return token

    def _request_json_with_auth_retry(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        files: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        content_type_json: bool = False,
    ) -> Dict[str, Any]:
        for auth_attempt in range(2):
            token = self.refresh_token(force=auth_attempt > 0)
            merged_headers: Dict[str, str] = {"Authorization": f"Bearer {token}"}
            if headers:
                merged_headers.update(headers)
            if content_type_json:
                merged_headers["Content-Type"] = "application/json; charset=utf-8"
            response = self._request_with_retry(
                method,
                url,
                params=params,
                json=payload,
                data=data,
                files=files,
                headers=merged_headers,
            )
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                if response.status_code in {401, 403} and auth_attempt == 0:
                    self._tenant_access_token = None
                    continue
                detail = self._extract_http_error_detail(response)
                if detail:
                    raise RuntimeError(f"飞书接口调用失败: status={response.status_code}, detail={detail}") from exc
                raise
            body = response.json()
            if body.get("code") == 0:
                return body
            if auth_attempt == 0 and self._is_token_invalid_code(body.get("code")):
                self._tenant_access_token = None
                continue
            raise RuntimeError(f"飞书接口调用失败: {body}")
        raise RuntimeError("飞书接口调用失败: 鉴权重试后仍失败")

    def upload_file(self, file_path: str) -> Dict[str, Any]:
        path = Path(str(file_path or "").strip())
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"待发送文件不存在: {path}")
        mime_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        with path.open("rb") as handle:
            body = self._request_json_with_auth_retry(
                "POST",
                self.UPLOAD_FILE_URL,
                data={"file_type": "stream", "file_name": path.name},
                files={"file": (path.name, handle, mime_type)},
            )
        data = body.get("data", {}) if isinstance(body.get("data", {}), dict) else {}
        file_key = str(data.get("file_key", "") or "").strip()
        if not file_key:
            raise RuntimeError(f"飞书文件上传失败: {body}")
        return {"file_key": file_key, "raw": body}

    def upload_image(self, image_path: str) -> Dict[str, Any]:
        path = Path(str(image_path or "").strip())
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"待发送图片不存在: {path}")
        mime_type = mimetypes.guess_type(path.name)[0] or "image/png"
        with path.open("rb") as handle:
            body = self._request_json_with_auth_retry(
                "POST",
                self.UPLOAD_IMAGE_URL,
                data={"image_type": "message"},
                files={"image": (path.name, handle, mime_type)},
            )
        data = body.get("data", {}) if isinstance(body.get("data", {}), dict) else {}
        image_key = str(data.get("image_key", "") or "").strip()
        if not image_key:
            raise RuntimeError(f"飞书图片上传失败: {body}")
        return {"image_key": image_key, "raw": body}

    def send_file_message(
        self,
        *,
        receive_id: str,
        receive_id_type: str,
        file_key: str,
    ) -> Dict[str, Any]:
        receive_id_text = str(receive_id or "").strip()
        receive_id_type_text = str(receive_id_type or "").strip() or "user_id"
        file_key_text = str(file_key or "").strip()
        if not receive_id_text:
            raise ValueError("receive_id 不能为空")
        if not file_key_text:
            raise ValueError("file_key 不能为空")
        body = self._request_json_with_auth_retry(
            "POST",
            self.SEND_MESSAGE_URL,
            params={"receive_id_type": receive_id_type_text},
            payload={
                "receive_id": receive_id_text,
                "msg_type": "file",
                "content": json.dumps({"file_key": file_key_text}, ensure_ascii=False),
            },
            content_type_json=True,
        )
        data = body.get("data", {}) if isinstance(body.get("data", {}), dict) else {}
        return {
            "message_id": str(data.get("message_id", "") or "").strip(),
            "raw": body,
        }

    def send_image_message(
        self,
        *,
        receive_id: str,
        receive_id_type: str,
        image_key: str,
    ) -> Dict[str, Any]:
        receive_id_text = str(receive_id or "").strip()
        receive_id_type_text = str(receive_id_type or "").strip() or "open_id"
        image_key_text = str(image_key or "").strip()
        if not receive_id_text:
            raise ValueError("receive_id 不能为空")
        if not image_key_text:
            raise ValueError("image_key 不能为空")
        body = self._request_json_with_auth_retry(
            "POST",
            self.SEND_MESSAGE_URL,
            params={"receive_id_type": receive_id_type_text},
            payload={
                "receive_id": receive_id_text,
                "msg_type": "image",
                "content": json.dumps({"image_key": image_key_text}, ensure_ascii=False),
            },
            content_type_json=True,
        )
        data = body.get("data", {}) if isinstance(body.get("data", {}), dict) else {}
        return {
            "message_id": str(data.get("message_id", "") or "").strip(),
            "raw": body,
        }

    def send_text_message(
        self,
        *,
        receive_id: str,
        receive_id_type: str,
        text: str,
    ) -> Dict[str, Any]:
        receive_id_text = str(receive_id or "").strip()
        receive_id_type_text = str(receive_id_type or "").strip() or "open_id"
        text_value = str(text or "").strip()
        if not receive_id_text:
            raise ValueError("receive_id 不能为空")
        if not text_value:
            raise ValueError("text 不能为空")
        body = self._request_json_with_auth_retry(
            "POST",
            self.SEND_MESSAGE_URL,
            params={"receive_id_type": receive_id_type_text},
            payload={
                "receive_id": receive_id_text,
                "msg_type": "text",
                "content": json.dumps({"text": text_value}, ensure_ascii=False),
            },
            content_type_json=True,
        )
        data = body.get("data", {}) if isinstance(body.get("data", {}), dict) else {}
        return {
            "message_id": str(data.get("message_id", "") or "").strip(),
            "raw": body,
        }
