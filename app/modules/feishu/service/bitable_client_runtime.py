from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests


class FeishuBitableClient:
    AUTH_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    CREATE_RECORD_URL = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    LIST_RECORD_URL = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    GET_RECORD_URL = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    LIST_FIELD_URL = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    BATCH_CREATE_RECORD_URL = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
    BATCH_DELETE_RECORD_URL = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete"
    UPLOAD_FILE_URL = "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all"

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        app_token: str,
        calc_table_id: str,
        attachment_table_id: str,
        date_field_mode: str = "timestamp",
        date_field_day: int = 1,
        date_tz_offset_hours: int = 8,
        timeout: int = 30,
        request_retry_count: int = 3,
        request_retry_interval_sec: float = 1.0,
        *,
        date_text_to_timestamp_ms_fn: Callable[..., int],
        canonical_metric_name_fn: Callable[[Any], str],
        dimension_mapping: Dict[str, tuple[str, str, str]],
    ) -> None:
        self.app_id = app_id
        self.app_secret = app_secret
        self.app_token = app_token
        self.calc_table_id = calc_table_id
        self.attachment_table_id = attachment_table_id
        self.date_field_mode = date_field_mode
        self.date_field_day = date_field_day
        self.date_tz_offset_hours = date_tz_offset_hours
        self.timeout = timeout
        self.request_retry_count = max(0, int(request_retry_count))
        self.request_retry_interval_sec = max(0.0, float(request_retry_interval_sec))
        self._tenant_access_token: Optional[str] = None
        self._date_text_to_timestamp_ms_fn = date_text_to_timestamp_ms_fn
        self._canonical_metric_name_fn = canonical_metric_name_fn
        self._dimension_mapping = dict(dimension_mapping)

    def _to_feishu_date(self, date_text: str) -> Any:
        if self.date_field_mode == "text":
            return date_text
        if self.date_field_mode == "timestamp":
            return self._date_text_to_timestamp_ms_fn(
                date_text=date_text,
                default_day=self.date_field_day,
                tz_offset_hours=self.date_tz_offset_hours,
            )
        raise ValueError(f"不支持的日期字段模式: {self.date_field_mode}")

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
        text = str(code).strip()
        return text in {"99991661", "99991663", "99991668"}

    @staticmethod
    def _is_retryable_api_error(body: Dict[str, Any]) -> bool:
        code_text = str(body.get("code", "")).strip()
        if code_text in {"1255002"}:
            return True
        msg = str(body.get("msg", "")).lower()
        return "something went wrong" in msg

    def _request_with_retry(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        total_attempts = self.request_retry_count + 1
        req_kwargs = dict(kwargs)
        timeout = req_kwargs.pop("timeout", self.timeout)
        last_exc: Optional[Exception] = None

        for attempt in range(1, total_attempts + 1):
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    timeout=timeout,
                    **req_kwargs,
                )
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

    def refresh_token(self, force: bool = True) -> str:
        if self._tenant_access_token and not force:
            return self._tenant_access_token

        try:
            response = self._request_with_retry(
                "POST",
                self.AUTH_URL,
                json={"app_id": self.app_id, "app_secret": self.app_secret},
                headers={"Content-Type": "application/json; charset=utf-8", "Connection": "close"},
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"飞书获取token失败: {exc}") from exc

        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RuntimeError(f"飞书获取token失败: {data}")
        token = data["tenant_access_token"]
        self._tenant_access_token = token
        return token

    def _request_json_with_auth_retry(
        self,
        method: str,
        url: str,
        *,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        files: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
        content_type_json: bool = False,
    ) -> Dict[str, Any]:
        api_attempts = self.request_retry_count + 1
        last_error: Optional[str] = None
        for api_attempt in range(1, api_attempts + 1):
            should_retry_api = False
            for auth_attempt in range(2):
                if not self._tenant_access_token:
                    self.refresh_token(force=False)

                headers: Dict[str, str] = {
                    "Authorization": f"Bearer {self._tenant_access_token}",
                }
                if content_type_json:
                    headers["Content-Type"] = "application/json; charset=utf-8"

                req_kwargs: Dict[str, Any] = {"headers": headers}
                if payload is not None:
                    req_kwargs["json"] = payload
                if params is not None:
                    req_kwargs["params"] = params
                if data is not None:
                    req_kwargs["data"] = data
                if files is not None:
                    req_kwargs["files"] = files
                if timeout is not None:
                    req_kwargs["timeout"] = timeout

                response = self._request_with_retry(method, url, **req_kwargs)
                try:
                    response.raise_for_status()
                except requests.HTTPError:
                    if response.status_code in {401, 403} and auth_attempt == 0:
                        self.refresh_token(force=True)
                        continue
                    raise

                try:
                    body = response.json()
                except Exception as exc:  # noqa: BLE001
                    raise RuntimeError(f"飞书接口返回非JSON: status={response.status_code}") from exc

                if body.get("code") == 0:
                    return body

                if auth_attempt == 0 and self._is_token_invalid_code(body.get("code")):
                    self.refresh_token(force=True)
                    continue

                if self._is_retryable_api_error(body) and api_attempt < api_attempts:
                    last_error = f"飞书接口调用失败(将重试): {body}"
                    should_retry_api = True
                    if self.request_retry_interval_sec > 0:
                        time.sleep(self.request_retry_interval_sec * api_attempt)
                    break

                raise RuntimeError(f"飞书接口调用失败: {body}")

            if should_retry_api:
                continue
            raise RuntimeError("飞书接口调用失败: 鉴权重试后仍失败")

        if last_error:
            raise RuntimeError(last_error.replace("(将重试)", "(重试后仍失败)"))
        raise RuntimeError("飞书接口调用失败: 重试后仍失败")

    def _post_json(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request_json_with_auth_retry(
            "POST",
            url,
            payload=payload,
            content_type_json=True,
        )

    def _get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._request_json_with_auth_retry(
            "GET",
            url,
            params=params or {},
        )

    def batch_create_records(
        self,
        table_id: str,
        fields_list: List[Dict[str, Any]],
        batch_size: int = 200,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> List[Dict[str, Any]]:
        if not fields_list:
            return []
        url = self.BATCH_CREATE_RECORD_URL.format(app_token=self.app_token, table_id=table_id)
        responses: List[Dict[str, Any]] = []
        total = len(fields_list)
        uploaded = 0
        for i in range(0, len(fields_list), batch_size):
            chunk = fields_list[i : i + batch_size]
            payload = {"records": [{"fields": fields} for fields in chunk]}
            responses.append(self._post_json(url, payload))
            uploaded += len(chunk)
            if callable(progress_callback):
                progress_callback(uploaded, total)
        return responses

    def list_records(
        self,
        table_id: str,
        page_size: int = 500,
        max_records: int = 0,
        *,
        view_id: str = "",
        filter_formula: str = "",
    ) -> List[Dict[str, Any]]:
        if page_size <= 0:
            raise ValueError("page_size 必须大于0")
        max_count = int(max_records or 0)
        if max_count < 0:
            max_count = 0

        url = self.LIST_RECORD_URL.format(app_token=self.app_token, table_id=table_id)
        records: List[Dict[str, Any]] = []
        page_token = ""
        while True:
            params: Dict[str, Any] = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            if str(view_id).strip():
                params["view_id"] = str(view_id).strip()
            if str(filter_formula).strip():
                params["filter"] = str(filter_formula).strip()

            data = self._get_json(url, params=params)
            payload = data.get("data") if isinstance(data, dict) else {}
            items = payload.get("items") if isinstance(payload, dict) else []
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        records.append(item)
                        if max_count > 0 and len(records) >= max_count:
                            return records[:max_count]

            has_more = bool(payload.get("has_more")) if isinstance(payload, dict) else False
            if not has_more:
                break
            page_token = str(payload.get("page_token", "")).strip() if isinstance(payload, dict) else ""
            if not page_token:
                break
        return records

    def list_record_ids(self, table_id: str, page_size: int = 500) -> List[str]:
        if page_size <= 0:
            raise ValueError("page_size 必须大于0")
        url = self.LIST_RECORD_URL.format(app_token=self.app_token, table_id=table_id)
        record_ids: List[str] = []
        page_token = ""
        while True:
            params: Dict[str, Any] = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            data = self._get_json(url, params=params)
            payload = data.get("data") if isinstance(data, dict) else {}
            items = payload.get("items") if isinstance(payload, dict) else []
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    record_id = str(item.get("record_id", "")).strip()
                    if record_id:
                        record_ids.append(record_id)

            has_more = bool(payload.get("has_more")) if isinstance(payload, dict) else False
            if not has_more:
                break
            page_token = str(payload.get("page_token", "")).strip() if isinstance(payload, dict) else ""
            if not page_token:
                break
        return record_ids

    def get_record_by_id(self, table_id: str, record_id: str) -> Dict[str, Any]:
        table_text = str(table_id or "").strip()
        record_text = str(record_id or "").strip()
        if not table_text:
            raise ValueError("table_id 不能为空")
        if not record_text:
            raise ValueError("record_id 不能为空")
        url = self.GET_RECORD_URL.format(
            app_token=self.app_token,
            table_id=table_text,
            record_id=record_text,
        )
        data = self._get_json(url, params={})
        payload = data.get("data") if isinstance(data, dict) else {}
        item = payload.get("record") if isinstance(payload, dict) else {}
        return item if isinstance(item, dict) else {}

    def batch_delete_records(self, table_id: str, record_ids: List[str], batch_size: int = 500) -> int:
        normalized = [str(record_id).strip() for record_id in record_ids if str(record_id).strip()]
        if not normalized:
            return 0
        if batch_size <= 0:
            raise ValueError("batch_size 必须大于0")

        url = self.BATCH_DELETE_RECORD_URL.format(app_token=self.app_token, table_id=table_id)
        deleted = 0
        for i in range(0, len(normalized), batch_size):
            chunk = normalized[i : i + batch_size]
            payload = {"records": chunk}
            self._post_json(url, payload)
            deleted += len(chunk)
        return deleted

    def clear_table(self, table_id: str, list_page_size: int = 500, delete_batch_size: int = 500) -> int:
        record_ids = self.list_record_ids(table_id=table_id, page_size=list_page_size)
        if not record_ids:
            return 0
        return self.batch_delete_records(table_id=table_id, record_ids=record_ids, batch_size=delete_batch_size)

    def list_fields(self, table_id: str, page_size: int = 500) -> List[Dict[str, Any]]:
        if page_size <= 0:
            raise ValueError("page_size 必须大于0")
        url = self.LIST_FIELD_URL.format(app_token=self.app_token, table_id=table_id)
        fields: List[Dict[str, Any]] = []
        page_token = ""
        while True:
            params: Dict[str, Any] = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            data = self._get_json(url, params=params)
            payload = data.get("data") if isinstance(data, dict) else {}
            items = payload.get("items") if isinstance(payload, dict) else []
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        fields.append(item)
            has_more = bool(payload.get("has_more")) if isinstance(payload, dict) else False
            if not has_more:
                break
            page_token = str(payload.get("page_token", "")).strip() if isinstance(payload, dict) else ""
            if not page_token:
                break
        return fields

    def upload_attachment(self, file_path: str) -> str:
        if not self._tenant_access_token:
            self.refresh_token()
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(file_path)
        with path.open("rb") as f:
            content = f.read()
        return self.upload_attachment_bytes(
            file_name=path.name,
            content=content,
            mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    def upload_attachment_bytes(
        self,
        file_name: str,
        content: bytes,
        mime_type: str = "application/octet-stream",
        timeout: Optional[int] = None,
    ) -> str:
        if not file_name:
            raise ValueError("file_name 不能为空")
        if not content:
            raise ValueError("content 不能为空")

        data = {
            "file_name": file_name,
            "parent_type": "bitable_file",
            "parent_node": self.app_token,
            "size": str(len(content)),
        }
        files = {"file": (file_name, content, mime_type or "application/octet-stream")}
        result = self._request_json_with_auth_retry(
            "POST",
            self.UPLOAD_FILE_URL,
            data=data,
            files=files,
            timeout=timeout if timeout is not None else self.timeout,
        )
        return result["data"]["file_token"]

    def upload_calc_records(
        self,
        records: List[Any],
        skip_zero_records: bool = False,
        date_override: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        fields_list: List[Dict[str, Any]] = []
        for record in records:
            if skip_zero_records and record.value == 0:
                continue
            canonical_name = self._canonical_metric_name_fn(record.item_name)
            mapped = self._dimension_mapping.get(canonical_name)
            if mapped:
                type_name, category_name, item_name = mapped
            else:
                type_name, category_name, item_name = record.type_name, record.category_name, record.item_name
            date_value_text = date_override or record.month
            fields_list.append(
                record.to_feishu_fields(
                    date_value=self._to_feishu_date(date_value_text),
                    type_name=type_name,
                    category_name=category_name,
                    item_name=item_name,
                )
            )
        return self.batch_create_records(self.calc_table_id, fields_list)

    def upload_attachment_record(
        self,
        report_type: str,
        building: str,
        date_text: str,
        attachment_tokens: List[str],
    ) -> Dict[str, Any]:
        fields = {
            "类型": report_type,
            "楼栋": building,
            "日期": self._to_feishu_date(date_text),
            "附件": [{"file_token": token} for token in attachment_tokens],
        }
        url = self.CREATE_RECORD_URL.format(app_token=self.app_token, table_id=self.attachment_table_id)
        return self._post_json(url, {"fields": fields})
