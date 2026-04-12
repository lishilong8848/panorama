from __future__ import annotations

import time
import urllib.parse
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from app.modules.feishu.service.feishu_auth_resolver import resolve_feishu_auth_settings


class FeishuSheetsClientRuntime:
    AUTH_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    WIKI_GET_NODE_URL = "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node"
    WIKI_LIST_NODES_URL = "https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes"
    WIKI_CREATE_NODE_URL = "https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes"
    WIKI_MOVE_DOC_URL = "https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes/move_docs_to_wiki"
    SPREADSHEET_CREATE_URL = "https://open.feishu.cn/open-apis/sheets/v3/spreadsheets"
    DRIVE_COPY_URL = "https://open.feishu.cn/open-apis/drive/v1/files/{file_token}/copy"
    SHEETS_QUERY_URL = "https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
    SHEETS_BATCH_UPDATE_URL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/sheets_batch_update"
    VALUES_BATCH_UPDATE_URL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_batch_update"
    STYLE_UPDATE_URL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/style"
    STYLES_BATCH_UPDATE_URL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/styles_batch_update"
    DIMENSION_RANGE_URL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/dimension_range"
    MERGE_CELLS_URL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/merge_cells"
    UNMERGE_CELLS_URL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/unmerge_cells"

    def __init__(
        self,
        *,
        app_id: str,
        app_secret: str,
        timeout: int = 20,
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
        self.timeout = max(1, int(auth.get("timeout", 20) or 20))
        self.request_retry_count = max(0, int(auth.get("request_retry_count", 0) or 0))
        self.request_retry_interval_sec = max(0.0, float(auth.get("request_retry_interval_sec", 0.0) or 0.0))
        self._tenant_access_token: Optional[str] = None

        if not self.app_id or not self.app_secret:
            raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")

    @staticmethod
    def extract_node_token_from_url(url: str) -> str:
        raw = str(url or "").strip()
        if not raw:
            raise ValueError("root_wiki_url 不能为空")
        parsed = urllib.parse.urlparse(raw)
        parts = [part for part in str(parsed.path or "").split("/") if part]
        if len(parts) >= 2 and parts[0] == "wiki":
            return str(parts[-1]).strip()
        raise ValueError(f"无法从 URL 中提取 wiki node token: {url}")

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
        return str(code or "").strip() in {"99991661", "99991663", "99991668"}

    @staticmethod
    def _is_retryable_api_error(body: Dict[str, Any]) -> bool:
        code = str(body.get("code", "")).strip()
        msg = str(body.get("msg", "")).strip().lower()
        return (
            code in {"90217", "1254290", "1255002"}
            or "something went wrong" in msg
            or "too many requests" in msg
            or "too many request" in msg
        )

    def _request_with_retry(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        total_attempts = self.request_retry_count + 1
        timeout = kwargs.pop("timeout", self.timeout)
        last_exc: Optional[Exception] = None
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
        try:
            response = self._request_with_retry(
                "POST",
                self.AUTH_URL,
                headers={"Content-Type": "application/json; charset=utf-8", "Connection": "close"},
                json={"app_id": self.app_id, "app_secret": self.app_secret},
            )
            response.raise_for_status()
            data = response.json()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"飞书获取 token 失败: {exc}") from exc
        if data.get("code") != 0 or not str(data.get("tenant_access_token", "")).strip():
            raise RuntimeError(f"飞书获取 token 失败: {data}")
        self._tenant_access_token = str(data["tenant_access_token"]).strip()
        return self._tenant_access_token

    def _request_json_with_auth_retry(
        self,
        method: str,
        url: str,
        *,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        attempts = self.request_retry_count + 1
        last_error = ""
        last_error_detail = ""
        for api_attempt in range(1, attempts + 1):
            should_retry = False
            for auth_attempt in range(2):
                if not self._tenant_access_token:
                    self.refresh_token(force=False)
                headers = {
                    "Authorization": f"Bearer {self._tenant_access_token}",
                    "Content-Type": "application/json; charset=utf-8",
                }
                response = self._request_with_retry(
                    method,
                    url,
                    headers=headers,
                    json=payload,
                    params=params or {},
                    timeout=timeout or self.timeout,
                )
                try:
                    response.raise_for_status()
                except requests.HTTPError as exc:
                    if response.status_code in {401, 403} and auth_attempt == 0:
                        self.refresh_token(force=True)
                        continue
                    error_body: Dict[str, Any]
                    try:
                        parsed_body = response.json()
                        error_body = parsed_body if isinstance(parsed_body, dict) else {"body": parsed_body}
                    except Exception:  # noqa: BLE001
                        error_body = {
                            "http_status": response.status_code,
                            "text": str(response.text or "").strip()[:500],
                        }
                    raise RuntimeError(
                        self._format_api_error_message(
                            method=method,
                            url=url,
                            payload=payload,
                            params=params,
                            body=error_body,
                        )
                    ) from exc
                try:
                    body = response.json()
                except Exception as exc:  # noqa: BLE001
                    raise RuntimeError(f"飞书接口返回非 JSON: status={response.status_code}") from exc
                if body.get("code") == 0:
                    return body
                if auth_attempt == 0 and self._is_token_invalid_code(body.get("code")):
                    self.refresh_token(force=True)
                    continue
                if self._is_retryable_api_error(body) and api_attempt < attempts:
                    should_retry = True
                    last_error = str(body)
                    last_error_detail = self._format_api_error_message(
                        method=method,
                        url=url,
                        payload=payload,
                        params=params,
                        body=body,
                    )
                    if self.request_retry_interval_sec > 0:
                        time.sleep(self.request_retry_interval_sec * api_attempt)
                    break
                raise RuntimeError(
                    self._format_api_error_message(
                        method=method,
                        url=url,
                        payload=payload,
                        params=params,
                        body=body,
                    )
                )
            if should_retry:
                continue
            raise RuntimeError("飞书接口调用失败: 鉴权重试后仍失败")
        raise RuntimeError(last_error_detail or f"飞书接口调用失败: {last_error or '重试后仍失败'}")

    @staticmethod
    def _safe_path_from_url(url: str) -> str:
        parsed = urllib.parse.urlparse(str(url or "").strip())
        return parsed.path or str(url or "").strip()

    @staticmethod
    def _summarize_values_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        ranges = payload.get("valueRanges", [])
        if not isinstance(ranges, list):
            return {"valueRanges": "invalid"}
        summary = []
        for item in ranges:
            if not isinstance(item, dict):
                continue
            values = item.get("values", [])
            rows = len(values) if isinstance(values, list) else 0
            cols = 0
            if rows and isinstance(values[0], list):
                cols = len(values[0])
            summary.append(
                {
                    "range": str(item.get("range", "")).strip(),
                    "rows": rows,
                    "cols": cols,
                }
            )
        return {"valueRanges": summary}

    @staticmethod
    def _summarize_payload(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        if "valueRanges" in payload:
            return FeishuSheetsClientRuntime._summarize_values_payload(payload)
        if "appendStyle" in payload:
            append = payload.get("appendStyle", {})
            if isinstance(append, dict):
                return {
                    "appendStyle": {
                        "range": str(append.get("range", "")).strip(),
                        "style_keys": sorted((append.get("style") or {}).keys()) if isinstance(append.get("style"), dict) else [],
                    }
                }
        if "data" in payload and isinstance(payload.get("data"), list):
            return {"data_len": len(payload.get("data", []))}
        if "dimension" in payload:
            dim = payload.get("dimension", {})
            props = payload.get("dimensionProperties", {})
            return {
                "dimension": {
                    "sheetId": str((dim or {}).get("sheetId", "")).strip(),
                    "majorDimension": str((dim or {}).get("majorDimension", "")).strip(),
                    "startIndex": (dim or {}).get("startIndex"),
                    "endIndex": (dim or {}).get("endIndex"),
                    "length": (dim or {}).get("length"),
                },
                "dimensionProperties": {
                    "fixedSize": (props or {}).get("fixedSize"),
                },
            }
        if "range" in payload or "mergeType" in payload:
            return {
                "range": str(payload.get("range", "")).strip(),
                "mergeType": str(payload.get("mergeType", "")).strip(),
            }
        return {key: payload.get(key) for key in sorted(payload.keys())}

    def _format_api_error_message(
        self,
        *,
        method: str,
        url: str,
        payload: Optional[Dict[str, Any]],
        params: Optional[Dict[str, Any]],
        body: Dict[str, Any],
    ) -> str:
        detail: Dict[str, Any] = {
            "method": str(method or "").upper(),
            "path": self._safe_path_from_url(url),
            "payload": self._summarize_payload(payload),
        }
        if isinstance(params, dict) and params:
            detail["params"] = params
        return f"飞书接口调用失败: {detail}, body={body}"

    @staticmethod
    def _node_summary(node: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "node_token": str(node.get("node_token", "") or "").strip(),
            "obj_type": str(node.get("obj_type", "") or "").strip(),
            "obj_token": str(node.get("obj_token", "") or "").strip(),
            "space_id": str(node.get("space_id", "") or "").strip(),
            "title": str(node.get("title", "") or "").strip(),
        }

    def get_wiki_node_info(self, node_token: str) -> Dict[str, Any]:
        body = self._request_json_with_auth_retry(
            "GET",
            self.WIKI_GET_NODE_URL,
            params={"token": str(node_token or "").strip()},
        )
        node = (body.get("data") or {}).get("node") or {}
        if not isinstance(node, dict) or not str(node.get("node_token", "")).strip():
            raise RuntimeError("飞书 wiki 节点信息缺失")
        return self._node_summary(node)

    def get_wiki_subnodes(self, space_id: str, parent_node_token: str) -> List[Dict[str, Any]]:
        output: List[Dict[str, Any]] = []
        page_token = ""
        while True:
            params: Dict[str, Any] = {"parent_node_token": parent_node_token, "page_size": 50}
            if page_token:
                params["page_token"] = page_token
            body = self._request_json_with_auth_retry(
                "GET",
                self.WIKI_LIST_NODES_URL.format(space_id=space_id),
                params=params,
            )
            data = body.get("data") or {}
            items = data.get("items") or []
            if isinstance(items, list):
                output.extend(self._node_summary(item) for item in items if isinstance(item, dict))
            if not bool(data.get("has_more", False)):
                break
            page_token = str(data.get("page_token", "") or data.get("next_page_token", "")).strip()
            if not page_token:
                break
        return output

    def create_wiki_node(self, space_id: str, parent_node_token: str, title: str, obj_type: str = "docx") -> Dict[str, Any]:
        body = self._request_json_with_auth_retry(
            "POST",
            self.WIKI_CREATE_NODE_URL.format(space_id=space_id),
            payload={
                "obj_type": obj_type,
                "parent_node_token": parent_node_token,
                "title": str(title or "").strip(),
                "node_type": "origin",
            },
        )
        node = (body.get("data") or {}).get("node") or {}
        if not isinstance(node, dict) or not str(node.get("node_token", "")).strip():
            raise RuntimeError("飞书创建 wiki 节点失败: 响应缺少 node")
        return self._node_summary(node)

    def move_doc_to_wiki(self, space_id: str, obj_token: str, obj_type: str, parent_wiki_token: str) -> Dict[str, Any]:
        body = self._request_json_with_auth_retry(
            "POST",
            self.WIKI_MOVE_DOC_URL.format(space_id=space_id),
            payload={
                "obj_type": obj_type,
                "obj_token": obj_token,
                "parent_wiki_token": parent_wiki_token,
            },
        )
        data = body.get("data") or {}
        if not isinstance(data, dict):
            raise RuntimeError("飞书移动文档到 wiki 失败: 响应缺少 data")
        return data

    def create_spreadsheet(self, title: str, folder_token: str = "") -> Dict[str, Any]:
        body = self._request_json_with_auth_retry(
            "POST",
            self.SPREADSHEET_CREATE_URL,
            payload={"title": str(title or "").strip(), "folder_token": str(folder_token or "").strip()},
        )
        spreadsheet = (body.get("data") or {}).get("spreadsheet") or {}
        if not isinstance(spreadsheet, dict):
            raise RuntimeError("飞书创建云表失败: 响应缺少 spreadsheet")
        return {
            "spreadsheet_token": str(spreadsheet.get("spreadsheet_token", "") or "").strip(),
            "title": str(spreadsheet.get("title", "") or "").strip(),
            "url": str(spreadsheet.get("url", "") or "").strip(),
        }

    def copy_spreadsheet(self, source_obj_token: str, new_name: str) -> Dict[str, Any]:
        body = self._request_json_with_auth_retry(
            "POST",
            self.DRIVE_COPY_URL.format(file_token=source_obj_token),
            payload={"name": str(new_name or "").strip(), "type": "sheet", "folder_token": ""},
        )
        file_info = (body.get("data") or {}).get("file") or {}
        if not isinstance(file_info, dict) or not str(file_info.get("token", "")).strip():
            raise RuntimeError("飞书复制云表失败: 响应缺少 file.token")
        return {
            "token": str(file_info.get("token", "") or "").strip(),
            "name": str(file_info.get("name", "") or "").strip(),
        }

    def find_or_create_year_node(self, space_id: str, parent_node_token: str, year: int) -> Dict[str, Any]:
        year_text = f"{int(year)}年度"
        for node in self.get_wiki_subnodes(space_id, parent_node_token):
            if year_text in str(node.get("title", "")):
                return node
        return self.create_wiki_node(space_id, parent_node_token, year_text, "docx")

    def find_or_create_month_node(self, space_id: str, parent_node_token: str, month: int) -> Dict[str, Any]:
        month_text = f"{int(month)}月"
        for node in self.get_wiki_subnodes(space_id, parent_node_token):
            if month_text in str(node.get("title", "")):
                return node
        return self.create_wiki_node(space_id, parent_node_token, month_text, "docx")

    def find_or_create_date_spreadsheet(
        self,
        *,
        root_wiki_url: str,
        template_node_token: str,
        spreadsheet_title: str,
        duty_date: str,
    ) -> Dict[str, Any]:
        try:
            duty_dt = datetime.strptime(str(duty_date or "").strip(), "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"无效 duty_date: {duty_date}") from exc

        root_token = self.extract_node_token_from_url(root_wiki_url)
        root_node = self.get_wiki_node_info(root_token)
        space_id = str(root_node.get("space_id", "") or "").strip()
        if not space_id:
            raise RuntimeError("飞书根 wiki 节点缺少 space_id")

        year_node = self.find_or_create_year_node(space_id, str(root_node.get("node_token", "")), duty_dt.year)
        month_node = self.find_or_create_month_node(space_id, str(year_node.get("node_token", "")), duty_dt.month)

        month_nodes = self.get_wiki_subnodes(space_id, str(month_node.get("node_token", "")))
        for node in month_nodes:
            if str(node.get("obj_type", "")) == "sheet" and str(node.get("title", "")).strip() == spreadsheet_title:
                return {
                    "spreadsheet_token": str(node.get("obj_token", "") or "").strip(),
                    "title": str(node.get("title", "") or "").strip(),
                    "url": f"https://vnet.feishu.cn/wiki/{str(node.get('node_token', '') or '').strip()}",
                    "space_id": space_id,
                    "wiki_node_token": str(node.get("node_token", "") or "").strip(),
                }

        template_node = self.get_wiki_node_info(template_node_token)
        template_obj_token = str(template_node.get("obj_token", "") or "").strip()
        if not template_obj_token:
            raise RuntimeError("飞书模板节点缺少 obj_token")

        copied = self.copy_spreadsheet(template_obj_token, spreadsheet_title)
        new_spreadsheet_token = str(copied.get("token", "") or "").strip()
        move_result = self.move_doc_to_wiki(
            space_id,
            new_spreadsheet_token,
            "sheet",
            str(month_node.get("node_token", "") or "").strip(),
        )
        wiki_node_token = str(move_result.get("wiki_token", "") or "").strip()
        if not wiki_node_token:
            time.sleep(min(2.0, max(0.5, self.request_retry_interval_sec or 0.5)))
            month_nodes = self.get_wiki_subnodes(space_id, str(month_node.get("node_token", "")))
            for node in month_nodes:
                if str(node.get("obj_type", "")) == "sheet" and str(node.get("obj_token", "")).strip() == new_spreadsheet_token:
                    wiki_node_token = str(node.get("node_token", "") or "").strip()
                    break

        url = (
            f"https://vnet.feishu.cn/wiki/{wiki_node_token}"
            if wiki_node_token
            else f"https://vnet.feishu.cn/sheets/{new_spreadsheet_token}"
        )
        return {
            "spreadsheet_token": new_spreadsheet_token,
            "title": spreadsheet_title,
            "url": url,
            "space_id": space_id,
            "wiki_node_token": wiki_node_token,
        }

    def query_sheets(
        self,
        spreadsheet_token: str,
        *,
        sheet_cache: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        force_refresh: bool = False,
    ) -> List[Dict[str, Any]]:
        cache_key = str(spreadsheet_token or "").strip()
        if isinstance(sheet_cache, dict) and cache_key and cache_key in sheet_cache and not force_refresh:
            return [dict(item) for item in sheet_cache[cache_key]]
        body = self._request_json_with_auth_retry(
            "GET",
            self.SHEETS_QUERY_URL.format(spreadsheet_token=spreadsheet_token),
        )
        data = body.get("data") or {}
        sheets = data.get("sheets") or data.get("items") or []
        output: List[Dict[str, Any]] = []
        if isinstance(sheets, list):
            for item in sheets:
                if not isinstance(item, dict):
                    continue
                output.append(
                    {
                        "sheet_id": str(item.get("sheet_id", item.get("sheetId", "")) or "").strip(),
                        "title": str(item.get("title", "") or "").strip(),
                        "index": int(item.get("index", 0) or 0),
                        "row_count": int(
                            (
                                item.get("grid_properties", {}) if isinstance(item.get("grid_properties", {}), dict) else {}
                            ).get("row_count", item.get("rowCount", 0))
                            or 0
                        ),
                        "column_count": int(
                            (
                                item.get("grid_properties", {}) if isinstance(item.get("grid_properties", {}), dict) else {}
                            ).get("column_count", item.get("columnCount", 0))
                            or 0
                        ),
                        "merges": self._normalize_sheet_merges(item.get("merges", [])),
                    }
                )
        if isinstance(sheet_cache, dict) and cache_key:
            sheet_cache[cache_key] = [dict(item) for item in output]
        return output

    @staticmethod
    def _normalize_sheet_merges(raw: Any) -> List[Dict[str, int]]:
        merges: List[Dict[str, int]] = []
        if not isinstance(raw, list):
            return merges
        for item in raw:
            if not isinstance(item, dict):
                continue
            try:
                start_row = int(item.get("start_row_index", item.get("startRowIndex", 0)) or 0)
                end_row = int(item.get("end_row_index", item.get("endRowIndex", 0)) or 0)
                start_col = int(item.get("start_column_index", item.get("startColumnIndex", 0)) or 0)
                end_col = int(item.get("end_column_index", item.get("endColumnIndex", 0)) or 0)
            except (TypeError, ValueError):
                continue
            if end_row < start_row or end_col < start_col:
                continue
            if end_row == start_row:
                end_row += 1
            if end_col == start_col:
                end_col += 1
            merges.append(
                {
                    "start_row_index": start_row,
                    "end_row_index": end_row,
                    "start_column_index": start_col,
                    "end_column_index": end_col,
                }
            )
        return merges

    def batch_update_sheet_requests(self, spreadsheet_token: str, requests_payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not requests_payload:
            return {"replies": []}
        body = self._request_json_with_auth_retry(
            "POST",
            self.SHEETS_BATCH_UPDATE_URL.format(spreadsheet_token=spreadsheet_token),
            payload={"requests": requests_payload},
        )
        return body.get("data") or {}

    def delete_sheet(
        self,
        spreadsheet_token: str,
        sheet_id: str,
        *,
        sheet_cache: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ) -> None:
        self.batch_update_sheet_requests(
            spreadsheet_token,
            [{"deleteSheet": {"sheetId": str(sheet_id or "").strip()}}],
        )
        cache_key = str(spreadsheet_token or "").strip()
        if isinstance(sheet_cache, dict) and cache_key in sheet_cache:
            sheet_cache[cache_key] = [
                dict(item)
                for item in sheet_cache[cache_key]
                if str(item.get("sheet_id", "")).strip() != str(sheet_id or "").strip()
            ]

    def add_sheet(
        self,
        spreadsheet_token: str,
        title: str,
        index: int = 0,
        *,
        sheet_cache: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ) -> Dict[str, Any]:
        self.batch_update_sheet_requests(
            spreadsheet_token,
            [{"addSheet": {"properties": {"title": str(title or "").strip(), "index": int(index or 0)}}}],
        )
        for sheet in self.query_sheets(spreadsheet_token, sheet_cache=sheet_cache, force_refresh=True):
            if str(sheet.get("title", "")).strip() == str(title or "").strip():
                return sheet
        raise RuntimeError(f"飞书创建目标 sheet 后未找到: {title}")

    def copy_sheet(
        self,
        spreadsheet_token: str,
        *,
        source_sheet_id: str,
        title: str,
        sheet_cache: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ) -> Dict[str, Any]:
        normalized_source_sheet_id = str(source_sheet_id or "").strip()
        normalized_title = str(title or "").strip()
        if not normalized_source_sheet_id:
            raise ValueError("source_sheet_id 不能为空")
        if not normalized_title:
            raise ValueError("sheet title 不能为空")
        data = self.batch_update_sheet_requests(
            spreadsheet_token,
            [
                {
                    "copySheet": {
                        "source": {"sheetId": normalized_source_sheet_id},
                        "destination": {"title": normalized_title},
                    }
                }
            ],
        )
        copied_sheet_id = ""
        replies = data.get("replies") or []
        if isinstance(replies, list) and replies:
            copy_reply = (replies[0] or {}).get("copySheet", {})
            if isinstance(copy_reply, dict):
                properties = copy_reply.get("properties", {})
                if isinstance(properties, dict):
                    copied_sheet_id = str(properties.get("sheetId", "") or "").strip()
        sheets = self.query_sheets(spreadsheet_token, sheet_cache=sheet_cache, force_refresh=True)
        if copied_sheet_id:
            for sheet in sheets:
                if str(sheet.get("sheet_id", "")).strip() == copied_sheet_id:
                    return sheet
        matched = [sheet for sheet in sheets if str(sheet.get("title", "")).strip() == normalized_title]
        if matched:
            matched.sort(key=lambda item: int(item.get("index", 0) or 0))
            return matched[-1]
        raise RuntimeError(f"飞书复制目标 sheet 后未找到: {title}")

    def rename_sheet(
        self,
        spreadsheet_token: str,
        *,
        sheet_id: str,
        title: str,
        sheet_cache: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ) -> Dict[str, Any]:
        return self.rename_and_move_sheet(
            spreadsheet_token,
            sheet_id=sheet_id,
            title=title,
            index=None,
            sheet_cache=sheet_cache,
        )

    def move_sheet(
        self,
        spreadsheet_token: str,
        *,
        sheet_id: str,
        index: int,
        sheet_cache: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ) -> Dict[str, Any]:
        return self.rename_and_move_sheet(
            spreadsheet_token,
            sheet_id=sheet_id,
            title=None,
            index=index,
            sheet_cache=sheet_cache,
        )

    def rename_and_move_sheet(
        self,
        spreadsheet_token: str,
        *,
        sheet_id: str,
        title: Optional[str] = None,
        index: Optional[int] = None,
        sheet_cache: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ) -> Dict[str, Any]:
        normalized_sheet_id = str(sheet_id or "").strip()
        if not normalized_sheet_id:
            raise ValueError("sheet_id 不能为空")
        properties: Dict[str, Any] = {"sheetId": normalized_sheet_id}
        if title is not None:
            normalized_title = str(title or "").strip()
            if not normalized_title:
                raise ValueError("sheet title 不能为空")
            properties["title"] = normalized_title
        if index is not None:
            properties["index"] = int(index or 0)
        self.batch_update_sheet_requests(
            spreadsheet_token,
            [{"updateSheet": {"properties": properties}}],
        )
        sheets = self.query_sheets(spreadsheet_token, sheet_cache=sheet_cache, force_refresh=True)
        for sheet in sheets:
            if str(sheet.get("sheet_id", "")).strip() == normalized_sheet_id:
                return sheet
        raise RuntimeError(f"飞书更新目标 sheet 后未找到: {sheet_id}")

    def get_or_create_named_sheet(
        self,
        spreadsheet_token: str,
        title: str,
        index: int = 0,
        *,
        sheet_cache: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ) -> Dict[str, Any]:
        normalized_title = str(title or "").strip()
        if not normalized_title:
            raise ValueError("sheet title 不能为空")
        matched = [
            sheet
            for sheet in self.query_sheets(spreadsheet_token, sheet_cache=sheet_cache)
            if str(sheet.get("title", "")).strip() == normalized_title
        ]
        if matched:
            matched.sort(key=lambda item: int(item.get("index", 0) or 0))
            return matched[0]
        return self.add_sheet(spreadsheet_token, normalized_title, index=index, sheet_cache=sheet_cache)

    def dedupe_named_sheets(
        self,
        spreadsheet_token: str,
        title: str,
        *,
        sheet_cache: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ) -> Dict[str, Any]:
        normalized_title = str(title or "").strip()
        if not normalized_title:
            raise ValueError("sheet title 不能为空")
        matched = [
            sheet
            for sheet in self.query_sheets(spreadsheet_token, sheet_cache=sheet_cache)
            if str(sheet.get("title", "")).strip() == normalized_title
        ]
        if not matched:
            return {}
        matched.sort(key=lambda item: int(item.get("index", 0) or 0))
        keep = matched[0]
        for duplicate in matched[1:]:
            duplicate_id = str(duplicate.get("sheet_id", "")).strip()
            if duplicate_id:
                self.delete_sheet(spreadsheet_token, duplicate_id, sheet_cache=sheet_cache)
        return keep

    def batch_update_values(self, spreadsheet_token: str, value_ranges: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not value_ranges:
            return {}
        body = self._request_json_with_auth_retry(
            "POST",
            self.VALUES_BATCH_UPDATE_URL.format(spreadsheet_token=spreadsheet_token),
            payload={"valueRanges": value_ranges},
        )
        return body.get("data") or {}

    def batch_clear_values(self, spreadsheet_token: str, range_name: str, rows: int, cols: int) -> Dict[str, Any]:
        row_count = max(1, int(rows or 1))
        col_count = max(1, int(cols or 1))
        blank_values = [["" for _ in range(col_count)] for _ in range(row_count)]
        return self.batch_update_values(
            spreadsheet_token,
            [{"range": str(range_name or "").strip(), "values": blank_values}],
        )

    def apply_style_matrix(self, spreadsheet_token: str, range_name: str, styles: List[List[Dict[str, Any] | None]]) -> Dict[str, Any]:
        body = self._request_json_with_auth_retry(
            "PUT",
            self.STYLE_UPDATE_URL.format(spreadsheet_token=spreadsheet_token),
            payload={"appendStyle": {"style": {"range": range_name, "styles": styles}}},
        )
        return body.get("data") or {}

    def apply_blank_style_matrix(self, spreadsheet_token: str, range_name: str, rows: int, cols: int) -> Dict[str, Any]:
        row_count = max(1, int(rows or 1))
        col_count = max(1, int(cols or 1))
        blank_styles = [[{} for _ in range(col_count)] for _ in range(row_count)]
        return self.apply_style_matrix(spreadsheet_token, range_name, blank_styles)

    def update_dimension_range(
        self,
        spreadsheet_token: str,
        *,
        sheet_id: str,
        major_dimension: str,
        start_index: int,
        end_index: int,
        pixel_size: int,
    ) -> Dict[str, Any]:
        body = self._request_json_with_auth_retry(
            "PUT",
            self.DIMENSION_RANGE_URL.format(spreadsheet_token=spreadsheet_token),
            payload={
                "dimension": {
                    "sheetId": str(sheet_id or "").strip(),
                    "majorDimension": str(major_dimension or "ROWS").strip().upper(),
                    "startIndex": int(start_index) + 1,
                    "endIndex": max(int(start_index) + 1, int(end_index)),
                },
                "dimensionProperties": {
                    "fixedSize": int(pixel_size),
                },
            },
        )
        return body.get("data") or {}

    def add_dimension(
        self,
        spreadsheet_token: str,
        *,
        sheet_id: str,
        major_dimension: str,
        length: int,
    ) -> Dict[str, Any]:
        add_length = max(0, int(length or 0))
        if add_length <= 0:
            return {"addCount": 0, "majorDimension": str(major_dimension or "ROWS").strip().upper()}
        body = self._request_json_with_auth_retry(
            "POST",
            self.DIMENSION_RANGE_URL.format(spreadsheet_token=spreadsheet_token),
            payload={
                "dimension": {
                    "sheetId": str(sheet_id or "").strip(),
                    "majorDimension": str(major_dimension or "ROWS").strip().upper(),
                    "length": add_length,
                }
            },
        )
        return body.get("data") or {}

    def delete_dimension(
        self,
        spreadsheet_token: str,
        *,
        sheet_id: str,
        major_dimension: str,
        start_index: int,
        end_index: int,
    ) -> Dict[str, Any]:
        normalized_sheet_id = str(sheet_id or "").strip()
        normalized_dimension = str(major_dimension or "ROWS").strip().upper()
        normalized_start_index = max(0, int(start_index or 0))
        normalized_end_index = max(normalized_start_index, int(end_index or 0))
        if normalized_end_index <= normalized_start_index:
            return {"delCount": 0, "majorDimension": normalized_dimension}
        body = self._request_json_with_auth_retry(
            "DELETE",
            self.DIMENSION_RANGE_URL.format(spreadsheet_token=spreadsheet_token),
            payload={
                "dimension": {
                    "sheetId": normalized_sheet_id,
                    "majorDimension": normalized_dimension,
                    "startIndex": normalized_start_index + 1,
                    "endIndex": max(normalized_start_index + 1, normalized_end_index),
                }
            },
        )
        return body.get("data") or {}

    def batch_merge_cells(self, spreadsheet_token: str, sheet_id: str, merges: List[Dict[str, int]]) -> Dict[str, Any]:
        if not merges:
            return {"responses": []}
        responses = []
        for item in merges:
            body = self._request_json_with_auth_retry(
                "POST",
                self.MERGE_CELLS_URL.format(spreadsheet_token=spreadsheet_token),
                payload={
                    "range": self.build_sheet_id_range(
                        sheet_id=sheet_id,
                        start_row_index=int(item["start_row_index"]),
                        end_row_index=int(item["end_row_index"]),
                        start_column_index=int(item["start_column_index"]),
                        end_column_index=int(item["end_column_index"]),
                    ),
                    "mergeType": "MERGE_ALL",
                },
            )
            responses.append(body.get("data") or {})
        return {"responses": responses}

    def batch_unmerge_cells(self, spreadsheet_token: str, sheet_id: str, merges: List[Dict[str, int]]) -> Dict[str, Any]:
        if not merges:
            return {"responses": []}
        responses = []
        for item in merges:
            body = self._request_json_with_auth_retry(
                "POST",
                self.UNMERGE_CELLS_URL.format(spreadsheet_token=spreadsheet_token),
                payload={
                    "range": self.build_sheet_id_range(
                        sheet_id=sheet_id,
                        start_row_index=int(item["start_row_index"]),
                        end_row_index=int(item["end_row_index"]),
                        start_column_index=int(item["start_column_index"]),
                        end_column_index=int(item["end_column_index"]),
                    )
                },
            )
            responses.append(body.get("data") or {})
        return {"responses": responses}

    @staticmethod
    def build_sheet_id_range(
        *,
        sheet_id: str,
        start_row_index: int,
        end_row_index: int,
        start_column_index: int,
        end_column_index: int,
    ) -> str:
        start_col = FeishuSheetsClientRuntime._column_index_to_letter(start_column_index)
        end_col = FeishuSheetsClientRuntime._column_index_to_letter(max(start_column_index, end_column_index - 1))
        start_row = int(start_row_index) + 1
        end_row = max(start_row, int(end_row_index))
        return f"{str(sheet_id or '').strip()}!{start_col}{start_row}:{end_col}{end_row}"

    @staticmethod
    def _column_index_to_letter(column_index: int) -> str:
        number = int(column_index) + 1
        if number <= 0:
            raise ValueError("column_index must be >= 0")
        letters = []
        while number:
            number, remainder = divmod(number - 1, 26)
            letters.append(chr(65 + remainder))
        return "".join(reversed(letters))
