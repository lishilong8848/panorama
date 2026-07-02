from __future__ import annotations

import json
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Tuple


class InternalBridgeHttpError(RuntimeError):
    pass


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _infer_base_url_from_shared_root(shared_root: str, *, port: int) -> str:
    root = str(shared_root or "").strip()
    match = re.match(r"^\\\\([^\\\/]+)[\\\/]", root)
    if not match:
        return ""
    host = str(match.group(1) or "").strip()
    if not host:
        return ""
    return f"http://{host}:{int(port or 18765)}"


class InternalBridgeHttpClient:
    def __init__(
        self,
        *,
        base_url: str,
        auth_token: str = "",
        connect_timeout_sec: int = 3,
        read_timeout_sec: int = 15,
        max_attempts: int = 2,
    ) -> None:
        self.base_url = str(base_url or "").strip().rstrip("/")
        self.auth_token = ""
        self.connect_timeout_sec = max(1, int(connect_timeout_sec or 3))
        self.read_timeout_sec = max(self.connect_timeout_sec, int(read_timeout_sec or 15))
        self.max_attempts = max(1, int(max_attempts or 2))
        self._source_index_batch_lock = threading.Lock()
        self._source_index_batch_pending: List[Dict[str, Any]] = []
        self._source_index_batch_window_sec = 0.03

    @classmethod
    def from_runtime_config(cls, runtime_config: Dict[str, Any]) -> "InternalBridgeHttpClient | None":
        cfg = _dict(runtime_config.get("internal_bridge_http"))
        shared_bridge = _dict(runtime_config.get("shared_bridge"))
        port = int(cfg.get("port", 18765) or 18765)
        base_url = str(cfg.get("base_url", "") or "").strip()
        if not base_url:
            base_url = _infer_base_url_from_shared_root(str(shared_bridge.get("root_dir", "") or ""), port=port)
        if not base_url:
            return None
        configured_read_timeout = int(cfg.get("read_timeout_sec", cfg.get("request_timeout_sec", 15)) or 15)
        return cls(
            base_url=base_url,
            auth_token="",
            connect_timeout_sec=int(cfg.get("connect_timeout_sec", 3) or 3),
            read_timeout_sec=configured_read_timeout,
            max_attempts=int(cfg.get("max_attempts", 2) or 2),
        )

    @staticmethod
    def _is_retryable_error(exc: BaseException) -> bool:
        text = str(exc or "").lower()
        return any(
            token in text
            for token in (
                "timed out",
                "timeout",
                "urlopen error",
                "connection reset",
                "connection refused",
                "remote end closed",
                "temporarily unavailable",
                "正在处理其他请求",
                "正在排队",
                "稍后重试",
                "retry_after",
                "busy",
            )
        )

    @staticmethod
    def _raise_if_busy_payload(payload: Dict[str, Any], *, scope: str) -> None:
        if not isinstance(payload, dict):
            return
        status_text = str(payload.get("status", "") or "").strip().lower()
        queued = bool(payload.get("queued", False))
        if status_text != "busy" and not queued:
            return
        try:
            retry_after = int(payload.get("retry_after_sec", 60) or 60)
        except (TypeError, ValueError):
            retry_after = 60
        retry_after = max(1, retry_after)
        message = str(payload.get("message", "") or "").strip()
        if not message:
            message = f"内网端 {scope} 正在排队，请约 {retry_after} 秒后重试"
        raise InternalBridgeHttpError(f"{message}; retry_after_sec={retry_after}")

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: Dict[str, Any] | None = None,
        query: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        if not self.base_url:
            raise InternalBridgeHttpError("内网端 HTTP 桥接 base_url 未配置")
        path_text = str(path or "").strip()
        if not path_text.startswith("/"):
            path_text = "/" + path_text
        url = self.base_url + path_text
        if query:
            clean_query = {
                str(key): str(value)
                for key, value in query.items()
                if value is not None and str(value).strip() != ""
            }
            if clean_query:
                url += "?" + urllib.parse.urlencode(clean_query)
        data = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=str(method or "GET").upper(), headers=headers)
        last_exc: BaseException | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                with urllib.request.urlopen(req, timeout=self.read_timeout_sec) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")
                    if not raw:
                        return {}
                    parsed = json.loads(raw)
                    return parsed if isinstance(parsed, dict) else {"data": parsed}
            except urllib.error.HTTPError as exc:
                body = ""
                try:
                    body = exc.read().decode("utf-8", errors="replace")
                except Exception:
                    body = ""
                raise InternalBridgeHttpError(f"内网端 HTTP 返回 {exc.code}: {body or exc.reason}") from exc
            except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
                last_exc = exc
                if attempt >= self.max_attempts or not self._is_retryable_error(exc):
                    break
                time.sleep(min(1.5, 0.5 * attempt))
        raise InternalBridgeHttpError(f"内网端 HTTP 请求失败: {last_exc}") from last_exc

    def _request_bytes(
        self,
        method: str,
        path: str,
        *,
        query: Dict[str, Any] | None = None,
    ) -> Tuple[bytes, str, str]:
        if not self.base_url:
            raise InternalBridgeHttpError("内网端 HTTP 桥接 base_url 未配置")
        path_text = str(path or "").strip()
        if not path_text.startswith("/"):
            path_text = "/" + path_text
        url = self.base_url + path_text
        if query:
            clean_query = {
                str(key): str(value)
                for key, value in query.items()
                if value is not None and str(value).strip() != ""
            }
            if clean_query:
                url += "?" + urllib.parse.urlencode(clean_query)
        req = urllib.request.Request(
            url,
            method=str(method or "GET").upper(),
            headers={"Accept": "application/octet-stream"},
        )
        last_exc: BaseException | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                with urllib.request.urlopen(req, timeout=self.read_timeout_sec) as resp:
                    content = resp.read()
                    content_type = str(resp.headers.get("Content-Type", "") or "").strip()
                    disposition = str(resp.headers.get("Content-Disposition", "") or "").strip()
                    return content, self._filename_from_content_disposition(disposition), content_type
            except urllib.error.HTTPError as exc:
                body = ""
                try:
                    body = exc.read().decode("utf-8", errors="replace")
                except Exception:
                    body = ""
                raise InternalBridgeHttpError(f"内网端 HTTP 返回 {exc.code}: {body or exc.reason}") from exc
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last_exc = exc
                if attempt >= self.max_attempts or not self._is_retryable_error(exc):
                    break
                time.sleep(min(1.5, 0.5 * attempt))
        raise InternalBridgeHttpError(f"内网端 HTTP 请求失败: {last_exc}") from last_exc

    @staticmethod
    def _filename_from_content_disposition(value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        match = re.search(r"filename\*=UTF-8''([^;]+)", text, flags=re.IGNORECASE)
        if match:
            return urllib.parse.unquote(match.group(1).strip().strip('"'))
        match = re.search(r'filename="([^"]+)"', text, flags=re.IGNORECASE)
        if match:
            return urllib.parse.unquote(match.group(1).strip())
        match = re.search(r"filename=([^;]+)", text, flags=re.IGNORECASE)
        if match:
            return urllib.parse.unquote(match.group(1).strip().strip('"'))
        return ""

    def health(self) -> Dict[str, Any]:
        return self._request("GET", "/api/internal-bridge/health")

    def create_task(
        self,
        *,
        get_or_create_name: str,
        create_name: str,
        payload: Dict[str, Any],
        requested_by: str,
    ) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/api/internal-bridge/tasks",
            payload={
                "task_type": str(create_name or "").strip(),
                "get_or_create_name": str(get_or_create_name or "").strip(),
                "create_name": str(create_name or "").strip(),
                "payload": payload if isinstance(payload, dict) else {},
                "requested_by": str(requested_by or "").strip() or "external_http",
            },
        )

    def create_alarm_event_window_query_task(
        self,
        *,
        buildings: List[str],
        query_start: str,
        query_end: str,
        duty_date: str,
        duty_shift: str,
        requested_by: str = "handover_alarm_window",
    ) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/api/internal-bridge/alarm-events/window-query",
            payload={
                "buildings": [str(item or "").strip() for item in (buildings or []) if str(item or "").strip()],
                "query_start": str(query_start or "").strip(),
                "query_end": str(query_end or "").strip(),
                "duty_date": str(duty_date or "").strip(),
                "duty_shift": str(duty_shift or "").strip().lower(),
                "requested_by": str(requested_by or "").strip() or "handover_alarm_window",
            },
        )

    def get_task(self, task_id: str) -> Dict[str, Any] | None:
        task_text = str(task_id or "").strip()
        if not task_text:
            return None
        return self._request("GET", f"/api/internal-bridge/tasks/{urllib.parse.quote(task_text)}")

    def list_tasks(self, *, status: str = "", limit: int = 100) -> List[Dict[str, Any]]:
        payload = self._request(
            "GET",
            "/api/internal-bridge/tasks",
            query={"status": status, "limit": limit},
        )
        tasks = payload.get("tasks", [])
        return tasks if isinstance(tasks, list) else []

    def cancel_task(self, task_id: str) -> bool:
        task_text = str(task_id or "").strip()
        if not task_text:
            return False
        payload = self._request("POST", f"/api/internal-bridge/tasks/{urllib.parse.quote(task_text)}/cancel")
        return bool(payload.get("ok", False))

    def source_index(
        self,
        *,
        source_family: str = "",
        bucket_or_date: str = "",
        building: str = "",
        bucket_kind: str = "",
        duty_shift: str = "",
        status: str = "ready",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        results = self.source_index_batch(
            [
                {
                    "source_family": source_family,
                    "bucket_or_date": bucket_or_date,
                    "building": building,
                    "bucket_kind": bucket_kind,
                    "duty_shift": duty_shift,
                    "status": status,
                    "limit": limit,
                }
            ],
            default_limit=limit,
        )
        payload = results[0] if results else {}
        entries = payload.get("entries", []) if isinstance(payload, dict) else []
        return entries if isinstance(entries, list) else []

    def source_index_batch(self, queries: List[Dict[str, Any]], *, default_limit: int = 50) -> List[Dict[str, Any]]:
        request_item: Dict[str, Any] = {
            "queries": queries if isinstance(queries, list) else [],
            "default_limit": int(default_limit or 50),
            "event": threading.Event(),
            "results": None,
            "error": None,
        }
        with self._source_index_batch_lock:
            self._source_index_batch_pending.append(request_item)
            owns_flush = len(self._source_index_batch_pending) == 1
        if owns_flush:
            time.sleep(self._source_index_batch_window_sec)
            self._flush_source_index_batch_pending()
        wait_timeout = max(1.0, float(self.read_timeout_sec * self.max_attempts) + 5.0)
        if not request_item["event"].wait(wait_timeout):
            raise InternalBridgeHttpError("内网端 source-index batch 合并请求等待超时")
        error = request_item.get("error")
        if error is not None:
            raise error
        results = request_item.get("results", [])
        return results if isinstance(results, list) else []

    @staticmethod
    def _source_index_query_key(query: Dict[str, Any], *, default_limit: int) -> str:
        normalized = {
            "source_family": str(query.get("source_family", "") or "").strip().lower(),
            "bucket_or_date": str(query.get("bucket_or_date", "") or "").strip(),
            "building": str(query.get("building", "") or "").strip(),
            "bucket_kind": str(query.get("bucket_kind", "") or "").strip().lower(),
            "duty_shift": str(query.get("duty_shift", "") or "").strip().lower(),
            "status": str(query.get("status", "ready") or "ready").strip().lower(),
            "limit": int(query.get("limit", default_limit) or default_limit),
        }
        return json.dumps(normalized, ensure_ascii=False, sort_keys=True)

    def _flush_source_index_batch_pending(self) -> None:
        with self._source_index_batch_lock:
            pending = self._source_index_batch_pending
            self._source_index_batch_pending = []
        if not pending:
            return

        combined_queries: List[Dict[str, Any]] = []
        combined_index_by_key: Dict[str, int] = {}
        mappings: Dict[int, List[Tuple[Dict[str, Any], int]]] = {}
        combined_default_limit = 50
        for request_item in pending:
            request_queries = request_item.get("queries", [])
            default_limit = int(request_item.get("default_limit", 50) or 50)
            combined_default_limit = max(combined_default_limit, default_limit)
            prepared_results: List[Dict[str, Any] | None] = [None] * len(request_queries if isinstance(request_queries, list) else [])
            request_item["results"] = prepared_results
            for original_index, raw_query in enumerate(request_queries if isinstance(request_queries, list) else []):
                if not isinstance(raw_query, dict):
                    prepared_results[original_index] = {
                        "index": original_index,
                        "ok": False,
                        "entries": [],
                        "error": "query must be an object",
                    }
                    continue
                query = dict(raw_query)
                try:
                    query["limit"] = int(query.get("limit", default_limit) or default_limit)
                    key = self._source_index_query_key(query, default_limit=default_limit)
                except (TypeError, ValueError) as exc:
                    prepared_results[original_index] = {
                        "index": original_index,
                        "ok": False,
                        "entries": [],
                        "error": f"invalid source-index query: {exc}",
                    }
                    continue
                if key not in combined_index_by_key:
                    combined_index_by_key[key] = len(combined_queries)
                    combined_queries.append(query)
                combined_index = combined_index_by_key[key]
                mappings.setdefault(combined_index, []).append((request_item, original_index))

        if not combined_queries:
            for request_item in pending:
                results = request_item.get("results", [])
                request_item["results"] = [item for item in results if isinstance(item, dict)]
                request_item["event"].set()
            return

        try:
            payload = self._request(
                "POST",
                "/api/internal-bridge/source-index/batch",
                payload={
                    "queries": combined_queries,
                    "default_limit": combined_default_limit,
                },
            )
            self._raise_if_busy_payload(payload, scope="source-index/batch")
        except Exception as exc:  # noqa: BLE001
            for request_item in pending:
                request_item["error"] = exc
                request_item["event"].set()
            return
        raw_results = payload.get("results", [])
        results_by_combined_index: Dict[int, Dict[str, Any]] = {}
        for result in raw_results if isinstance(raw_results, list) else []:
            if not isinstance(result, dict):
                continue
            try:
                index = int(result.get("index", -1))
            except (TypeError, ValueError):
                index = -1
            if index >= 0:
                results_by_combined_index[index] = result
        for combined_index, request_mappings in mappings.items():
            result = results_by_combined_index.get(
                combined_index,
                {"ok": False, "entries": [], "error": "inner source-index batch result missing"},
            )
            for request_item, original_index in request_mappings:
                prepared_results = request_item.get("results", [])
                item = dict(result)
                item["index"] = original_index
                prepared_results[original_index] = item
        for request_item in pending:
            prepared_results = request_item.get("results", [])
            request_item["results"] = [
                item if isinstance(item, dict) else {"index": index, "ok": False, "entries": [], "error": "source-index result missing"}
                for index, item in enumerate(prepared_results if isinstance(prepared_results, list) else [])
            ]
            request_item["event"].set()

    def refresh_latest_source_cache(self, *, source_family: str, buildings: List[str]) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/api/internal-bridge/source-cache/refresh-latest",
            payload={
                "source_family": str(source_family or "").strip(),
                "buildings": [str(item or "").strip() for item in (buildings or []) if str(item or "").strip()],
            },
        )

    def list_alarm_rule_export_files(self, *, period: str = "", building: str = "") -> Dict[str, Any]:
        return self._request(
            "GET",
            "/api/internal-bridge/alarm-rule-export/files",
            query={
                "period": str(period or "").strip(),
                "building": str(building or "").strip(),
            },
        )

    def download_alarm_rule_export_file(
        self,
        *,
        period: str,
        building: str,
        file_name: str,
    ) -> Tuple[bytes, str, str]:
        return self._request_bytes(
            "GET",
            "/api/internal-bridge/alarm-rule-export/files/download",
            query={
                "period": str(period or "").strip(),
                "building": str(building or "").strip(),
                "file_name": str(file_name or "").strip(),
            },
        )

    def run_system_screenshot_capture(self, *, capture_date: str = "", force: bool = False) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/api/internal-bridge/system-screenshots/run",
            payload={
                "capture_date": str(capture_date or "").strip(),
                "force": bool(force),
            },
        )

    def list_system_screenshot_files(self, *, capture_date: str = "", target_key: str = "") -> Dict[str, Any]:
        return self._request(
            "GET",
            "/api/internal-bridge/system-screenshots/files",
            query={
                "capture_date": str(capture_date or "").strip(),
                "target_key": str(target_key or "").strip(),
            },
        )

    def download_system_screenshot_file(
        self,
        *,
        capture_date: str,
        target_key: str,
        file_name: str = "",
    ) -> Tuple[bytes, str, str]:
        return self._request_bytes(
            "GET",
            "/api/internal-bridge/system-screenshots/files/download",
            query={
                "capture_date": str(capture_date or "").strip(),
                "target_key": str(target_key or "").strip(),
                "file_name": str(file_name or "").strip(),
            },
        )
