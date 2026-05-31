from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List


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
        read_timeout_sec: int = 5,
    ) -> None:
        self.base_url = str(base_url or "").strip().rstrip("/")
        self.auth_token = str(auth_token or "").strip()
        self.connect_timeout_sec = max(1, int(connect_timeout_sec or 3))
        self.read_timeout_sec = max(self.connect_timeout_sec, int(read_timeout_sec or 5))

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
        configured_read_timeout = int(cfg.get("read_timeout_sec", cfg.get("request_timeout_sec", 5)) or 5)
        return cls(
            base_url=base_url,
            auth_token=str(cfg.get("auth_token", "") or "").strip(),
            connect_timeout_sec=int(cfg.get("connect_timeout_sec", 3) or 3),
            read_timeout_sec=configured_read_timeout,
        )

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
        if self.auth_token:
            headers["X-Bridge-Token"] = self.auth_token
        req = urllib.request.Request(url, data=data, method=str(method or "GET").upper(), headers=headers)
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
            raise InternalBridgeHttpError(f"内网端 HTTP 请求失败: {exc}") from exc

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
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        payload = self._request(
            "GET",
            "/api/internal-bridge/source-index",
            query={
                "source_family": source_family,
                "bucket_or_date": bucket_or_date,
                "building": building,
                "bucket_kind": bucket_kind,
                "duty_shift": duty_shift,
                "limit": limit,
            },
        )
        entries = payload.get("entries", [])
        return entries if isinstance(entries, list) else []

    def source_index_batch(self, queries: List[Dict[str, Any]], *, default_limit: int = 50) -> List[Dict[str, Any]]:
        payload = self._request(
            "POST",
            "/api/internal-bridge/source-index/batch",
            payload={
                "queries": queries if isinstance(queries, list) else [],
                "default_limit": int(default_limit or 50),
            },
        )
        results = payload.get("results", [])
        return results if isinstance(results, list) else []

    def refresh_latest_source_cache(self, *, source_family: str, buildings: List[str]) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/api/internal-bridge/source-cache/refresh-latest",
            payload={
                "source_family": str(source_family or "").strip(),
                "buildings": [str(item or "").strip() for item in (buildings or []) if str(item or "").strip()],
            },
        )
