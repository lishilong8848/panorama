from __future__ import annotations

import re
import threading
import time
import urllib.parse
from datetime import datetime
from typing import Any, Dict

from app.modules.feishu.service.sheets_client_runtime import FeishuSheetsClientRuntime


def build_bitable_url(app_token: str, table_id: str) -> str:
    app_token_text = str(app_token or "").strip()
    table_id_text = str(table_id or "").strip()
    if not app_token_text or not table_id_text:
        return ""
    return f"https://vnet.feishu.cn/base/{app_token_text}?table={table_id_text}"


def build_wiki_bitable_url(node_token: str, table_id: str) -> str:
    node_token_text = str(node_token or "").strip()
    table_id_text = str(table_id or "").strip()
    if not node_token_text or not table_id_text:
        return ""
    return f"https://vnet.feishu.cn/wiki/{node_token_text}?table={table_id_text}"


def extract_bitable_table_id_from_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    candidates = [raw]
    try:
        parsed = urllib.parse.urlparse(raw)
        candidates.append(str(parsed.query or ""))
        candidates.append(str(parsed.fragment or ""))
    except Exception:
        pass
    for text in candidates:
        match = re.search(r"(?:^|[?&#])(?:table|table_id)=([^&#]+)", text, re.IGNORECASE)
        if match:
            return urllib.parse.unquote(str(match.group(1) or "").strip())
    return ""


def extract_bitable_app_token_from_base_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""

    def _scan_path(text: str) -> str:
        try:
            parsed = urllib.parse.urlparse(text)
            path = parsed.path or text
        except Exception:
            path = text
        parts = [part for part in str(path or "").split("/") if part]
        for idx, part in enumerate(parts):
            if part.lower() == "base" and idx + 1 < len(parts):
                return str(parts[idx + 1] or "").strip()
        return ""

    app_token = _scan_path(raw)
    if app_token:
        return app_token
    try:
        fragment = urllib.parse.urlparse(raw).fragment or ""
    except Exception:
        fragment = ""
    return _scan_path(fragment)


def is_wiki_url(url: str) -> bool:
    raw = str(url or "").strip()
    return bool(raw) and bool(re.search(r"/wiki/([^/?#]+)", raw, re.IGNORECASE))


def has_bitable_target_input(target: Dict[str, Any] | None) -> bool:
    current = target if isinstance(target, dict) else {}
    app_token = str(current.get("app_token", "") or "").strip()
    table_id = str(current.get("table_id", "") or "").strip()
    base_url = str(current.get("base_url", "") or "").strip()
    wiki_url = str(current.get("wiki_url", "") or "").strip()
    if app_token and table_id:
        return True
    if base_url:
        if extract_bitable_app_token_from_base_url(base_url) and (
            extract_bitable_table_id_from_url(base_url) or table_id
        ):
            return True
    if wiki_url and (extract_bitable_table_id_from_url(wiki_url) or table_id):
        return True
    return False


_TOKEN_PAIR_PREVIEW_CACHE: dict[tuple[str, str, str, str], dict[str, Any]] = {}
_TOKEN_PAIR_PREVIEW_CACHE_LOCK = threading.Lock()
_TOKEN_PAIR_PREVIEW_CACHE_VERSION = "prefer_wiki_display_v2"
_SUCCESS_TARGET_KINDS = {"base_token_pair", "wiki_token_pair"}
_RETRYABLE_PROBE_CODES = {"90217", "1254290", "1255002", "99991661", "99991663", "99991668"}
_PROBE_ERROR_HINTS = (
    "timeout",
    "timed out",
    "proxy",
    "ssl",
    "connection",
    "too many requests",
    "too many request",
    "permission",
    "forbidden",
    "unauthorized",
    "auth",
    "tenant_access_token",
    "internal error",
    "something went wrong",
    "temporary",
    "temporarily",
    "限流",
    "超时",
    "连接",
    "鉴权",
    "权限",
    "暂时",
)
_INVALID_HINTS = (
    "not found",
    "not exist",
    "invalid",
    "illegal",
    "不存在",
    "未找到",
    "鏃犳晥",
    "闈炴硶",
    "bitable",
    "wiki",
    "table",
    "obj_token",
    "obj type",
)


class BitableTargetResolver:
    BITABLE_LIST_FIELDS_URL = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"

    def __init__(
        self,
        *,
        app_id: str,
        app_secret: str,
        timeout: int = 20,
        request_retry_count: int = 3,
        request_retry_interval_sec: float = 1.0,
    ) -> None:
        self.app_id = str(app_id or "").strip()
        self.app_secret = str(app_secret or "").strip()
        self.timeout = max(1, int(timeout or 20))
        self.request_retry_count = max(0, int(request_retry_count or 0))
        self.request_retry_interval_sec = max(0.0, float(request_retry_interval_sec or 0.0))

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _preview_ttl_sec(target_kind: str) -> int:
        normalized = str(target_kind or "").strip().lower()
        if normalized in _SUCCESS_TARGET_KINDS:
            return 24 * 60 * 60
        if normalized == "invalid":
            return 10 * 60
        return 60

    def _new_preview(
        self,
        *,
        configured_app_token: str,
        operation_app_token: str,
        table_id: str,
        target_kind: str,
        display_url: str = "",
        wiki_node_token: str = "",
        message: str = "",
    ) -> Dict[str, str]:
        configured_text = str(configured_app_token or "").strip()
        operation_text = str(operation_app_token or "").strip()
        table_text = str(table_id or "").strip()
        target_kind_text = str(target_kind or "").strip()
        display_url_text = str(display_url or "").strip()
        wiki_node_text = str(wiki_node_token or "").strip()
        message_text = str(message or "").strip()
        return {
            "configured_app_token": configured_text,
            "operation_app_token": operation_text,
            "app_token": operation_text,
            "table_id": table_text,
            "target_kind": target_kind_text,
            "resolved_from": target_kind_text,
            "display_url": display_url_text,
            "bitable_url": display_url_text,
            "source_url": display_url_text,
            "wiki_node_token": wiki_node_text,
            "wiki_obj_type": "",
            "message": message_text,
            "resolved_at": self._now_text(),
        }

    def _new_wiki_client(self) -> FeishuSheetsClientRuntime:
        if not self.app_id or not self.app_secret:
            raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")
        return FeishuSheetsClientRuntime(
            app_id=self.app_id,
            app_secret=self.app_secret,
            timeout=self.timeout,
            request_retry_count=self.request_retry_count,
            request_retry_interval_sec=self.request_retry_interval_sec,
        )

    @staticmethod
    def _combine_error_text(status_code: int, body: Dict[str, Any]) -> str:
        code_text = str(body.get("code", "") or "").strip()
        msg_text = str(body.get("msg", "") or "").strip()
        pieces = []
        if status_code:
            pieces.append(f"http={status_code}")
        if code_text:
            pieces.append(f"code={code_text}")
        if msg_text:
            pieces.append(msg_text)
        return " ".join(pieces).strip()

    @staticmethod
    def _classify_probe_failure(*, status_code: int, body: Dict[str, Any]) -> str:
        code_text = str(body.get("code", "") or "").strip()
        message = BitableTargetResolver._combine_error_text(status_code, body).lower()
        if code_text in _RETRYABLE_PROBE_CODES:
            return "probe_error"
        if any(hint in message for hint in _PROBE_ERROR_HINTS):
            return "probe_error"
        if status_code in {401, 403, 429} or status_code >= 500:
            return "probe_error"
        if status_code in {400, 404}:
            return "invalid"
        if any(hint in message for hint in _INVALID_HINTS):
            return "invalid"
        return "probe_error"

    def _request_json_probe(
        self,
        client: FeishuSheetsClientRuntime,
        *,
        url: str,
        params: Dict[str, Any],
        context_label: str,
    ) -> Dict[str, Any]:
        last_message = f"{context_label} 探测失败"
        for auth_attempt in range(2):
            try:
                token = client.refresh_token(force=auth_attempt > 0)
                response = client._request_with_retry(  # noqa: SLF001
                    "GET",
                    url,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json; charset=utf-8",
                    },
                    params=params,
                    timeout=self.timeout,
                )
            except Exception as exc:  # noqa: BLE001
                return {
                    "ok": False,
                    "kind": "probe_error",
                    "message": f"{context_label} 探测失败: {exc}",
                }

            status_code = int(getattr(response, "status_code", 0) or 0)
            try:
                body = response.json()
            except Exception:
                body = {}
            body = body if isinstance(body, dict) else {}

            if status_code in {401, 403} and auth_attempt == 0:
                last_message = f"{context_label} 鉴权失败"
                continue

            if status_code >= 400:
                message = self._combine_error_text(status_code, body) or f"http={status_code}"
                return {
                    "ok": False,
                    "kind": self._classify_probe_failure(status_code=status_code, body=body),
                    "message": f"{context_label} 探测失败: {message}",
                }

            if body.get("code") == 0:
                return {"ok": True, "body": body}

            message = self._combine_error_text(status_code, body) or last_message
            return {
                "ok": False,
                "kind": self._classify_probe_failure(status_code=status_code, body=body),
                "message": f"{context_label} 探测失败: {message}",
            }

        return {"ok": False, "kind": "probe_error", "message": last_message}

    def _probe_bitable_fields(
        self,
        client: FeishuSheetsClientRuntime,
        *,
        app_token: str,
        table_id: str,
        context_label: str,
    ) -> Dict[str, Any]:
        url = self.BITABLE_LIST_FIELDS_URL.format(
            app_token=urllib.parse.quote(str(app_token or "").strip(), safe=""),
            table_id=urllib.parse.quote(str(table_id or "").strip(), safe=""),
        )
        return self._request_json_probe(
            client,
            url=url,
            params={"page_size": 1},
            context_label=context_label,
        )

    def _probe_wiki_node(
        self,
        client: FeishuSheetsClientRuntime,
        *,
        node_token: str,
    ) -> Dict[str, Any]:
        result = self._request_json_probe(
            client,
            url=client.WIKI_GET_NODE_URL,
            params={"token": str(node_token or "").strip()},
            context_label="Wiki 节点",
        )
        if not result.get("ok"):
            return result
        body = result.get("body", {})
        node = (body.get("data") or {}).get("node") or {}
        if not isinstance(node, dict):
            return {
                "ok": False,
                "kind": "invalid",
                "message": "Wiki 节点无效: 未返回节点信息",
            }
        node_summary = {
            "node_token": str(node.get("node_token", "") or "").strip(),
            "obj_type": str(node.get("obj_type", "") or "").strip(),
            "obj_token": str(node.get("obj_token", "") or "").strip(),
            "space_id": str(node.get("space_id", "") or "").strip(),
            "title": str(node.get("title", "") or "").strip(),
        }
        if not node_summary["node_token"]:
            return {
                "ok": False,
                "kind": "invalid",
                "message": "Wiki 节点无效: node_token 为空",
            }
        return {"ok": True, "node": node_summary}

    def resolve_token_pair_preview(
        self,
        *,
        configured_app_token: str,
        table_id: str,
        force_refresh: bool = False,
    ) -> Dict[str, str]:
        configured_text = str(configured_app_token or "").strip()
        table_text = str(table_id or "").strip()
        if not configured_text or not table_text:
            return self._new_preview(
                configured_app_token=configured_text,
                operation_app_token="",
                table_id=table_text,
                target_kind="invalid",
                message="璇峰厛濉啓婀跨悆娓╁害鐩爣鐨?App Token 鍜?Table ID",
            )

        cache_key = (
            configured_text,
            table_text,
            self.app_id,
            f"{self.app_secret}:{_TOKEN_PAIR_PREVIEW_CACHE_VERSION}",
        )
        now = time.time()
        if not force_refresh:
            with _TOKEN_PAIR_PREVIEW_CACHE_LOCK:
                entry = _TOKEN_PAIR_PREVIEW_CACHE.get(cache_key)
                if entry and float(entry.get("expires_at", 0.0) or 0.0) > now:
                    return dict(entry.get("preview", {}))

        try:
            client = self._new_wiki_client()
        except Exception as exc:  # noqa: BLE001
            preview = self._new_preview(
                configured_app_token=configured_text,
                operation_app_token="",
                table_id=table_text,
                target_kind="probe_error",
                message=str(exc),
            )
        else:
            wiki_probe = self._probe_wiki_node(client, node_token=configured_text)
            if wiki_probe.get("ok"):
                node = wiki_probe.get("node", {})
                operation_app_token = str(node.get("obj_token", "") or "").strip()
                wiki_obj_type = str(node.get("obj_type", "") or "").strip()
                if operation_app_token:
                    wiki_bitable_probe = self._probe_bitable_fields(
                        client,
                        app_token=operation_app_token,
                        table_id=table_text,
                        context_label="Wiki 对应多维表",
                    )
                    if wiki_bitable_probe.get("ok"):
                        preview = self._new_preview(
                            configured_app_token=configured_text,
                            operation_app_token=operation_app_token,
                            table_id=table_text,
                            target_kind="wiki_token_pair",
                            display_url=build_wiki_bitable_url(configured_text, table_text),
                            wiki_node_token=configured_text,
                        )
                        preview["wiki_obj_type"] = wiki_obj_type
                    else:
                        preview = self._new_preview(
                            configured_app_token=configured_text,
                            operation_app_token="",
                            table_id=table_text,
                            target_kind=str(wiki_bitable_probe.get("kind", "")).strip() or "invalid",
                            message=str(wiki_bitable_probe.get("message", "")).strip()
                            or "Wiki 节点不是可操作的多维表目标",
                        )
                else:
                    preview = self._new_preview(
                        configured_app_token=configured_text,
                        operation_app_token="",
                        table_id=table_text,
                        target_kind="invalid",
                        message="Wiki 节点未解析出可操作的多维表 Token",
                    )
            else:
                base_probe = self._probe_bitable_fields(
                    client,
                    app_token=configured_text,
                    table_id=table_text,
                    context_label="Base 多维表",
                )
                if base_probe.get("ok"):
                    preview = self._new_preview(
                        configured_app_token=configured_text,
                        operation_app_token=configured_text,
                        table_id=table_text,
                        target_kind="base_token_pair",
                        display_url=build_bitable_url(configured_text, table_text),
                    )
                else:
                    base_kind = str(base_probe.get("kind", "")).strip()
                    wiki_kind = str(wiki_probe.get("kind", "")).strip()
                    if "probe_error" in {base_kind, wiki_kind}:
                        preview = self._new_preview(
                            configured_app_token=configured_text,
                            operation_app_token="",
                            table_id=table_text,
                            target_kind="probe_error",
                            message=str(wiki_probe.get("message", "")).strip()
                            or str(base_probe.get("message", "")).strip()
                            or "目标探测失败",
                        )
                    else:
                        preview = self._new_preview(
                            configured_app_token=configured_text,
                            operation_app_token="",
                            table_id=table_text,
                            target_kind="invalid",
                            message=str(wiki_probe.get("message", "")).strip()
                            or str(base_probe.get("message", "")).strip()
                            or "目标既不是可访问的 Base，也不是可解析的 Wiki 多维表",
                        )

        ttl_sec = self._preview_ttl_sec(str(preview.get("target_kind", "")).strip())
        with _TOKEN_PAIR_PREVIEW_CACHE_LOCK:
            _TOKEN_PAIR_PREVIEW_CACHE[cache_key] = {
                "preview": dict(preview),
                "expires_at": now + ttl_sec,
            }
        return dict(preview)

    def resolve(self, target: Dict[str, Any] | None) -> Dict[str, str]:
        current = target if isinstance(target, dict) else {}
        explicit_app_token = str(current.get("app_token", "") or "").strip()
        explicit_table_id = str(current.get("table_id", "") or "").strip()
        base_url = str(current.get("base_url", "") or "").strip()
        wiki_url = str(current.get("wiki_url", "") or "").strip()

        if base_url:
            app_token = extract_bitable_app_token_from_base_url(base_url) or explicit_app_token
            table_id = extract_bitable_table_id_from_url(base_url) or explicit_table_id
            if not app_token or not table_id:
                raise ValueError(
                    "多维目标 base_url 无法解析 app_token/table_id，请补充 Table ID 或改填标准 Base 链接"
                )
            return {
                "resolved_from": "base_url",
                "source_url": base_url,
                "app_token": app_token,
                "table_id": table_id,
                "bitable_url": build_bitable_url(app_token, table_id),
                "wiki_node_token": "",
                "wiki_obj_type": "",
            }

        if wiki_url:
            table_id = extract_bitable_table_id_from_url(wiki_url) or explicit_table_id
            app_token = explicit_app_token
            wiki_node_token = ""
            wiki_obj_type = ""
            if not app_token:
                wiki_client = self._new_wiki_client()
                wiki_node_token = wiki_client.extract_node_token_from_url(wiki_url)
                node = wiki_client.get_wiki_node_info(wiki_node_token)
                wiki_obj_type = str(node.get("obj_type", "") or "").strip()
                app_token = str(node.get("obj_token", "") or "").strip()
            if not table_id:
                raise ValueError(
                    "多维目标 wiki_url 缺少表标识，请在链接中包含 table 参数或单独填写 Table ID"
                )
            if not app_token:
                raise ValueError(
                    "多维目标 wiki_url 无法解析 app_token，请改填 Base 链接或单独填写 App Token"
                )
            return {
                "resolved_from": "wiki_url",
                "source_url": wiki_url,
                "app_token": app_token,
                "table_id": table_id,
                "bitable_url": build_bitable_url(app_token, table_id),
                "wiki_node_token": wiki_node_token,
                "wiki_obj_type": wiki_obj_type,
            }

        if explicit_app_token and explicit_table_id:
            return {
                "resolved_from": "token_pair",
                "source_url": "",
                "app_token": explicit_app_token,
                "table_id": explicit_table_id,
                "bitable_url": build_bitable_url(explicit_app_token, explicit_table_id),
                "wiki_node_token": "",
                "wiki_obj_type": "",
            }

        raise ValueError("多维目标配置缺失: 请填写 App Token/Table ID，或填写 Base/Wiki 链接")
