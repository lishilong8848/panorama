from __future__ import annotations

import base64
import json
import subprocess
from typing import Any, Callable
from urllib.parse import urlsplit

import requests


class ReportApiError(RuntimeError):
    """A report API error that never includes credentials or session IDs."""


class ReportApiUnauthorized(ReportApiError):
    """A site session has expired or the account is not authorized."""


# The vendor login page uses this public protocol key, not a site password.
_LOGIN_KEY_B64 = "Z29uZ2ppa2VqaTIwMTkwOQ=="
_ENCRYPT_SCRIPT = r"""
const crypto = require('crypto');
let text = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => { text += chunk; });
process.stdin.on('end', () => {
  const data = JSON.parse(text);
  const key = Buffer.from(data.key, 'base64');
  const cipher = crypto.createCipheriv('aes-128-cbc', key, key);
  const body = JSON.stringify({account: data.account, password: data.password});
  process.stdout.write(Buffer.concat([cipher.update(body, 'utf8'), cipher.final()]).toString('base64'));
});
"""


def encrypt_login_payload(username: str, password: str) -> str:
    """Use the Node runtime already bundled with Playwright for the vendor's AES login."""
    try:
        from playwright._impl._driver import compute_driver_executable

        node_path, _driver_path = compute_driver_executable()
        result = subprocess.run(
            [str(node_path), "-e", _ENCRYPT_SCRIPT],
            input=json.dumps(
                {"key": _LOGIN_KEY_B64, "account": username, "password": password},
                ensure_ascii=False,
            ),
            text=True,
            encoding="utf-8",
            capture_output=True,
            timeout=15,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if result.returncode != 0 or not result.stdout:
            raise ReportApiError("动环登录加密失败，请检查 Playwright 运行时")
        base64.b64decode(result.stdout, validate=True)
        return result.stdout
    except ReportApiError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise ReportApiError(f"动环登录加密失败: {type(exc).__name__}") from None


class BmsReportApiSession:
    def __init__(
        self,
        site: dict[str, Any],
        *,
        session: Any | None = None,
        encryptor: Callable[[str, str], str] = encrypt_login_payload,
    ) -> None:
        if not isinstance(site, dict):
            raise ReportApiError("动环站点配置无效")
        raw_host = str(site.get("host") or site.get("url") or "").strip()
        if "://" not in raw_host:
            raw_host = f"http://{raw_host}"
        try:
            parsed = urlsplit(raw_host)
            _ = parsed.port
        except ValueError:
            raise ReportApiError("动环站点地址无效") from None
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ReportApiError("动环站点地址无效")
        self.base_url = f"{parsed.scheme}://{parsed.netloc}"
        self.host = parsed.hostname
        self.username = str(site.get("username") or "").strip()
        self.password = str(site.get("password") or "")
        if not self.username or not self.password:
            raise ReportApiError("动环站点账号或密码未配置")
        self.session = session if session is not None else requests.Session()
        self.session.trust_env = False
        self.encryptor = encryptor
        self._session_id = ""

    def __enter__(self) -> "BmsReportApiSession":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def close(self) -> None:
        self._session_id = ""
        self.session.close()

    def request(self, method: str, path: str, *, timeout: Any = (5, 30), **kwargs: Any) -> Any:
        if not isinstance(path, str) or not path.startswith("/") or path.startswith("//"):
            raise ReportApiError("动环接口路径无效")
        parsed = urlsplit(path)
        if parsed.netloc or parsed.scheme or parsed.query or parsed.fragment or "\\" in path:
            raise ReportApiError("动环接口路径无效")
        try:
            response = self.session.request(
                method, self.base_url + path, timeout=timeout, allow_redirects=False, **kwargs
            )
        except requests.RequestException as exc:
            raise ReportApiError(f"动环接口请求失败: {type(exc).__name__}") from None
        status = int(response.status_code)
        if status in {401, 403}:
            raise ReportApiUnauthorized(f"动环接口认证失败 HTTP {status}")
        if not 200 <= status < 300:
            raise ReportApiError(f"动环接口返回 HTTP {status}")
        return response

    def login(self) -> None:
        encrypted = self.encryptor(self.username, self.password)
        response = self.request("POST", "/api/v3/auth/sso/login", json={"text": encrypted})
        try:
            payload = response.json()
        except (ValueError, TypeError):
            raise ReportApiError("动环登录响应格式无效") from None
        if not isinstance(payload, dict) or str(payload.get("error_code", "")) != "00":
            raise ReportApiError("动环登录失败，请检查站点账号和登录状态")
        data = payload.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("session"), str) or not data["session"]:
            raise ReportApiError("动环登录未返回有效会话")
        self._session_id = data["session"]
        values = {
            "X_GU_SID": self._session_id,
            "USER_ID": str(data.get("id") or ""),
            "ACCOUNT": base64.b64encode(str(data.get("account") or self.username).encode()).decode(),
            "USER_NAME": base64.b64encode(str(data.get("name") or "").encode()).decode(),
            "THEME": "default",
            "MODE": "0",
            "X_PRODUCT": "gu",
        }
        for name, value in values.items():
            self.session.cookies.set(name, value, domain=self.host, path="/")

    def find_report(self, report_name: str, *, page_size: int = 50) -> dict[str, Any]:
        name = str(report_name or "").strip()
        if not name or not 1 <= page_size <= 50:
            raise ReportApiError("动环报表查询参数无效")
        matches: list[dict[str, Any]] = []
        page_number = 1
        received = 0
        expected_total: int | None = None
        while True:
            response = self.request(
                "GET",
                "/xbreport/api/v1/immedireport/immediateReport",
                params={
                    "service_type": "-1", "period": "-1", "freq_type": 6,
                    "sort": "create_time", "order": "desc", "page_num": page_number,
                    "page_size": page_size,
                },
                headers={"X-Requested-With": "XMLHttpRequest"},
            )
            try:
                result = response.json()
                rows = result["list"]
                total = int(result["total"])
            except (KeyError, TypeError, ValueError):
                raise ReportApiError("动环报表列表响应格式无效") from None
            if not isinstance(rows, list) or total < 0 or total > 10000:
                raise ReportApiError("动环报表列表响应格式无效")
            if expected_total is None:
                expected_total = total
            elif total != expected_total:
                raise ReportApiError("动环报表列表在查询期间发生变化，请重试")
            if not rows and received < total:
                raise ReportApiError("动环报表列表不完整，请重试")
            for row in rows:
                if not isinstance(row, dict):
                    raise ReportApiError("动环报表列表响应格式无效")
                if str(row.get("plan_name") or "").strip() == name:
                    matches.append(row)
            received += len(rows)
            if received >= total:
                break
            page_number += 1
            if page_number > 10000:
                raise ReportApiError("动环报表列表分页异常")
        if len(matches) > 1:
            raise ReportApiError(f"动环报表名称重复，无法确定目标: {name}")
        if not matches:
            raise ReportApiError(f"未找到动环报表: {name}")
        return matches[0]
