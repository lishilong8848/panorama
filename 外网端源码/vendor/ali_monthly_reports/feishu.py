from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import requests

from .config import ReportConfig
from .utils import make_unique_path, sanitize_download_filename


FEISHU_TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"


def request_tenant_token(config: ReportConfig) -> str:
    response = requests.post(
        FEISHU_TOKEN_URL,
        json={"app_id": config.feishu_app_id, "app_secret": config.feishu_app_secret},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    if data.get("code") != 0:
        raise RuntimeError(f"获取飞书 tenant token 失败: {data.get('msg') or data}")
    return str(data["tenant_access_token"])


def build_feishu_session(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session


def fetch_bitable_records(
    config: ReportConfig,
    table_config: dict[str, str],
    token: str,
    log: Callable[[str, str], None] | None = None,
) -> list[dict[str, Any]]:
    records_url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{config.feishu_app_token}"
        f"/tables/{table_config['table_id']}/records"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    params: dict[str, Any] = {"view_id": table_config["view_id"], "page_size": 500}
    records: list[dict[str, Any]] = []
    page_token = None

    if log:
        log(f"读取表格记录: {table_config['name']}", "info")
    while True:
        if page_token:
            params["page_token"] = page_token
        response = requests.get(records_url, headers=headers, params=params, timeout=120)
        response.raise_for_status()
        result = response.json()
        if result.get("code") != 0:
            raise RuntimeError(f"获取记录失败: {result.get('msg') or result}")
        data_page = result.get("data", {})
        records.extend(data_page.get("items", []))
        if not data_page.get("has_more"):
            break
        page_token = data_page.get("page_token")

    if log:
        log(f"共获取 {len(records)} 条记录", "success")
    return records


def download_attachment(url: str, save_path: Path, token: str) -> None:
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, stream=True, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f"附件下载失败: HTTP {response.status_code}")
    with save_path.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                handle.write(chunk)


def download_record_attachments(
    records: list[dict[str, Any]],
    token: str,
    download_folder: Path,
    attachment_filter: Callable[[str], bool] | None = None,
    attachment_downloader: Callable[[str, Path, str], None] = download_attachment,
    log: Callable[[str, str], None] | None = None,
) -> tuple[list[dict[str, str]], list[str], list[Path]]:
    downloaded: list[dict[str, str]] = []
    errors: list[str] = []
    paths: list[Path] = []

    for record_index, record in enumerate(records, start=1):
        fields = record.get("fields", {})
        for field_value in fields.values():
            if not isinstance(field_value, list) or not field_value:
                continue
            if not isinstance(field_value[0], dict) or "url" not in field_value[0]:
                continue
            for attachment in field_value:
                file_name = sanitize_download_filename(attachment.get("name", ""), f"attachment_{record_index}")
                if attachment_filter and not attachment_filter(file_name):
                    continue
                url = str(attachment.get("url", "")).strip()
                if not url:
                    errors.append(f"{file_name}: 无下载 URL")
                    continue
                save_path = make_unique_path(download_folder, file_name)
                try:
                    if log:
                        log(f"下载: {save_path.name}", "info")
                    attachment_downloader(url, save_path, token)
                    downloaded.append({"name": save_path.name, "size": f"{save_path.stat().st_size / 1024:.1f}KB"})
                    paths.append(save_path)
                except Exception as exc:
                    errors.append(f"{save_path.name}: {exc}")
    return downloaded, errors, paths
