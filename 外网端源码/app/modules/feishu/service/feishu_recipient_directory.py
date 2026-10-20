from __future__ import annotations

import hashlib
import json
import threading
import time
from typing import Any, Callable

from app.config.feishu_recipient_defaults import PUBLIC_ACCOUNT_APP_ID, PUBLIC_ACCOUNT_IDS
from app.core.app_state import AppStateRepository
from app.modules.feishu.service.feishu_auth_resolver import current_feishu_config
from pipeline_utils import get_app_dir


PERSONNEL_APP_TOKEN = "F9lDbA5XKaLmyasoARRcZFpKnZd"
PERSONNEL_TABLE_ID = "tblOu75UtJTDNHIM"
_NAMESPACE = "feishu_recipient_identity"


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return "".join(_text(part) for part in value).strip()
    if isinstance(value, dict):
        return _text(value.get("text", ""))
    return ""


class FeishuRecipientDirectory:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._key = ""
        self._expires = 0.0
        self._retry_at = 0.0
        self._names: dict[str, set[str]] = {}
        self._aliases: dict[str, set[str]] = {}
        self._repository: AppStateRepository | None = None

    @staticmethod
    def _read_records(request: Callable[..., dict]) -> list[dict]:
        records: list[dict] = []
        page = ""
        seen: set[str] = set()
        while True:
            params = {
                "page_size": 500,
                "user_id_type": "open_id",
                "field_names": json.dumps(
                    ["姓名", "员工姓名", "飞书 open_id", "离职/异动情况"], ensure_ascii=False,
                ),
            }
            if page:
                params["page_token"] = page
            body = request(
                "GET",
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{PERSONNEL_APP_TOKEN}"
                f"/tables/{PERSONNEL_TABLE_ID}/records",
                params=params,
            )
            data = body.get("data", {})
            records.extend(data.get("items", []))
            if not data.get("has_more"):
                return records
            page = data.get("page_token", "")
            if not page or page in seen:
                raise RuntimeError("飞书接收人目录分页异常，未发送消息")
            seen.add(page)

    @staticmethod
    def _index(records: list[dict]) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
        names: dict[str, set[str]] = {}
        aliases: dict[str, set[str]] = {}
        for row in records:
            fields = row.get("fields", {})
            name = _text(fields.get("姓名"))
            old_id = _text(fields.get("飞书 open_id"))
            if fields.get("离职/异动情况") in (True, 1, "true", "1"):
                if old_id:
                    aliases.setdefault(old_id, set()).add("")
                continue
            if not name:
                continue
            if old_id:
                aliases.setdefault(old_id, set()).add(name)
            # A shared account on multiple rows is not a unique personal identity.
            users = fields.get("员工姓名", [])
            valid = (
                isinstance(users, list) and len(users) == 1 and isinstance(users[0], dict)
                and _text(users[0].get("name")) == name
                and str(users[0].get("id", "")).startswith("ou_")
            )
            names.setdefault(name, set())
            if valid:
                open_id = users[0]["id"]
                names[name].add(open_id)
                aliases.setdefault(open_id, set()).add(name)
            else:
                names[name].add("")
        return names, aliases

    def resolve(
        self, receive_id: str, *, app_id: str, app_secret: str,
        request: Callable[..., dict], repository: AppStateRepository | None = None,
        name: str = "",
    ) -> str:
        for label, pair in PUBLIC_ACCOUNT_IDS.items():
            if receive_id and receive_id in pair:
                if app_id != PUBLIC_ACCOUNT_APP_ID:
                    raise RuntimeError(f"{label}公共账号的open_id属于另一飞书应用，请更新接收人配置")
                return pair[1]
        config = current_feishu_config() or {}
        paths = config.get("common", {}).get("paths", {})
        key = hashlib.sha256(
            (app_id + ":" + app_secret + ":" + str(get_app_dir()) + ":" + str(paths)).encode()
        ).hexdigest()
        with self._lock:
            if self._key != key:
                self._key, self._expires, self._retry_at = key, 0.0, 0.0
                self._names, self._aliases, self._repository = {}, {}, None
            if time.monotonic() < self._retry_at:
                raise RuntimeError("飞书接收人目录暂不可用，请稍后重试；未使用旧应用openid发送")
            if time.monotonic() >= self._expires:
                try:
                    names, aliases = self._index(self._read_records(request))
                except Exception:
                    self._retry_at = time.monotonic() + 30
                    raise
                self._names, self._aliases = names, aliases
                self._expires = time.monotonic() + 3600
            if repository is None:
                if self._repository is None:
                    self._repository = AppStateRepository(runtime_config={"paths": paths})
                    self._repository.ensure_ready()
                repository = self._repository
            candidates = set(self._aliases.get(receive_id, set()))
            saved = repository.get_runtime_kv(_NAMESPACE, receive_id) or {}
            recipients = (
                config.get("features", {}).get("handover_log", {}).get("review_ui", {})
                .get("review_link_recipients_by_building", {})
            )
            for rows in recipients.values():
                for row in rows:
                    if row.get("open_id") == receive_id:
                        configured_name = _text(row.get("name")) or _text(row.get("note"))
                        if configured_name in self._names:
                            candidates.add(configured_name)
            if name:
                candidates.add(name.strip())
            if saved.get("name"):
                candidates.add(saved["name"])
            if len(candidates) != 1:
                raise ValueError("飞书接收人无法唯一匹配姓名，请核对人员表或更新接收人openid")
            selected = next(iter(candidates))
            ids = self._names.get(selected, set())
            if len(ids) != 1 or "" in ids:
                raise ValueError(f"飞书接收人“{selected}”缺少唯一有效的员工姓名字段，未发送消息")
            resolved = next(iter(ids))
            for alias in {receive_id, resolved}:
                if alias:
                    repository.put_runtime_kv(_NAMESPACE, alias, {"name": selected})
                    self._aliases.setdefault(alias, set()).add(selected)
            return resolved


feishu_recipient_directory = FeishuRecipientDirectory()
