"""Read-only audit of configured and fixed Bitable targets. No business writes."""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config.config_adapter import ensure_v3_config
from app.config.settings_loader import load_pipeline_config
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from app.modules.feishu.service.feishu_token_manager import feishu_token_manager


FIXED_TARGETS = (
    ("F9lDbA5XKaLmyasoARRcZFpKnZd", "tblOu75UtJTDNHIM", "飞书接收人身份目录"),
    ("ASLxbfESPahdTKs0A9NccgbrnXc", "tbl8Ni54taYGeWAa", "110主变多维"),
    ("ASLxbfESPahdTKs0A9NccgbrnXc", "tblz4TkZqrUJB90y", "容量表耗水"),
    ("ASLxbfESPahdTKs0A9NccgbrnXc", "tblqSskJvBnx9UJj", "容量表总用电量"),
    ("G7oUwGdwaiTmimk8i2ecGTWOn4d", "tblATnYxuleJpyVl", "交接班消息联系人"),
    ("HU38bc1vnamMK9sCeOgclUvXnFc", "tbluozblhRAjbljX", "人员签名"),
    ("HU38bc1vnamMK9sCeOgclUvXnFc", "tblC77nllNrprHBY", "公司外人员签名"),
    ("HScCwZt9QiqPCUkSrHjcbBL1ngb", "tblGy6Z1GTxbY1EQ", "系统告警日志"),
)


def collect_targets(config: dict[str, Any]) -> list[dict[str, Any]]:
    targets: dict[tuple[str, str], dict[str, Any]] = {}

    def add(token: str, table: str, source: str) -> None:
        if not token or not re.fullmatch(r"tbl[A-Za-z0-9]+", table):
            return
        row = targets.setdefault((token, table), {"configured_token": token, "table_id": table, "sources": []})
        if source not in row["sources"]:
            row["sources"].append(source)

    def walk(value: Any, path: str, inherited: str = "") -> None:
        if isinstance(value, list):
            for index, item in enumerate(value):
                walk(item, f"{path}[{index}]", inherited)
            return
        if not isinstance(value, dict):
            return
        token = str(value.get("app_token") or "").strip()
        if not token:
            for key in ("source", "target"):
                nested = value.get(key)
                if isinstance(nested, dict) and nested.get("app_token"):
                    token = str(nested["app_token"]).strip()
                    break
        token = token or inherited
        for key, item in value.items():
            if isinstance(item, str):
                if key == "table_id" or key.endswith("_table_id"):
                    add(token, item, f"{path}.{key}")
                elif "/base/" in item or "/wiki/" in item:
                    parsed = urlparse(item)
                    match = re.search(r"/(?:base|wiki)/([^/?#]+)", parsed.path)
                    table = parse_qs(parsed.query).get("table", [""])[0]
                    if match:
                        add(match.group(1), table, f"{path}.{key}")
            walk(item, f"{path}.{key}", token)

    walk(config, "config")
    for token, table, label in FIXED_TARGETS:
        add(token, table, label)
    return sorted(targets.values(), key=lambda row: (row["configured_token"], row["table_id"]))


def audit(config: dict[str, Any], *, parallel: int = 4) -> dict[str, Any]:
    auth = require_feishu_auth_settings(config)
    access_token = feishu_token_manager.get_token(
        app_id=auth["app_id"], app_secret=auth["app_secret"], timeout=20,
    )
    headers = {"Authorization": f"Bearer {access_token}"}

    def get(path: str, **params: Any) -> dict[str, Any]:
        for attempt in range(3):
            try:
                response = requests.get(
                    "https://open.feishu.cn/open-apis/" + path, headers=headers, params=params, timeout=(5, 20),
                )
                payload = response.json()
                result = {"http": response.status_code, **payload}
            except (requests.RequestException, ValueError) as exc:
                result = {"code": "transport_error", "msg": type(exc).__name__}
            if result.get("code") in (0, 99991672, 91403, 1254302, 1254303) or attempt == 2:
                return result
            if (result.get("http", 0) < 500 and result.get("http") != 429
                    and result.get("code") not in ("transport_error", 1254607, 1254290, 1255001, 1255002)):
                return result
            time.sleep(attempt + 1)
        return result

    def summary(payload: dict[str, Any]) -> dict[str, Any]:
        return {key: payload[key] for key in ("http", "code", "msg") if key in payload}

    targets = collect_targets(config)
    bases: dict[str, dict[str, Any]] = {}
    for configured in sorted({row["configured_token"] for row in targets}):
        token = configured
        metadata = get(f"bitable/v1/apps/{token}")
        if metadata.get("code") != 0:
            node = get("wiki/v2/spaces/get_node", token=configured)
            if node.get("code") == 0:
                token = node.get("data", {}).get("node", {}).get("obj_token") or configured
                metadata = get(f"bitable/v1/apps/{token}")
        token = metadata.get("data", {}).get("app", {}).get("app_token") or token
        table_names = {}
        page_token = ""
        while True:
            tables = get(f"bitable/v1/apps/{token}/tables", page_size=100, **({"page_token": page_token} if page_token else {}))
            if tables.get("code") != 0:
                break
            data = tables.get("data", {})
            table_names.update({row["table_id"]: row.get("name", "") for row in data.get("items", [])})
            next_token = data.get("page_token", "")
            if not data.get("has_more") or not next_token or next_token == page_token:
                break
            page_token = next_token
        edit = get(f"drive/v1/permissions/{token}/members/auth", type="bitable", action="edit")
        bases[configured] = {
            "app_token": token,
            "name": metadata.get("data", {}).get("app", {}).get("name", ""),
            "table_names": table_names,
            "edit_allowed": edit.get("data", {}).get("auth_result") if edit.get("code") == 0 else None,
            "edit_check": summary(edit),
        }

    def check(row: dict[str, Any]) -> dict[str, Any]:
        base = bases[row["configured_token"]]
        token, table = base["app_token"], row["table_id"]
        path = f"bitable/v1/apps/{token}/tables/{table}"
        fields = get(path + "/fields", page_size=100)
        records = get(path + "/records", page_size=1)
        return {
            **row, "app_token": token, "base_name": base["name"],
            "table_name": base["table_names"].get(table, ""),
            "url": f"https://vnet.feishu.cn/base/{token}?table={table}",
            "read_allowed": fields.get("code") == 0 and records.get("code") == 0,
            "edit_allowed": base["edit_allowed"], "edit_check": base["edit_check"],
            "fields_check": summary(fields), "records_check": summary(records),
        }

    with ThreadPoolExecutor(max_workers=max(1, min(parallel, 5))) as pool:
        results = list(pool.map(check, targets))
    return {"app_id": auth["app_id"], "checked_at": datetime.now().astimezone().isoformat(), "tables": results}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "表格计算配置.json")
    parser.add_argument("--output", type=Path, default=ROOT / ".runtime" / "feishu_table_permissions.json")
    parser.add_argument("--list-only", action="store_true")
    args = parser.parse_args()
    config = ensure_v3_config(load_pipeline_config(args.config))
    if args.list_only:
        print(json.dumps(collect_targets(config), ensure_ascii=False, indent=2))
        return 0
    result = audit(config)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    for row in result["tables"]:
        print(json.dumps({key: row[key] for key in ("table_id", "table_name", "read_allowed", "edit_allowed", "fields_check", "records_check")}, ensure_ascii=False), flush=True)
    print(f"Checked {len(result['tables'])} tables; report={args.output}")
    return int(any(not row["read_allowed"] or row["edit_allowed"] is False for row in result["tables"]))


if __name__ == "__main__":
    raise SystemExit(main())
