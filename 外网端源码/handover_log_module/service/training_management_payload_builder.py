from __future__ import annotations

import copy
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

import requests


TRAINING_URL = "https://vnet.feishu.link/app/app_4jt61zhh0crjd/openapi/datacenter/started-teaching"
TRAINING_SECTION = "培训管理"
TRAINING_COLUMNS = {"B": "培训项目", "D": "培训时间段", "E": "培训方式", "F": "培训讲师", "G": "培训参与人"}
BEIJING = timezone(timedelta(hours=8))


def training_people(*values: Any) -> str:
    names: list[str] = []
    for value in values:
        parts = value if isinstance(value, (list, tuple)) else re.split(r"[、,，;；/\s]+", str(value or ""))
        for part in parts:
            name = str(part or "").strip()
            if name and name not in {"/", "-"} and name not in names:
                names.append(name)
    return "、".join(names)


def latest_training_notice(item: dict[str, Any]) -> datetime | None:
    raw_times = item.get("signInNoticeTimes", [])
    values = list(raw_times) if isinstance(raw_times, list) else []
    values.append(item.get("signInNoticeTime"))
    parsed = []
    for raw in values:
        try:
            value = datetime.fromisoformat(str(raw or "").strip())
            parsed.append(value.replace(tzinfo=BEIJING) if value.tzinfo is None else value.astimezone(BEIJING))
        except (ValueError, TypeError):
            continue
    return max(parsed) if parsed else None


def normalize_training_rows(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list) or len(rows) > 1000:
        raise ValueError("培训记录格式无效或数量过多")
    result = []
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("cells"), dict):
            raise ValueError("培训记录缺少字段内容")
        row_id = str(row.get("row_id", "")).strip()
        if not row_id or len(row_id) > 200:
            raise ValueError("培训记录缺少有效标识")
        cells = {key: str(row["cells"].get(key) or "").strip() for key in TRAINING_COLUMNS}
        if any(len(value) > 10000 for value in cells.values()):
            raise ValueError("培训字段内容过长")
        result.append({"row_id": row_id, "cells": cells})
    return result


def append_training_rows(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = copy.deepcopy(existing)
    seen = {str(row.get("row_id", "")) for row in result}
    for row in incoming:
        key = str(row.get("row_id", ""))
        if key and key not in seen:
            result.append(copy.deepcopy(row))
            seen.add(key)
    return result


def attach_training_row_ids(document: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    # Excel does not retain row IDs; attach them only when importing a newly generated document.
    remaining = copy.deepcopy(rows)
    for section in document.get("sections", []):
        if section.get("name") != TRAINING_SECTION:
            continue
        for row in section.get("rows", []):
            cells = row.get("cells", {})
            match = next((candidate for candidate in remaining if all(
                str(cells.get(key) or "").strip() == str(candidate.get("cells", {}).get(key) or "").strip()
                for key in TRAINING_COLUMNS
            )), None)
            if match is not None:
                row["row_id"] = match["row_id"]
                remaining.remove(match)


class TrainingManagementPayloadBuilder:
    def __init__(self, handover_cfg: dict[str, Any], *, http_get=None, now=None) -> None:
        self.config = handover_cfg.get("training_management_section", {})
        self.http_get = http_get or requests.get
        self.now = now or (lambda: datetime.now(BEIJING))

    def build(self, *, building: str, duty_date: str, current_people: Any, next_people: Any,
              emit_log: Callable[[str], None] = print) -> dict[str, Any]:
        target = date.fromisoformat(duty_date)
        if not self.config.get("enabled", True):
            return {TRAINING_SECTION: []}
        if target.strftime("%Y-%m") != self.now().strftime("%Y-%m"):
            raise ValueError("培训接口只提供当月数据；已保留该历史班次的培训内容")
        api_key = str(self.config.get("api_key") or os.environ.get("HANDOVER_TRAINING_API_KEY", "")).strip()
        if not api_key or "*" in api_key:
            raise ValueError("未配置培训接口密钥，请在后端 training_management_section.api_key 中配置")
        try:
            response = self.http_get(
                TRAINING_URL, params={"room": "南通", "limit": 500},
                headers={"Authorization": f"Bearer {api_key}"}, timeout=(5, 15), allow_redirects=False,
            )
            try:
                if response.status_code != 200:
                    raise ValueError(f"培训接口返回 HTTP {response.status_code}，已保留原有内容")
                payload = response.json()
            finally:
                response.close()
        except requests.RequestException as exc:
            raise ValueError("培训接口请求失败或超时，已保留原有内容，请稍后刷新") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise ValueError("培训接口返回格式不正确，已保留原有内容")
        items = payload["items"]
        total = payload.get("total")
        if not isinstance(total, int) or total < 0 or total > len(items):
            raise ValueError("培训接口返回条数不完整，已保留原有内容，请检查接口 limit 上限")
        latest_by_id: dict[str, tuple[datetime, dict[str, Any]]] = {}
        for item in items:
            if not isinstance(item, dict):
                raise ValueError("培训接口记录格式不正确，已保留原有内容")
            task_id = str(item.get("taskId") or "").strip()
            notice = latest_training_notice(item)
            if not task_id or notice is None:
                emit_log("[交接班][培训管理] 跳过缺少任务ID或有效通知时间的记录")
                continue
            previous = latest_by_id.get(task_id)
            if previous is None or notice > previous[0]:
                latest_by_id[task_id] = (notice, item)
        rows = []
        people = training_people(current_people, next_people)
        for task_id, (notice, item) in sorted(latest_by_id.items(), key=lambda pair: (pair[1][0], pair[0])):
            if notice.date() != target or str(item.get("buildingName") or "").strip() != building:
                continue
            title = str(item.get("taskName") or "").strip()
            if not title:
                continue
            rows.append({"row_id": f"training:{task_id}", "cells": {
                "B": title, "D": "17：30-18：00", "E": "理论",
                "F": training_people(item.get("instructors", [])), "G": people,
            }})
        return {TRAINING_SECTION: normalize_training_rows(rows)}
