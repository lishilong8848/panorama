from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Tuple
from urllib.parse import urlparse


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return int(default)


def _assert_identifier(name: str, field: str) -> str:
    text = str(name or "").strip()
    if not text:
        raise ValueError(f"alarm_db.{field} 不能为空")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", text):
        raise ValueError(f"alarm_db.{field} 非法: {text}")
    return text


def _assert_table_name(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        raise ValueError("数据库表名不能为空")
    if not re.fullmatch(r"[A-Za-z0-9_]+", text):
        raise ValueError(f"数据库表名非法: {text}")
    return text


def _extract_site_host(raw_value: Any) -> str:
    raw = str(raw_value or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        parsed = urlparse(raw)
        return str(parsed.hostname or "").strip()
    parsed = urlparse(f"http://{raw}")
    return str(parsed.hostname or "").strip()


def _format_table_name(pattern: str, year: int, month: int) -> str:
    text = str(pattern or "event_{year}_{month:02d}").strip() or "event_{year}_{month:02d}"
    return _assert_table_name(text.format(year=year, month=month))


def _iterate_months(start_dt: datetime, end_dt: datetime) -> List[Tuple[int, int]]:
    cursor = datetime(start_dt.year, start_dt.month, 1)
    end_month = datetime(end_dt.year, end_dt.month, 1)
    out: List[Tuple[int, int]] = []
    while cursor <= end_month:
        out.append((cursor.year, cursor.month))
        if cursor.month == 12:
            cursor = datetime(cursor.year + 1, 1, 1)
        else:
            cursor = datetime(cursor.year, cursor.month + 1, 1)
    return out


def _datetime_text(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _detect_time_mode_from_sample(sample: Any) -> str:
    if sample is None:
        return ""
    if isinstance(sample, datetime):
        return "datetime"
    if isinstance(sample, (int, float)) and not isinstance(sample, bool):
        num = int(float(sample))
        if abs(num) >= 10**12:
            return "unix_millis"
        return "unix_seconds"
    text = str(sample).strip()
    if not text:
        return ""
    if text.isdigit():
        num = int(text)
        if len(text) >= 13 or abs(num) >= 10**12:
            return "unix_millis"
        return "unix_seconds"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            datetime.strptime(text[:19], fmt)
            return "datetime"
        except Exception:  # noqa: BLE001
            continue
    return ""


def _time_range_params(start_dt: datetime, end_dt: datetime, mode: str) -> Tuple[Any, Any]:
    if mode == "unix_millis":
        return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)
    if mode == "unix_seconds":
        return int(start_dt.timestamp()), int(end_dt.timestamp())
    return _datetime_text(start_dt), _datetime_text(end_dt)


def _to_sort_key(value: Any, mode: str) -> int:
    if value is None:
        return -1
    if mode in {"unix_seconds", "unix_millis"}:
        try:
            return int(float(value))
        except Exception:  # noqa: BLE001
            return -1
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    text = str(value).strip()
    if not text:
        return -1
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return int(datetime.strptime(text[:19], fmt).timestamp() * 1000)
        except Exception:  # noqa: BLE001
            continue
    return -1


def _is_table_missing_error(exc: Exception) -> bool:
    code = None
    try:
        code = int(getattr(exc, "args", [None])[0])
    except Exception:  # noqa: BLE001
        code = None
    return code == 1146


@dataclass
class AlarmSummary:
    total_count: int
    unrecovered_count: int
    accept_description: str
    used_host: str
    used_mode: str
    queried_tables: List[str]


class AlarmRepository:
    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg

    def _resolve_site_host(self, building: str) -> str:
        text = str(building or "").strip()
        if not text:
            raise ValueError("building 不能为空")
        sites = self.handover_cfg.get("sites", [])
        if not isinstance(sites, list):
            sites = []
        for site in sites:
            if not isinstance(site, dict):
                continue
            if str(site.get("building", "")).strip() != text:
                continue
            host = _extract_site_host(site.get("host", "")) or _extract_site_host(site.get("url", ""))
            if host:
                return host
        raise ValueError(f"未找到楼栋站点主机: {text}")

    def _normalize_alarm_cfg(self) -> Dict[str, Any]:
        raw = self.handover_cfg.get("alarm_db", {})
        cfg = raw if isinstance(raw, dict) else {}
        return {
            "port": _safe_int(cfg.get("port", 3306), 3306),
            "user": str(cfg.get("user", "root")).strip() or "root",
            "password": str(cfg.get("password", "123456")),
            "database": str(cfg.get("database", "e_event")).strip() or "e_event",
            "table_pattern": str(cfg.get("table_pattern", "event_{year}_{month:02d}")).strip() or "event_{year}_{month:02d}",
            "time_field": _assert_identifier(cfg.get("time_field", "event_time"), "time_field"),
            "masked_field": _assert_identifier(cfg.get("masked_field", "masked"), "masked_field"),
            "is_recover_field": _assert_identifier(cfg.get("is_recover_field", "is_recover"), "is_recover_field"),
            "accept_description_field": _assert_identifier(
                cfg.get("accept_description_field", "accept_description"),
                "accept_description_field",
            ),
            "time_field_mode": str(cfg.get("time_field_mode", "auto")).strip().lower() or "auto",
            "charset": str(cfg.get("charset", "utf8mb4")).strip() or "utf8mb4",
            "connect_timeout_sec": max(1, _safe_int(cfg.get("connect_timeout_sec", 5), 5)),
            "read_timeout_sec": max(1, _safe_int(cfg.get("read_timeout_sec", 20), 20)),
            "write_timeout_sec": max(1, _safe_int(cfg.get("write_timeout_sec", 20), 20)),
        }

    def _determine_mode(
        self,
        conn: Any,
        tables: List[str],
        time_field: str,
        configured_mode: str,
        emit_log: Callable[[str], None],
    ) -> str:
        if configured_mode in {"unix_seconds", "unix_millis", "datetime"}:
            return configured_mode
        for table in tables:
            sql = f"SELECT `{time_field}` AS t FROM `{table}` WHERE `{time_field}` IS NOT NULL ORDER BY `{time_field}` DESC LIMIT 1"
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    row = cur.fetchone()
                if not row:
                    continue
                mode = _detect_time_mode_from_sample(row.get("t"))
                if mode:
                    emit_log(f"[交接班][告警查询] 自动识别时间字段模式: {mode}, table={table}")
                    return mode
            except Exception as exc:  # noqa: BLE001
                if _is_table_missing_error(exc):
                    continue
                raise
        emit_log("[交接班][告警查询] 自动识别失败，回退 unix_seconds")
        return "unix_seconds"

    def query_alarm_summary(
        self,
        *,
        building: str,
        start_time: str,
        end_time: str,
        time_format: str = "%Y-%m-%d %H:%M:%S",
        emit_log: Callable[[str], None] = print,
    ) -> AlarmSummary:
        start_dt = datetime.strptime(str(start_time).strip(), time_format)
        end_dt = datetime.strptime(str(end_time).strip(), time_format)
        if end_dt <= start_dt:
            raise ValueError(f"告警查询时间窗无效: start={start_time}, end={end_time}")

        host = self._resolve_site_host(building)
        cfg = self._normalize_alarm_cfg()
        table_pattern = cfg["table_pattern"]
        tables = [_format_table_name(table_pattern, y, m) for y, m in _iterate_months(start_dt, end_dt)]

        emit_log(
            f"[交接班][告警查询] 楼栋={building}, host={host}, "
            f"window={start_time}~{end_time}, tables={','.join(tables)}"
        )

        import pymysql  # local import: avoid hard dependency at module import time

        conn = pymysql.connect(
            host=host,
            port=int(cfg["port"]),
            user=cfg["user"],
            password=cfg["password"],
            database=cfg["database"],
            charset=cfg["charset"],
            connect_timeout=int(cfg["connect_timeout_sec"]),
            read_timeout=int(cfg["read_timeout_sec"]),
            write_timeout=int(cfg["write_timeout_sec"]),
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            mode = self._determine_mode(
                conn=conn,
                tables=tables,
                time_field=cfg["time_field"],
                configured_mode=cfg["time_field_mode"],
                emit_log=emit_log,
            )
            start_param, end_param = _time_range_params(start_dt, end_dt, mode)
            total = 0
            unrecovered = 0
            best_desc = ""
            best_ts = -1
            queried_tables: List[str] = []
            for table in tables:
                where_sql = (
                    f"`{cfg['time_field']}` >= %s AND `{cfg['time_field']}` < %s "
                    f"AND (`{cfg['masked_field']}` IS NULL OR `{cfg['masked_field']}` <> 1)"
                )
                count_sql = (
                    "SELECT COUNT(*) AS total_count, "
                    f"SUM(CASE WHEN `{cfg['is_recover_field']}` IS NULL OR `{cfg['is_recover_field']}` <> 1 THEN 1 ELSE 0 END) AS unrecovered_count "
                    f"FROM `{table}` WHERE {where_sql}"
                )
                desc_sql = (
                    f"SELECT `{cfg['accept_description_field']}` AS accept_description, `{cfg['time_field']}` AS t "
                    f"FROM `{table}` WHERE {where_sql} "
                    f"AND (`{cfg['is_recover_field']}` IS NULL OR `{cfg['is_recover_field']}` <> 1) "
                    f"AND `{cfg['accept_description_field']}` IS NOT NULL "
                    f"AND TRIM(`{cfg['accept_description_field']}`) <> '' "
                    f"ORDER BY `{cfg['time_field']}` DESC LIMIT 1"
                )
                params = (start_param, end_param)
                try:
                    with conn.cursor() as cur:
                        cur.execute(count_sql, params)
                        row = cur.fetchone() or {}
                    total += _safe_int(row.get("total_count", 0), 0)
                    unrecovered += _safe_int(row.get("unrecovered_count", 0), 0)
                    with conn.cursor() as cur:
                        cur.execute(desc_sql, params)
                        desc_row = cur.fetchone() or {}
                    desc_text = str(desc_row.get("accept_description") or "").strip()
                    if desc_text:
                        ts = _to_sort_key(desc_row.get("t"), mode)
                        if ts >= best_ts:
                            best_ts = ts
                            best_desc = desc_text
                    queried_tables.append(table)
                except Exception as exc:  # noqa: BLE001
                    if _is_table_missing_error(exc):
                        emit_log(f"[交接班][告警查询] 表不存在，跳过: {table}")
                        continue
                    raise

            emit_log(
                f"[交接班][告警查询] 楼栋={building}, total={total}, "
                f"unrecovered={unrecovered}, accept_desc={'有' if bool(best_desc) else '无'}"
            )
            return AlarmSummary(
                total_count=total,
                unrecovered_count=unrecovered,
                accept_description=best_desc,
                used_host=host,
                used_mode=mode,
                queried_tables=queried_tables,
            )
        finally:
            conn.close()
