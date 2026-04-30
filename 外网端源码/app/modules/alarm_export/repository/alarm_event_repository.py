from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Tuple
from urllib.parse import urlparse


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return int(default)


def _normalize_host(raw_value: Any) -> str:
    raw = str(raw_value or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        parsed = urlparse(raw)
        return str(parsed.hostname or "").strip()
    parsed = urlparse(f"http://{raw}")
    return str(parsed.hostname or "").strip()


def _non_empty(raw: Any, default: str) -> str:
    text = str(raw or "").strip()
    return text if text else default


def _assert_identifier(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"alarm_bitable_export.db.{field} 不能为空")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", text):
        raise ValueError(f"alarm_bitable_export.db.{field} 非法: {text}")
    return text


def _assert_table_name(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        raise ValueError("数据库表名不能为空")
    if not re.fullmatch(r"[A-Za-z0-9_]+", text):
        raise ValueError(f"数据库表名非法: {text}")
    return text


def _format_table_name(pattern: str, year: int, month: int) -> str:
    return _assert_table_name(str(pattern).format(year=year, month=month))


def _iter_months(start_dt: datetime, end_dt: datetime) -> List[Tuple[int, int]]:
    start_month = datetime(start_dt.year, start_dt.month, 1)
    end_month = datetime(end_dt.year, end_dt.month, 1)
    out: List[Tuple[int, int]] = []
    cursor = start_month
    while cursor <= end_month:
        out.append((cursor.year, cursor.month))
        if cursor.month == 12:
            cursor = datetime(cursor.year + 1, 1, 1)
        else:
            cursor = datetime(cursor.year, cursor.month + 1, 1)
    return out


def _is_table_missing_error(exc: Exception) -> bool:
    code = None
    try:
        code = int(getattr(exc, "args", [None])[0])
    except Exception:  # noqa: BLE001
        code = None
    return code == 1146


def _detect_time_mode(sample: Any) -> str:
    if sample is None:
        return ""
    if isinstance(sample, datetime):
        return "datetime"
    if isinstance(sample, (int, float)) and not isinstance(sample, bool):
        num = int(float(sample))
        return "unix_millis" if abs(num) >= 10**12 else "unix_seconds"
    text = str(sample).strip()
    if not text:
        return ""
    if text.isdigit():
        num = int(text)
        return "unix_millis" if (len(text) >= 13 or abs(num) >= 10**12) else "unix_seconds"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            datetime.strptime(text[:19], fmt)
            return "datetime"
        except Exception:  # noqa: BLE001
            continue
    return ""


def _time_params(start_dt: datetime, end_dt: datetime, mode: str) -> Tuple[Any, Any]:
    if mode == "unix_millis":
        return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)
    if mode == "unix_seconds":
        return int(start_dt.timestamp()), int(end_dt.timestamp())
    return start_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt.strftime("%Y-%m-%d %H:%M:%S")


class AlarmEventRepository:
    def __init__(self, config: Dict[str, Any], export_cfg: Dict[str, Any]) -> None:
        self.config = config
        self.export_cfg = export_cfg

    def _normalize_db_cfg(self) -> Dict[str, Any]:
        db_cfg = self.export_cfg.get("db", {})
        if not isinstance(db_cfg, dict):
            db_cfg = {}
        return {
            "port": _safe_int(db_cfg.get("port", 3306), 3306),
            "user": str(db_cfg.get("user", "root")).strip() or "root",
            "password": str(db_cfg.get("password", "123456")),
            "database": str(db_cfg.get("database", "e_event")).strip() or "e_event",
            "table_pattern": str(db_cfg.get("table_pattern", "event_{year}_{month:02d}")).strip()
            or "event_{year}_{month:02d}",
            "charset": str(db_cfg.get("charset", "utf8mb4")).strip() or "utf8mb4",
            "connect_timeout_sec": max(1, _safe_int(db_cfg.get("connect_timeout_sec", 5), 5)),
            "read_timeout_sec": max(1, _safe_int(db_cfg.get("read_timeout_sec", 20), 20)),
            "write_timeout_sec": max(1, _safe_int(db_cfg.get("write_timeout_sec", 20), 20)),
            "time_field_mode": str(db_cfg.get("time_field_mode", "auto")).strip().lower() or "auto",
            "time_field": _non_empty(db_cfg.get("time_field"), "event_time"),
            "masked_field": _non_empty(db_cfg.get("masked_field"), "masked"),
        }

    def _normalize_test_db_cfg(self) -> Dict[str, Any]:
        raw = self.export_cfg.get("test_db", {})
        if not isinstance(raw, dict):
            raw = {}
        return {
            "enabled": bool(raw.get("enabled", False)),
            "host": _normalize_host(raw.get("host", "")) or "127.0.0.1",
            "port": max(1, _safe_int(raw.get("port", 3306), 3306)),
            "user": str(raw.get("user", "root")).strip() or "root",
            "password": str(raw.get("password", "123456")),
            "database": str(raw.get("database", "e_event")).strip() or "e_event",
            "table_mode": str(raw.get("table_mode", "fixed")).strip().lower() or "fixed",
            "fixed_table": _assert_table_name(str(raw.get("fixed_table", "event_2026_02")).strip() or "event_2026_02"),
            "building_label": str(raw.get("building_label", "测试楼栋")).strip() or "测试楼栋",
            "time_field_mode": str(raw.get("time_field_mode", "auto")).strip().lower() or "auto",
        }

    def _iter_enabled_sites(self) -> List[Dict[str, str]]:
        sites = self.config.get("download", {}).get("sites", [])
        if not isinstance(sites, list):
            return []
        out: List[Dict[str, str]] = []
        for site in sites:
            if not isinstance(site, dict):
                continue
            if not bool(site.get("enabled", False)):
                continue
            building = str(site.get("building", "")).strip()
            host = _normalize_host(site.get("host", "")) or _normalize_host(site.get("url", ""))
            if not building or not host:
                continue
            out.append({"building": building, "host": host})
        return out

    def _detect_mode(
        self,
        conn: Any,
        tables: List[str],
        *,
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
                mode = _detect_time_mode(row.get("t"))
                if mode:
                    emit_log(f"[告警导出] 自动识别时间字段模式: {mode}, table={table}")
                    return mode
            except Exception as exc:  # noqa: BLE001
                if _is_table_missing_error(exc):
                    continue
                raise
        emit_log("[告警导出] 时间字段模式自动识别失败，回退 unix_seconds")
        return "unix_seconds"

    def _load_table_columns(self, conn: Any, table: str) -> set[str]:
        sql = f"SHOW COLUMNS FROM `{table}`"
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall() or []
        out: set[str] = set()
        for row in rows:
            field = str((row or {}).get("Field", "")).strip()
            if field:
                out.add(field)
        return out

    @staticmethod
    def _build_select_fields(fields_cfg: Dict[str, str], columns: Iterable[str]) -> List[str]:
        available = set(columns)
        selected: List[str] = []
        seen: set[str] = set()
        for src in fields_cfg.values():
            field = str(src or "").strip()
            if not field or field in seen:
                continue
            if field in available:
                selected.append(field)
                seen.add(field)

        # Keep fallback status fields available for transformer priority rules.
        for fallback_field in ("is_recover", "is_accept", "is_confirm"):
            if fallback_field in available and fallback_field not in seen:
                selected.append(fallback_field)
                seen.add(fallback_field)
        return selected

    def _query_table_rows(
        self,
        *,
        conn: Any,
        table: str,
        fields_cfg: Dict[str, str],
        event_level_field: str,
        time_field: str,
        masked_field: str,
        skip_levels: List[str],
        start_param: Any,
        end_param: Any,
    ) -> List[Dict[str, Any]]:
        columns = self._load_table_columns(conn, table)
        if time_field not in columns:
            return []

        selected_fields = self._build_select_fields(fields_cfg, columns)
        if time_field not in selected_fields:
            selected_fields.append(time_field)
        if event_level_field in columns and event_level_field not in selected_fields:
            selected_fields.append(event_level_field)
        if masked_field in columns and masked_field not in selected_fields:
            selected_fields.append(masked_field)
        if not selected_fields:
            return []

        where_parts = [f"`{time_field}` >= %s", f"`{time_field}` < %s"]
        params: List[Any] = [start_param, end_param]
        if masked_field in columns:
            where_parts.append(f"(`{masked_field}` IS NULL OR `{masked_field}` <> 1)")
        if skip_levels and event_level_field in columns:
            placeholders = ",".join(["%s"] * len(skip_levels))
            where_parts.append(f"(`{event_level_field}` IS NULL OR `{event_level_field}` NOT IN ({placeholders}))")
            params.extend(skip_levels)

        select_clause = ", ".join([f"`{field}`" for field in selected_fields])
        where_clause = " AND ".join(where_parts)
        sql = f"SELECT {select_clause} FROM `{table}` WHERE {where_clause} ORDER BY `{time_field}` DESC"
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            records = cur.fetchall() or []
        return records if isinstance(records, list) else []

    def _query_events_from_test_db(
        self,
        *,
        start_dt: datetime,
        end_dt: datetime,
        fields_cfg: Dict[str, str],
        event_level_field: str,
        time_field: str,
        masked_field: str,
        skip_levels: List[str],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        test_cfg = self._normalize_test_db_cfg()
        if test_cfg["table_mode"] != "fixed":
            raise ValueError("alarm_bitable_export.test_db.table_mode 仅支持 fixed")

        building = test_cfg["building_label"]
        host = test_cfg["host"]
        table = test_cfg["fixed_table"]
        emit_log(f"[告警导出] 模式=测试数据库 host={host} table={table}")

        rows_out: List[Dict[str, Any]] = []
        succeeded_buildings: List[str] = []
        failed_buildings: List[Dict[str, str]] = []

        import pymysql  # local import: optional runtime dependency

        try:
            conn = pymysql.connect(
                host=host,
                port=int(test_cfg["port"]),
                user=test_cfg["user"],
                password=test_cfg["password"],
                database=test_cfg["database"],
                charset="utf8mb4",
                connect_timeout=5,
                read_timeout=20,
                write_timeout=20,
                cursorclass=pymysql.cursors.DictCursor,
            )
        except Exception as exc:  # noqa: BLE001
            failed_buildings.append({"building": building, "error": f"测试库连接失败: {exc}"})
            return {
                "rows": rows_out,
                "succeeded_buildings": succeeded_buildings,
                "failed_buildings": failed_buildings,
                "table_names": [table],
                "start_time": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            }

        try:
            mode = self._detect_mode(
                conn,
                [table],
                time_field=time_field,
                configured_mode=test_cfg["time_field_mode"],
                emit_log=emit_log,
            )
            start_param, end_param = _time_params(start_dt, end_dt, mode)
            records = self._query_table_rows(
                conn=conn,
                table=table,
                fields_cfg=fields_cfg,
                event_level_field=event_level_field,
                time_field=time_field,
                masked_field=masked_field,
                skip_levels=skip_levels,
                start_param=start_param,
                end_param=end_param,
            )
            for record in records:
                rows_out.append({"building": building, "host": host, "table": table, "row": record or {}})
            succeeded_buildings.append(building)
            emit_log(f"[告警导出] 测试库查询完成 building={building}, records={len(records)}")
        except Exception as exc:  # noqa: BLE001
            failed_buildings.append({"building": building, "error": str(exc)})
        finally:
            conn.close()

        return {
            "rows": rows_out,
            "succeeded_buildings": succeeded_buildings,
            "failed_buildings": failed_buildings,
            "table_names": [table],
            "start_time": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def query_events(
        self,
        *,
        start_dt: datetime,
        end_dt: datetime,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        if end_dt <= start_dt:
            raise ValueError("告警导出时间窗口无效")

        fields_cfg = self.export_cfg.get("fields", {})
        if not isinstance(fields_cfg, dict):
            raise ValueError("alarm_bitable_export.fields 配置错误")

        db_cfg = self._normalize_db_cfg()
        event_level_field = _assert_identifier(fields_cfg.get("event_level", "event_level"), "fields.event_level")
        time_field = _assert_identifier(fields_cfg.get("event_time", db_cfg.get("time_field", "event_time")), "fields.event_time")
        masked_field = _assert_identifier(db_cfg.get("masked_field", "masked"), "db.masked_field")
        skip_levels: List[str] = []
        for item in self.export_cfg.get("skip_levels", []):
            text = str(item).strip()
            if not text:
                continue
            try:
                skip_levels.append(str(int(float(text))))
            except Exception:  # noqa: BLE001
                continue

        test_cfg = self._normalize_test_db_cfg()
        if bool(test_cfg.get("enabled", False)):
            return self._query_events_from_test_db(
                start_dt=start_dt,
                end_dt=end_dt,
                fields_cfg=fields_cfg,
                event_level_field=event_level_field,
                time_field=time_field,
                masked_field=masked_field,
                skip_levels=skip_levels,
                emit_log=emit_log,
            )

        sites = self._iter_enabled_sites()
        if not sites:
            raise ValueError("download.sites 未找到启用楼栋，无法执行告警导出")

        table_names = [_format_table_name(db_cfg["table_pattern"], y, m) for y, m in _iter_months(start_dt, end_dt)]

        rows_out: List[Dict[str, Any]] = []
        succeeded_buildings: List[str] = []
        failed_buildings: List[Dict[str, str]] = []

        import pymysql  # local import: optional runtime dependency

        for site in sites:
            building = site["building"]
            host = site["host"]
            emit_log(
                f"[告警导出] 开始查询楼栋={building}, host={host}, "
                f"window={start_dt:%Y-%m-%d %H:%M:%S}~{end_dt:%Y-%m-%d %H:%M:%S}"
            )
            try:
                conn = pymysql.connect(
                    host=host,
                    port=int(db_cfg["port"]),
                    user=db_cfg["user"],
                    password=db_cfg["password"],
                    database=db_cfg["database"],
                    charset=db_cfg["charset"],
                    connect_timeout=int(db_cfg["connect_timeout_sec"]),
                    read_timeout=int(db_cfg["read_timeout_sec"]),
                    write_timeout=int(db_cfg["write_timeout_sec"]),
                    cursorclass=pymysql.cursors.DictCursor,
                )
            except Exception as exc:  # noqa: BLE001
                failed_buildings.append({"building": building, "error": f"数据库连接失败: {exc}"})
                continue

            try:
                mode = self._detect_mode(
                    conn,
                    table_names,
                    time_field=time_field,
                    configured_mode=db_cfg["time_field_mode"],
                    emit_log=emit_log,
                )
                start_param, end_param = _time_params(start_dt, end_dt, mode)
                table_hit = 0
                for table in table_names:
                    try:
                        records = self._query_table_rows(
                            conn=conn,
                            table=table,
                            fields_cfg=fields_cfg,
                            event_level_field=event_level_field,
                            time_field=time_field,
                            masked_field=masked_field,
                            skip_levels=skip_levels,
                            start_param=start_param,
                            end_param=end_param,
                        )
                    except Exception as exc:  # noqa: BLE001
                        if _is_table_missing_error(exc):
                            continue
                        raise
                    if records:
                        table_hit += 1
                        for record in records:
                            rows_out.append({"building": building, "host": host, "table": table, "row": record or {}})

                succeeded_buildings.append(building)
                emit_log(f"[告警导出] 楼栋={building} 查询完成, 命中表={table_hit}")
            except Exception as exc:  # noqa: BLE001
                failed_buildings.append({"building": building, "error": str(exc)})
            finally:
                conn.close()

        return {
            "rows": rows_out,
            "succeeded_buildings": succeeded_buildings,
            "failed_buildings": failed_buildings,
            "table_names": table_names,
            "start_time": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        }
