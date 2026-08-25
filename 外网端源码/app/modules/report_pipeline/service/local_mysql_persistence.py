from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any, Dict, List


class LocalMysqlCalculationWriter:
    def __init__(self, config: Dict[str, Any] | None) -> None:
        self.config = dict(config) if isinstance(config, dict) else {}
        self.enabled = bool(self.config.get("enabled", False))
        self._connection: Any | None = None
        self._table = str(self.config.get("table", "electricity_consumption_details") or "").strip()
        if self.enabled and not re.fullmatch(r"[A-Za-z0-9_]+", self._table):
            raise ValueError("local_mysql.table 仅允许字母、数字和下划线")

    def _connect(self) -> Any:
        if self._connection is not None:
            self._connection.ping(reconnect=True)
            return self._connection
        import pymysql

        required = ("host", "user", "database")
        missing = [key for key in required if not str(self.config.get(key, "") or "").strip()]
        if missing:
            raise ValueError(f"local_mysql 配置缺失: {','.join(missing)}")
        self._connection = pymysql.connect(
            host=str(self.config.get("host", "127.0.0.1") or "127.0.0.1").strip(),
            port=int(self.config.get("port", 3306) or 3306),
            user=str(self.config.get("user", "root") or "root").strip(),
            password=str(self.config.get("password", "") or ""),
            database=str(self.config.get("database", "") or "").strip(),
            charset=str(self.config.get("charset", "utf8mb4") or "utf8mb4").strip(),
            connect_timeout=max(1, int(self.config.get("connect_timeout_sec", 5) or 5)),
            read_timeout=max(1, int(self.config.get("read_timeout_sec", 20) or 20)),
            write_timeout=max(1, int(self.config.get("write_timeout_sec", 30) or 30)),
            autocommit=False,
        )
        return self._connection

    @staticmethod
    def _rows(result: Any, client: Any, *, date_text: str, skip_zero_records: bool) -> List[tuple[Any, ...]]:
        canonical_name = getattr(client, "_canonical_metric_name_fn", lambda value: str(value or "").strip())
        dimension_mapping = getattr(client, "_dimension_mapping", {})
        data_time = datetime.strptime(str(date_text or "").strip()[:10], "%Y-%m-%d")
        rows: List[tuple[Any, ...]] = []
        for record in list(getattr(result, "records", []) or []):
            value = float(getattr(record, "value", 0.0) or 0.0)
            if not math.isfinite(value):
                raise ValueError(f"MySQL用电量不是有限数字: {value}")
            if skip_zero_records and value == 0:
                continue
            item_name = str(getattr(record, "item_name", "") or "").strip()
            mapped = dimension_mapping.get(canonical_name(item_name)) if isinstance(dimension_mapping, dict) else None
            if mapped:
                type_name, category_name, item_name = mapped
            else:
                type_name = getattr(record, "type_name", "")
                category_name = getattr(record, "category_name", "")
            rows.append(
                (
                    str(type_name or "").strip(),
                    str(category_name or "").strip(),
                    str(item_name or "").strip(),
                    data_time,
                    str(getattr(record, "calc_method", "") or "").strip(),
                    value,
                    str(getattr(record, "building", "") or getattr(result, "building", "") or "").strip(),
                )
            )
        return rows

    def persist(self, result: Any, client: Any, *, date_text: str, skip_zero_records: bool) -> int:
        if not self.enabled:
            return 0
        rows = self._rows(result, client, date_text=date_text, skip_zero_records=skip_zero_records)
        if not rows:
            return 0
        connection = self._connect()
        sql = (
            f"INSERT INTO `{self._table}` "
            "(`type`,`sort`,`item`,`data_time`,`com_mode`,`ele_con`,`building`) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)"
        )
        try:
            with connection.cursor() as cursor:
                cursor.executemany(sql, rows)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return len(rows)

    def close(self) -> None:
        connection, self._connection = self._connection, None
        if connection is not None:
            connection.close()

