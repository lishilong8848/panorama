from __future__ import annotations

import math
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List

from app.modules.alarm_export.core.field_type_converter import build_field_meta_map, convert_alarm_row_by_field_meta
from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms


@dataclass(frozen=True)
class _PowerAlertTable:
    key: str
    name: str
    table_id: str
    view_id: str
    threshold: float
    enabled: bool = True
    app_token: str = ""


@dataclass(frozen=True)
class _SourceRow:
    building: str
    room: str
    room_short: str
    line_raw: str
    line: Dict[str, Any]
    pdu: str
    pdu_info: Dict[str, Any]
    branch_no: str
    powers: List[float]


class PowerAlertSyncService:
    """Generate branch-power derived Base tables using the existing Feishu app credentials."""

    DEFAULT_APP_TOKEN = "ASLxbfESPahdTKs0A9NccgbrnXc"
    DEFAULT_SOURCE_TABLE_ID = "tblT5KbsxGCK1SwA"
    HOURS = list(range(24))

    SOURCE_FIELDS = {
        "building": "机楼",
        "room": "包间",
        "line": "机列",
        # The current daily branch table stores the physical PDU code in
        # “支路编号” and the numeric branch number in “PDU编号”.
        "pdu": "支路编号",
        "branch_no": "PDU编号",
    }
    TARGET_FIELDS = {
        "branch": [
            "序号",
            "数据时间",
            "机房",
            "楼栋",
            "房间",
            "PDU编号",
            "支路号",
            "支路编号",
            "支路功率",
            "对侧PDU编号",
            "对侧支路功率",
            "采集时间点",
            "时长",
            "备注",
        ],
        "cabinet": [
            "序号",
            "数据时间",
            "机房",
            "楼栋",
            "房间",
            "机柜号",
            "机柜功率",
            "PDU编号",
            "电流值",
            "是否负载不均匀",
            "次数",
            "时长",
            "备注",
        ],
        "line_head": [
            "序号",
            "数据时间",
            "机房",
            "楼栋",
            "房间",
            "机列",
            "功率",
            "对侧机列",
            "对侧机列最大功率",
            "次数",
            "时长",
            "备注",
        ],
        "row_line": [
            "序号",
            "数据时间",
            "机房",
            "楼栋",
            "房间",
            "机列",
            "功率",
            "次数",
            "时长",
            "备注",
        ],
    }

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config if isinstance(config, dict) else {}

    def _emit(self, emit_log: Callable[[str], None], text: str) -> None:
        try:
            emit_log(text)
        except Exception:  # noqa: BLE001
            pass

    def _cfg(self) -> Dict[str, Any]:
        features = self.config.get("features", {})
        features = features if isinstance(features, dict) else {}
        feature_branch_cfg = features.get("branch_power_upload", {})
        feature_branch_cfg = feature_branch_cfg if isinstance(feature_branch_cfg, dict) else {}
        branch_cfg = self.config.get("branch_power_upload", {})
        branch_cfg = branch_cfg if isinstance(branch_cfg, dict) else {}

        merged: Dict[str, Any] = {}
        for source in (feature_branch_cfg.get("power_alert_sync", {}), branch_cfg.get("power_alert_sync", {})):
            if isinstance(source, dict):
                merged.update(source)
        return merged

    @staticmethod
    def _bool(value: Any, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y", "on", "启用", "是"}:
            return True
        if text in {"0", "false", "no", "n", "off", "禁用", "否"}:
            return False
        return default

    @staticmethod
    def _as_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except Exception:  # noqa: BLE001
            return default

    @staticmethod
    def _as_float(value: Any, default: float) -> float:
        try:
            number = float(value)
            if math.isnan(number) or math.isinf(number):
                return default
            return number
        except Exception:  # noqa: BLE001
            return default

    @staticmethod
    def _text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, dict):
            for key in ("text", "name", "value"):
                item = value.get(key)
                if item is not None:
                    return PowerAlertSyncService._text(item)
            return ""
        if isinstance(value, (list, tuple, set)):
            for item in value:
                text = PowerAlertSyncService._text(item)
                if text:
                    return text
            return ""
        return str(value).strip()

    @classmethod
    def _source_power_field(cls, hour: int) -> str:
        return f"功率-{hour}:00"

    @classmethod
    def _default_tables_cfg(cls) -> Dict[str, Dict[str, Any]]:
        return {
            "source": {
                "name": "机柜功率每日明细",
                "table_id": cls.DEFAULT_SOURCE_TABLE_ID,
                "view_id": "",
            },
            "branch": {
                "name": "单支路超6.25KW功率",
                "table_id": "",
                "view_id": "",
                "threshold": 6.25,
            },
            "cabinet": {
                "name": "机柜超18KW统计",
                "table_id": "",
                "view_id": "",
                "threshold": 18,
            },
            "line_head": {
                "name": "列头柜超107.5功率统计",
                "table_id": "",
                "view_id": "",
                "threshold": 107.5,
            },
            "row_line": {
                "name": "机列超215KW功率统计",
                "table_id": "",
                "view_id": "",
                "threshold": 215,
            },
        }

    def _table_cfg(self, cfg: Dict[str, Any], key: str) -> Dict[str, Any]:
        defaults = self._default_tables_cfg().get(key, {})
        tables_cfg = cfg.get("tables", {}) if isinstance(cfg.get("tables", {}), dict) else {}
        raw = tables_cfg.get(key, {})
        legacy_key = {"line_head": "lineHead", "row_line": "rowLine"}.get(key, key)
        if (not raw) and legacy_key != key:
            raw = tables_cfg.get(legacy_key, {})
        if not isinstance(raw, dict):
            raw = tables_cfg.get(legacy_key, {})
        if not isinstance(raw, dict):
            raw = {}
        direct = cfg.get(key, {})
        if isinstance(direct, dict):
            raw = {**raw, **direct}
        return {**defaults, **raw}

    def _app_token(self, cfg: Dict[str, Any], table_cfg: Dict[str, Any] | None = None) -> str:
        table_cfg = table_cfg if isinstance(table_cfg, dict) else {}
        return (
            self._text(table_cfg.get("app_token"))
            or self._text(table_cfg.get("appToken"))
            or self._text(cfg.get("app_token"))
            or self._text(cfg.get("appToken"))
            or self._text(cfg.get("base_token"))
            or self._text(cfg.get("baseToken"))
            or self.DEFAULT_APP_TOKEN
        )

    def _table_id(self, table_cfg: Dict[str, Any]) -> str:
        return self._text(table_cfg.get("table_id")) or self._text(table_cfg.get("tableId"))

    def _view_id(self, table_cfg: Dict[str, Any]) -> str:
        return self._text(table_cfg.get("view_id")) or self._text(table_cfg.get("viewId"))

    def _resolve_source_table(self, cfg: Dict[str, Any]) -> _PowerAlertTable:
        source_cfg = self._table_cfg(cfg, "source")
        source_cfg["table_id"] = self._table_id(source_cfg) or self.DEFAULT_SOURCE_TABLE_ID
        return _PowerAlertTable(
            key="source",
            name=self._text(source_cfg.get("name")) or "机柜功率每日明细",
            table_id=self._table_id(source_cfg),
            view_id=self._view_id(source_cfg),
            threshold=0,
            enabled=True,
            app_token=self._app_token(cfg, source_cfg),
        )

    def _resolve_target_tables(self, cfg: Dict[str, Any]) -> tuple[List[_PowerAlertTable], List[str]]:
        targets: List[_PowerAlertTable] = []
        missing: List[str] = []
        for key in ("branch", "cabinet", "line_head", "row_line"):
            table_cfg = self._table_cfg(cfg, key)
            if not self._bool(table_cfg.get("enabled"), True):
                continue
            table_id = self._table_id(table_cfg)
            if not table_id:
                missing.append(f"tables.{key}.table_id")
                continue
            targets.append(
                _PowerAlertTable(
                    key=key,
                    name=self._text(table_cfg.get("name")) or key,
                    table_id=table_id,
                    view_id=self._view_id(table_cfg),
                    threshold=self._as_float(table_cfg.get("threshold"), self._default_tables_cfg()[key]["threshold"]),
                    enabled=True,
                    app_token=self._app_token(cfg, table_cfg),
                )
            )
        return targets, missing

    def _new_client(
        self,
        *,
        app_token: str,
        table_id: str,
        emit_log: Callable[[str], None],
    ) -> FeishuBitableClient:
        auth = require_feishu_auth_settings(self.config)
        return FeishuBitableClient(
            app_id=str(auth.get("app_id", "") or "").strip(),
            app_secret=str(auth.get("app_secret", "") or "").strip(),
            app_token=app_token,
            calc_table_id=table_id,
            attachment_table_id=table_id,
            timeout=int(auth.get("timeout", 30) or 30),
            request_retry_count=int(auth.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(auth.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=date_text_to_timestamp_ms,
            canonical_metric_name_fn=lambda value: str(value or "").strip(),
            dimension_mapping={},
            emit_log=emit_log,
        )

    @staticmethod
    def _field_name(field: Dict[str, Any]) -> str:
        return str(field.get("field_name") or field.get("name") or "").strip()

    def _field_meta_map(self, client: FeishuBitableClient, table: _PowerAlertTable) -> Dict[str, Dict[str, Any]]:
        fields = client.list_fields(table.table_id, page_size=200)
        return build_field_meta_map(fields)

    def _require_fields(self, *, field_meta: Dict[str, Dict[str, Any]], table_name: str, names: Iterable[str]) -> None:
        missing = [name for name in names if name not in field_meta]
        if missing:
            raise RuntimeError(f"{table_name} 缺少字段: {', '.join(missing[:30])}")

    @staticmethod
    def _number_or_zero(value: Any) -> float:
        if value is None or isinstance(value, bool):
            return 0.0
        if isinstance(value, (int, float)):
            number = float(value)
            return 0.0 if math.isnan(number) or math.isinf(number) else number
        text = str(value).strip().replace(",", "").replace("，", "")
        if not text:
            return 0.0
        try:
            number = float(text)
            return 0.0 if math.isnan(number) or math.isinf(number) else number
        except ValueError:
            return 0.0

    @staticmethod
    def _parse_line(value: Any) -> Dict[str, Any]:
        match = re.match(r"^(.+)-([A-Z])列-(AC|DC)(\d+)$", str(value or "").strip(), re.IGNORECASE)
        if not match:
            return {}
        return {
            "room_short": match.group(1),
            "col": match.group(2).upper(),
            "type": match.group(3).upper(),
            "num": match.group(4).zfill(3),
        }

    @staticmethod
    def _parse_pdu(value: Any) -> Dict[str, Any]:
        match = re.match(r"^([A-Z])0*(\d+)-([AB])([12])$", str(value or "").strip(), re.IGNORECASE)
        if not match:
            return {}
        number = int(match.group(2))
        return {
            "col": match.group(1).upper(),
            "num": number,
            "num_pad2": str(number).zfill(2),
            "side": match.group(3).upper(),
            "feed": match.group(4),
        }

    @classmethod
    def _normalize_source_row(cls, fields: Dict[str, Any]) -> _SourceRow | None:
        building = cls._text(fields.get(cls.SOURCE_FIELDS["building"]))
        room = cls._text(fields.get(cls.SOURCE_FIELDS["room"]))
        line_raw = cls._text(fields.get(cls.SOURCE_FIELDS["line"]))
        pdu = cls._text(fields.get(cls.SOURCE_FIELDS["pdu"]))
        branch_no = cls._text(fields.get(cls.SOURCE_FIELDS["branch_no"]))
        line = cls._parse_line(line_raw)
        pdu_info = cls._parse_pdu(pdu)
        if not all((building, room, line_raw, pdu, branch_no, line, pdu_info)):
            return None
        powers = [cls._number_or_zero(fields.get(cls._source_power_field(hour))) for hour in cls.HOURS]
        return _SourceRow(
            building=building,
            room=room,
            room_short=re.sub(r"包间$", "", room),
            line_raw=line_raw,
            line=line,
            pdu=pdu,
            pdu_info=pdu_info,
            branch_no=branch_no,
            powers=powers,
        )

    def _read_source_rows(
        self,
        *,
        client: FeishuBitableClient,
        source_table: _PowerAlertTable,
        page_size: int,
        emit_log: Callable[[str], None],
    ) -> List[_SourceRow]:
        required = [
            self.SOURCE_FIELDS["building"],
            self.SOURCE_FIELDS["room"],
            self.SOURCE_FIELDS["line"],
            self.SOURCE_FIELDS["pdu"],
            self.SOURCE_FIELDS["branch_no"],
            *[self._source_power_field(hour) for hour in self.HOURS],
        ]
        self._require_fields(
            field_meta=self._field_meta_map(client, source_table),
            table_name=source_table.name,
            names=required,
        )
        records = client.list_records(
            table_id=source_table.table_id,
            page_size=page_size,
            max_records=0,
            view_id=source_table.view_id,
            field_names=required,
        )
        rows: List[_SourceRow] = []
        for record in records:
            if not isinstance(record, dict):
                continue
            fields = record.get("fields", {})
            if not isinstance(fields, dict):
                continue
            row = self._normalize_source_row(fields)
            if row is not None:
                rows.append(row)
        self._emit(
            emit_log,
            f"[动环功率统计同步] 主表读取完成 table_id={source_table.table_id}, records={len(records)}, valid_rows={len(rows)}",
        )
        return rows

    @staticmethod
    def _branch_key(room: str, pdu: str, branch_no: str) -> str:
        return f"{room}||{pdu}||{branch_no}"

    @classmethod
    def _build_branch_index(cls, rows: List[_SourceRow]) -> Dict[str, _SourceRow]:
        output: Dict[str, _SourceRow] = {}
        for row in rows:
            output.setdefault(cls._branch_key(row.room, row.pdu, row.branch_no), row)
        return output

    @staticmethod
    def _branch_complement(key: str) -> Dict[str, str] | None:
        return {
            "A2|38": {"side": "B", "feed": "1", "branch_no": "1"},
            "B1|1": {"side": "A", "feed": "2", "branch_no": "38"},
            "A1|37": {"side": "B", "feed": "2", "branch_no": "19"},
            "B2|19": {"side": "A", "feed": "1", "branch_no": "37"},
        }.get(key)

    def _find_opposite_branch(self, row: _SourceRow, index: Dict[str, _SourceRow]) -> _SourceRow | None:
        pdu = row.pdu_info
        opposite_side = "B" if pdu.get("side") == "A" else "A"
        exact_pdu = f"{pdu.get('col')}{pdu.get('num_pad2')}-{opposite_side}{pdu.get('feed')}"
        exact = index.get(self._branch_key(row.room, exact_pdu, row.branch_no))
        if exact:
            return exact
        complement = self._branch_complement(f"{pdu.get('side')}{pdu.get('feed')}|{row.branch_no}")
        if not complement:
            return None
        complement_pdu = f"{pdu.get('col')}{pdu.get('num_pad2')}-{complement['side']}{complement['feed']}"
        return index.get(self._branch_key(row.room, complement_pdu, complement["branch_no"]))

    @staticmethod
    def _max_of(values: List[float]) -> float:
        return max(values) if values else 0.0

    @classmethod
    def _threshold_stats(cls, values: List[float], threshold: float) -> Dict[str, Any]:
        over_count = 0
        runs = 0
        was_over = False
        max_value = -math.inf
        max_hour = 0
        for hour, value in enumerate(values):
            over = value > threshold
            if over:
                over_count += 1
                if not was_over:
                    runs += 1
                if value >= max_value:
                    max_value = value
                    max_hour = hour
            was_over = over
        return {
            "over_count": over_count,
            "runs": runs,
            "max_value": max_value if over_count else cls._max_of(values),
            "max_hour": max_hour,
        }

    @staticmethod
    def _fmt_trim(value: Any, digits: int) -> str:
        try:
            number = float(value)
            if math.isnan(number) or math.isinf(number):
                return ""
            rounded = round(number, digits)
            return ("%.*f" % (digits, rounded)).rstrip("0").rstrip(".")
        except Exception:  # noqa: BLE001
            return ""

    @staticmethod
    def _line_display(line: Dict[str, Any]) -> str:
        return f"{line.get('col')}列-{line.get('type')}{line.get('num')}"

    @classmethod
    def _make_branch_code(cls, pdu_text: str, branch_no: str) -> str | None:
        pdu = cls._parse_pdu(pdu_text)
        if not pdu or not str(branch_no or "").strip():
            return None
        power_type = "AC" if pdu.get("side") == "A" else "DC"
        return f"{pdu.get('col')}列-{power_type}{str(pdu.get('num')).zfill(3)} #{str(branch_no).strip()}"

    @staticmethod
    def _sum_by_hour(rows: List[_SourceRow]) -> List[float]:
        return [sum(row.powers[hour] for row in rows) for hour in range(24)]

    @staticmethod
    def _group_by(rows: List[_SourceRow], key_fn: Callable[[_SourceRow], str | None]) -> Dict[str, List[_SourceRow]]:
        output: Dict[str, List[_SourceRow]] = {}
        for row in rows:
            key = key_fn(row)
            if not key:
                continue
            output.setdefault(key, []).append(row)
        return output

    @staticmethod
    def _compare_pdu_key(row: _SourceRow) -> tuple[str, int, str]:
        pdu = row.pdu_info
        return (str(pdu.get("side", "")), int(pdu.get("feed", 0) or 0), row.pdu)

    @classmethod
    def _opposite_line_raw(cls, line_raw: str) -> str | None:
        line = cls._parse_line(line_raw)
        if not line:
            return None
        opposite_type = "DC" if line.get("type") == "AC" else "AC"
        return f"{line.get('room_short')}-{line.get('col')}列-{opposite_type}{line.get('num')}"

    def _generate_branch_rows(
        self,
        rows: List[_SourceRow],
        *,
        threshold: float,
        report_date: str,
    ) -> List[Dict[str, Any]]:
        index = self._build_branch_index(rows)
        output: List[Dict[str, Any]] = []
        for row in rows:
            stats = self._threshold_stats(row.powers, threshold)
            if not int(stats["over_count"] or 0):
                continue
            opposite = self._find_opposite_branch(row, index)
            max_hour = int(stats["max_hour"])
            output.append(
                {
                    "序号": len(output) + 1,
                    "数据时间": report_date,
                    "机房": row.line_raw,
                    "楼栋": row.building,
                    "房间": row.room,
                    "PDU编号": row.pdu,
                    "支路号": row.branch_no,
                    "支路编号": self._make_branch_code(row.pdu, row.branch_no),
                    "支路功率": self._fmt_trim(stats["max_value"], 3),
                    "对侧PDU编号": opposite.pdu if opposite else None,
                    "对侧支路功率": self._fmt_trim(opposite.powers[max_hour], 3) if opposite else None,
                    "采集时间点": f"{max_hour}:00",
                    "时长": f"{stats['over_count']}h",
                    "备注": None,
                }
            )
        return output

    def _generate_cabinet_rows(
        self,
        rows: List[_SourceRow],
        *,
        threshold: float,
        report_date: str,
    ) -> List[Dict[str, Any]]:
        groups = self._group_by(rows, lambda row: f"{row.room}||{row.pdu_info.get('col')}{row.pdu_info.get('num_pad2')}")
        output: List[Dict[str, Any]] = []
        for group in groups.values():
            stats = self._threshold_stats(self._sum_by_hour(group), threshold)
            if not int(stats["over_count"] or 0):
                continue
            first = group[0]
            pdu = first.pdu_info
            for item in sorted(group, key=self._compare_pdu_key):
                output.append(
                    {
                        "序号": str(len(output) + 1),
                        "数据时间": report_date,
                        "机房": f"{first.room_short}-{pdu.get('col')}列",
                        "楼栋": first.building,
                        "房间": first.room,
                        "机柜号": f"{pdu.get('col')}列{pdu.get('col')}{pdu.get('num_pad2')}",
                        "机柜功率": f"{self._fmt_trim(stats['max_value'], 2)}kw",
                        "PDU编号": item.pdu,
                        "电流值": float(self._fmt_trim(item.powers[int(stats["max_hour"])], 3) or 0),
                        "是否负载不均匀": "均匀",
                        "次数": stats["runs"],
                        "时长": f"{stats['over_count']}h",
                        "备注": None,
                    }
                )
        return output

    def _generate_line_head_rows(
        self,
        rows: List[_SourceRow],
        *,
        threshold: float,
        report_date: str,
        data_center_name: str,
    ) -> List[Dict[str, Any]]:
        groups = self._group_by(rows, lambda row: row.line_raw)
        group_stats = {key: {"group": group, "totals": self._sum_by_hour(group)} for key, group in groups.items()}
        output: List[Dict[str, Any]] = []
        for data in group_stats.values():
            stats = self._threshold_stats(data["totals"], threshold)
            if not int(stats["over_count"] or 0):
                continue
            first = data["group"][0]
            opposite_key = self._opposite_line_raw(first.line_raw)
            opposite = group_stats.get(opposite_key or "")
            opposite_max = self._max_of(opposite["totals"]) if opposite else None
            output.append(
                {
                    "序号": len(output) + 1,
                    "数据时间": report_date,
                    "机房": data_center_name,
                    "楼栋": first.building,
                    "房间": f"{first.room_short}.{data_center_name}",
                    "机列": self._line_display(first.line),
                    "功率": f"{self._fmt_trim(stats['max_value'], 3)}kw",
                    "对侧机列": self._line_display(opposite["group"][0].line) if opposite else None,
                    "对侧机列最大功率": f"{self._fmt_trim(opposite_max, 3)}kw" if opposite else None,
                    "次数": stats["runs"],
                    "时长": f"{stats['over_count']}h",
                    "备注": None,
                }
            )
        return output

    def _generate_row_line_rows(
        self,
        rows: List[_SourceRow],
        *,
        threshold: float,
        report_date: str,
        data_center_name: str,
    ) -> List[Dict[str, Any]]:
        groups = self._group_by(rows, lambda row: f"{row.room}||{row.line.get('col')}" if row.line else None)
        output: List[Dict[str, Any]] = []
        for group in groups.values():
            stats = self._threshold_stats(self._sum_by_hour(group), threshold)
            if not int(stats["over_count"] or 0):
                continue
            first = group[0]
            output.append(
                {
                    "序号": len(output) + 1,
                    "数据时间": report_date,
                    "机房": data_center_name,
                    "楼栋": first.building,
                    "房间": f"{first.room_short}.{data_center_name}",
                    "机列": f"{first.line.get('col')}列",
                    "功率": f"{self._fmt_trim(stats['max_value'], 3)}KW",
                    "次数": stats["runs"],
                    "时长": f"{stats['over_count']}h",
                    "备注": None,
                }
            )
        return output

    def _generate_all_targets(
        self,
        rows: List[_SourceRow],
        *,
        target_tables: List[_PowerAlertTable],
        report_date: str,
        data_center_name: str,
    ) -> Dict[str, List[Dict[str, Any]]]:
        by_key = {table.key: table for table in target_tables}
        output: Dict[str, List[Dict[str, Any]]] = {}
        if "branch" in by_key:
            output["branch"] = self._generate_branch_rows(
                rows,
                threshold=by_key["branch"].threshold,
                report_date=report_date,
            )
        if "cabinet" in by_key:
            output["cabinet"] = self._generate_cabinet_rows(
                rows,
                threshold=by_key["cabinet"].threshold,
                report_date=report_date,
            )
        if "line_head" in by_key:
            output["line_head"] = self._generate_line_head_rows(
                rows,
                threshold=by_key["line_head"].threshold,
                report_date=report_date,
                data_center_name=data_center_name,
            )
        if "row_line" in by_key:
            output["row_line"] = self._generate_row_line_rows(
                rows,
                threshold=by_key["row_line"].threshold,
                report_date=report_date,
                data_center_name=data_center_name,
            )
        return output

    @staticmethod
    def _normalize_date(value: Any) -> str:
        text = PowerAlertSyncService._text(value).replace("-", "/")
        match = re.match(r"^(\d{4})/(\d{1,2})/(\d{1,2})$", text[:10])
        if not match:
            raise ValueError(f"日期格式无效: {value}")
        return f"{int(match.group(1)):04d}/{int(match.group(2)):02d}/{int(match.group(3)):02d}"

    @staticmethod
    def _normalize_existing_date(value: Any) -> str:
        if isinstance(value, (list, tuple, set)):
            for item in value:
                text = PowerAlertSyncService._normalize_existing_date(item)
                if text:
                    return text
            return ""
        if isinstance(value, dict):
            for key in ("timestamp", "value", "text", "name"):
                if key in value:
                    text = PowerAlertSyncService._normalize_existing_date(value.get(key))
                    if text:
                        return text
            return ""
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            number = float(value)
            if math.isnan(number) or math.isinf(number):
                return ""
            if number > 1e12:
                seconds = number / 1000
            elif number > 1e10:
                seconds = number / 1000
            elif number > 1e9:
                seconds = number
            else:
                return ""
            dt = datetime.fromtimestamp(seconds, timezone(timedelta(hours=8)))
            return dt.strftime("%Y/%m/%d")
        text = str(value or "").strip()
        if not text:
            return ""
        for pattern in (
            r"(\d{4})[-/\.年](\d{1,2})[-/\.月](\d{1,2})",
            r"(\d{4})(\d{2})(\d{2})",
        ):
            match = re.search(pattern, text)
            if match:
                return f"{int(match.group(1)):04d}/{int(match.group(2)):02d}/{int(match.group(3)):02d}"
        return text

    def _target_same_date_record_ids(
        self,
        *,
        client: FeishuBitableClient,
        table: _PowerAlertTable,
        report_date: str,
        page_size: int,
        emit_log: Callable[[str], None] = print,
    ) -> List[str]:
        # Delete by date across the full target table. A configured view may filter out
        # old rows, so it must not limit the replacement scan.
        date_field = "数据时间"
        records = client.list_records(
            table_id=table.table_id,
            page_size=page_size,
            max_records=0,
            field_names=[date_field],
        )
        output: List[str] = []
        target = self._normalize_date(report_date)
        for item in records:
            if not isinstance(item, dict):
                continue
            fields = item.get("fields", {})
            if not isinstance(fields, dict):
                continue
            if self._normalize_existing_date(fields.get("数据时间")) != target:
                continue
            record_id = self._text(item.get("record_id"))
            if record_id:
                output.append(record_id)
        return output

    def _convert_target_rows(
        self,
        *,
        rows: List[Dict[str, Any]],
        field_meta: Dict[str, Dict[str, Any]],
        field_names: List[str],
    ) -> List[Dict[str, Any]]:
        converted: List[Dict[str, Any]] = []
        for row in rows:
            payload = {name: row.get(name) for name in field_names if row.get(name) is not None}
            converted_row, _stats = convert_alarm_row_by_field_meta(payload, field_meta, tz_offset_hours=8)
            converted.append(
                {
                    key: value
                    for key, value in converted_row.items()
                    if value is not None and self._text(value) != ""
                }
            )
        return converted

    def _replace_target_rows(
        self,
        *,
        client: FeishuBitableClient,
        table: _PowerAlertTable,
        rows: List[Dict[str, Any]],
        report_date: str,
        dry_run: bool,
        page_size: int,
        batch_size: int,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        field_names = self.TARGET_FIELDS[table.key]
        field_meta = self._field_meta_map(client, table)
        self._require_fields(field_meta=field_meta, table_name=table.name, names=field_names)
        same_date_ids = self._target_same_date_record_ids(
            client=client,
            table=table,
            report_date=report_date,
            page_size=page_size,
            emit_log=emit_log,
        )
        self._emit(
            emit_log,
            f"[动环功率统计同步] 目标表计划 table={table.name}, generated={len(rows)}, "
            f"same_date_existing={len(same_date_ids)}, mode={'dry_run' if dry_run else 'replace'}",
        )
        if dry_run:
            return {
                "table": table.name,
                "table_id": table.table_id,
                "generated": len(rows),
                "deleted": 0,
                "created": 0,
                "same_date_existing": len(same_date_ids),
                "dry_run": True,
            }
        deleted = 0
        if same_date_ids:
            deleted = client.batch_delete_records(
                table_id=table.table_id,
                record_ids=same_date_ids,
                batch_size=batch_size,
            )
        converted_rows = self._convert_target_rows(rows=rows, field_meta=field_meta, field_names=field_names)
        if converted_rows:
            client.batch_create_records(
                table_id=table.table_id,
                fields_list=converted_rows,
                batch_size=batch_size,
            )
        self._emit(
            emit_log,
            f"[动环功率统计同步] 目标表完成 table={table.name}, deleted={deleted}, created={len(converted_rows)}",
        )
        return {
            "table": table.name,
            "table_id": table.table_id,
            "generated": len(rows),
            "deleted": deleted,
            "created": len(converted_rows),
            "same_date_existing": len(same_date_ids),
            "dry_run": False,
        }

    def sync(
        self,
        *,
        report_date: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        started = time.perf_counter()
        cfg = self._cfg()
        if not self._bool(cfg.get("enabled"), True):
            return {"ok": True, "status": "skipped", "reason": "disabled"}

        required = self._bool(cfg.get("required"), False)
        dry_run = self._bool(cfg.get("dry_run"), False)
        page_size = max(1, self._as_int(cfg.get("page_size"), 500))
        batch_size = max(1, min(500, self._as_int(cfg.get("batch_size"), 200)))
        data_center_name = self._text(cfg.get("data_center_name")) or self._text(cfg.get("dataCenterName")) or "EA118"
        report_date_slash = self._normalize_date(report_date)

        source_table = self._resolve_source_table(cfg)
        target_tables, missing = self._resolve_target_tables(cfg)
        if missing or not target_tables:
            message = "；".join(missing or ["未配置任何动环功率统计目标表"])
            if required:
                raise RuntimeError(f"动环功率统计同步配置缺失: {message}")
            self._emit(emit_log, f"[动环功率统计同步] 已跳过: {message}")
            return {
                "ok": True,
                "status": "skipped",
                "reason": "missing_target_table_config",
                "missing": missing,
            }

        self._emit(
            emit_log,
            f"[动环功率统计同步] 开始 date={report_date_slash}, source_table={source_table.table_id}, "
            f"targets={','.join(table.name for table in target_tables)}, mode={'dry_run' if dry_run else 'write'}",
        )

        clients: Dict[str, FeishuBitableClient] = {}

        def _client_for(table: _PowerAlertTable) -> FeishuBitableClient:
            if table.app_token not in clients:
                clients[table.app_token] = self._new_client(
                    app_token=table.app_token,
                    table_id=table.table_id,
                    emit_log=emit_log,
                )
            return clients[table.app_token]

        try:
            source_rows = self._read_source_rows(
                client=_client_for(source_table),
                source_table=source_table,
                page_size=page_size,
                emit_log=emit_log,
            )
            if not source_rows:
                raise RuntimeError("动环功率主表没有可用数据，拒绝覆盖目标统计表")
            generated = self._generate_all_targets(
                source_rows,
                target_tables=target_tables,
                report_date=report_date_slash,
                data_center_name=data_center_name,
            )
            results: Dict[str, Any] = {}
            for table in target_tables:
                rows = generated.get(table.key, [])
                results[table.key] = self._replace_target_rows(
                    client=_client_for(table),
                    table=table,
                    rows=rows,
                    report_date=report_date_slash,
                    dry_run=dry_run,
                    page_size=page_size,
                    batch_size=batch_size,
                    emit_log=emit_log,
                )
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            self._emit(emit_log, f"[动环功率统计同步] 完成 date={report_date_slash}, elapsed_ms={elapsed_ms}")
            return {
                "ok": True,
                "status": "success",
                "report_date": report_date_slash,
                "dry_run": dry_run,
                "source_rows": len(source_rows),
                "targets": results,
                "elapsed_ms": elapsed_ms,
            }
        except Exception as exc:  # noqa: BLE001
            if required:
                raise
            message = f"动环功率统计同步失败: {exc}"
            self._emit(emit_log, f"[动环功率统计同步] 已记录失败，不阻断主流程: {message}")
            return {
                "ok": False,
                "status": "failed",
                "error": message,
                "report_date": report_date_slash,
                "dry_run": dry_run,
                "elapsed_ms": int((time.perf_counter() - started) * 1000),
            }
