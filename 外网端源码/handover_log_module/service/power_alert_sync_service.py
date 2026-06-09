from __future__ import annotations

import hashlib
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
from handover_log_module.repository.power_alert_stats_repository import PowerAlertStatsRepository


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
        self._stats_repository = PowerAlertStatsRepository.from_config(self.config)
        self._stats_repository_error_logged = False

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
        match = re.match(
            r"^(.+?)-([A-Z])列(?:(A|B)[路列])?-(AC|DC)-?0*(\d+)$",
            str(value or "").strip(),
            re.IGNORECASE,
        )
        if not match:
            return {}
        return {
            "room_short": match.group(1),
            "col": match.group(2).upper(),
            "route": (match.group(3) or "").upper(),
            "type": match.group(4).upper(),
            "num": match.group(5).zfill(3),
            "num_int": int(match.group(5)),
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

    @staticmethod
    def _branch_pdu_key(room: str, pdu: str) -> str:
        return f"{room}||{pdu}"

    @classmethod
    def _build_branch_index(cls, rows: List[_SourceRow]) -> Dict[str, Any]:
        by_branch: Dict[str, _SourceRow] = {}
        by_pdu: Dict[str, List[_SourceRow]] = {}
        for row in rows:
            by_branch.setdefault(cls._branch_key(row.room, row.pdu, row.branch_no), row)
            by_pdu.setdefault(cls._branch_pdu_key(row.room, row.pdu), []).append(row)
        return {"by_branch": by_branch, "by_pdu": by_pdu}

    def _find_opposite_branch(self, row: _SourceRow, index: Dict[str, Any]) -> _SourceRow | None:
        pdu = row.pdu_info
        opposite_side = "B" if pdu.get("side") == "A" else "A"
        exact_pdu = f"{pdu.get('col')}{pdu.get('num_pad2')}-{opposite_side}{pdu.get('feed')}"
        by_branch = index.get("by_branch", {}) if isinstance(index, dict) else {}
        exact = by_branch.get(self._branch_key(row.room, exact_pdu, row.branch_no))
        if exact is not None:
            return exact
        by_pdu = index.get("by_pdu", {}) if isinstance(index, dict) else {}
        candidates = by_pdu.get(self._branch_pdu_key(row.room, exact_pdu), [])
        return candidates[0] if len(candidates) == 1 else None

    @staticmethod
    def _expected_opposite_pdu(pdu_info: Dict[str, Any]) -> str:
        if not pdu_info:
            return ""
        opposite_side = "B" if pdu_info.get("side") == "A" else "A"
        return f"{pdu_info.get('col')}{pdu_info.get('num_pad2')}-{opposite_side}{pdu_info.get('feed')}"

    @staticmethod
    def _max_of(values: List[float]) -> float:
        return max(values) if values else 0.0

    @staticmethod
    def _stats_business_date_key(value: Any) -> str:
        text = str(value or "").strip().replace("/", "-")
        for fmt in ("%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(text[:10] if fmt == "%Y-%m-%d" else text, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return text[:10]

    @classmethod
    def _stats_previous_date_key(cls, value: Any) -> str:
        current = datetime.strptime(cls._stats_business_date_key(value), "%Y-%m-%d")
        return (current - timedelta(days=1)).strftime("%Y-%m-%d")

    @staticmethod
    def _power_alert_object_key(*parts: Any) -> str:
        normalized = [str(part or "").strip() for part in parts]
        return "||".join(normalized)

    @staticmethod
    def _stats_source_hash(values: List[float], threshold: float, *, source_hint: str = "") -> str:
        hour_values = list(values or [])[:24]
        text = "|".join([str(float(threshold or 0)), str(source_hint or ""), *[f"{float(value or 0):.6f}" for value in hour_values]])
        return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()

    def _lookup_previous_end_over(
        self,
        *,
        table_key: str,
        object_key: str,
        report_date: str,
        emit_log: Callable[[str], None] | None = None,
    ) -> bool | None:
        try:
            return self._stats_repository.get_end_over(
                table_key=table_key,
                business_date=self._stats_previous_date_key(report_date),
                object_key=object_key,
            )
        except Exception as exc:  # noqa: BLE001
            if not self._stats_repository_error_logged:
                self._stats_repository_error_logged = True
                if callable(emit_log):
                    self._emit(emit_log, f"[动环功率统计] 超限状态库读取失败，按当天独立计算: {exc}")
            return None

    def _persist_threshold_stats(
        self,
        stats: Dict[str, Any],
        *,
        table_key: str,
        object_key: str,
        report_date: str,
        threshold: float,
        source_file: str = "",
        source_hint: str = "",
        emit_log: Callable[[str], None] | None = None,
    ) -> None:
        try:
            self._stats_repository.upsert_stat(
                table_key=table_key,
                business_date=self._stats_business_date_key(report_date),
                object_key=object_key,
                threshold=float(threshold or 0),
                over_mask=int(stats.get("over_mask", 0) or 0),
                duration_hours=int(stats.get("over_count", 0) or 0),
                run_count=int(stats.get("runs", 0) or 0),
                max_hour=int(stats.get("max_hour", 0) or 0),
                max_value=float(stats.get("max_value", 0) or 0),
                end_over=bool(stats.get("end_over", False)),
                source_hash=str(stats.get("source_hash") or source_hint or ""),
                source_file=source_file,
                payload={
                    "over_hours": stats.get("over_hours", []),
                    "previous_end_over": stats.get("previous_end_over"),
                    "raw_runs": stats.get("raw_runs", 0),
                },
            )
        except Exception as exc:  # noqa: BLE001
            if not self._stats_repository_error_logged:
                self._stats_repository_error_logged = True
                if callable(emit_log):
                    self._emit(emit_log, f"[动环功率统计] 超限状态库写入失败，不阻断上传: {exc}")

    def _threshold_stats(
        self,
        values: List[float],
        threshold: float,
        *,
        table_key: str = "",
        object_key: str = "",
        report_date: str = "",
        previous_end_over: bool | None = None,
        source_file: str = "",
        source_hint: str = "",
        emit_log: Callable[[str], None] | None = None,
    ) -> Dict[str, Any]:
        hour_values = list(values or [])[:24]
        over_count = 0
        runs = 0
        raw_runs = 0
        was_over = False
        max_value = -math.inf
        max_hour = 0
        over_mask = 0
        over_hours: List[int] = []
        for hour, value in enumerate(hour_values):
            over = value > threshold
            if over:
                over_count += 1
                over_mask |= 1 << hour
                over_hours.append(hour)
                if not was_over:
                    runs += 1
                    raw_runs += 1
                if value >= max_value:
                    max_value = value
                    max_hour = hour
            was_over = over
        if previous_end_over is None and table_key and object_key and report_date:
            previous_end_over = self._lookup_previous_end_over(
                table_key=table_key,
                object_key=object_key,
                report_date=report_date,
                emit_log=emit_log,
            )
        if bool(previous_end_over) and bool(hour_values) and bool(over_mask & 1) and runs > 0:
            runs -= 1
        stats = {
            "over_count": over_count,
            "runs": runs,
            "raw_runs": raw_runs,
            "max_value": max_value if over_count else self._max_of(hour_values),
            "max_hour": max_hour,
            "over_mask": over_mask,
            "over_hours": over_hours,
            "end_over": bool(over_mask & (1 << 23)),
            "previous_end_over": previous_end_over,
        }
        stats["source_hash"] = self._stats_source_hash(hour_values, threshold, source_hint=source_hint or source_file)
        if table_key and object_key and report_date:
            self._persist_threshold_stats(
                stats,
                table_key=table_key,
                object_key=object_key,
                report_date=report_date,
                threshold=threshold,
                source_file=source_file,
                source_hint=source_hint,
                emit_log=emit_log,
            )
        return stats

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
        route = str(line.get("route") or "").strip().upper()
        route_text = f"{route}路" if route in {"A", "B"} else ""
        return f"{line.get('col')}列{route_text}-{line.get('type')}{line.get('num')}"

    @staticmethod
    def _make_branch_code(line: Dict[str, Any], branch_no: str) -> str | None:
        if not line or not str(branch_no or "").strip():
            return None
        col = str(line.get("col") or "").strip().upper()
        power_type = str(line.get("type") or "").strip().upper()
        number = str(line.get("num") or "").strip()
        if not col or power_type not in {"AC", "DC"} or not number:
            return None
        return f"{col}列-{power_type}{number.zfill(3)} #{str(branch_no).strip()}"

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

    @staticmethod
    def _line_room_number(line: Dict[str, Any]) -> str:
        match = re.search(r"(\d+)$", str(line.get("room_short") or ""))
        return match.group(1) if match else ""

    @staticmethod
    def _line_building_code(line: Dict[str, Any]) -> str:
        return str(line.get("room_short") or "").split("-", 1)[0].strip().upper()

    @classmethod
    def _is_third_floor_line(cls, line: Dict[str, Any]) -> bool:
        room_number = cls._line_room_number(line)
        return room_number.startswith("3")

    @staticmethod
    def _odd_even_pair_num(number: int, *, min_num: int, max_num: int) -> int | None:
        if number < min_num or number > max_num:
            return None
        if (number - min_num) % 2 == 0:
            pair = number + 1
        else:
            pair = number - 1
        return pair if min_num <= pair <= max_num else None

    @staticmethod
    def _line_spec_from(line: Dict[str, Any], *, line_type: str, number: int, route: str | None = None) -> Dict[str, Any]:
        return {
            "room_short": line.get("room_short"),
            "col": line.get("col"),
            "route": (route if route is not None else line.get("route") or ""),
            "type": line_type,
            "num": str(number).zfill(3),
            "num_int": int(number),
        }

    @classmethod
    def _special_opposite_line_spec(cls, line: Dict[str, Any]) -> Dict[str, Any] | None:
        if not cls._is_third_floor_line(line):
            return None

        building_code = cls._line_building_code(line)
        line_type = str(line.get("type") or "").upper()
        number = int(line.get("num_int") or 0)
        room_number = cls._line_room_number(line)

        if building_code in {"A", "B", "C", "D"}:
            if room_number.endswith("301"):
                if line_type == "AC" and 1 <= number <= 6:
                    return cls._line_spec_from(line, line_type="DC", number=number, route="")
                if line_type == "DC" and 1 <= number <= 6:
                    return cls._line_spec_from(line, line_type="AC", number=number, route="")
                if line_type == "DC":
                    pair = cls._odd_even_pair_num(number, min_num=7, max_num=14)
                    if pair is not None:
                        return cls._line_spec_from(line, line_type="DC", number=pair, route="")
            if room_number.endswith("302"):
                if line_type == "DC":
                    pair = cls._odd_even_pair_num(number, min_num=1, max_num=8)
                    if pair is not None:
                        return cls._line_spec_from(line, line_type="DC", number=pair, route="")
                    if 9 <= number <= 14:
                        return cls._line_spec_from(line, line_type="AC", number=number - 8, route="")
                if line_type == "AC" and 1 <= number <= 6:
                    return cls._line_spec_from(line, line_type="DC", number=number + 8, route="")

        if building_code == "E" and line_type == "DC":
            pair = cls._odd_even_pair_num(number, min_num=1, max_num=20)
            if pair is not None:
                route = str(line.get("route") or "").upper()
                if route == "A":
                    route = "B"
                elif route == "B":
                    route = "A"
                return cls._line_spec_from(line, line_type="DC", number=pair, route=route)

        return None

    @classmethod
    def _default_opposite_line_spec(cls, line: Dict[str, Any]) -> Dict[str, Any] | None:
        line_type = str(line.get("type") or "").upper()
        if line_type not in {"AC", "DC"}:
            return None
        opposite_type = "DC" if line_type == "AC" else "AC"
        return cls._line_spec_from(line, line_type=opposite_type, number=int(line.get("num_int") or 0), route="")

    @classmethod
    def _opposite_line_spec(cls, line_raw: str) -> Dict[str, Any] | None:
        line = cls._parse_line(line_raw)
        if not line:
            return None
        return cls._special_opposite_line_spec(line) or cls._default_opposite_line_spec(line)

    @staticmethod
    def _line_raw_from_spec(line: Dict[str, Any]) -> str:
        route = str(line.get("route") or "").strip().upper()
        route_text = f"{route}路" if route in {"A", "B"} else ""
        return f"{line.get('room_short')}-{line.get('col')}列{route_text}-{line.get('type')}{line.get('num')}"

    @classmethod
    def _opposite_line_raw(cls, line_raw: str) -> str | None:
        spec = cls._opposite_line_spec(line_raw)
        return cls._line_raw_from_spec(spec) if spec else None

    def _find_opposite_line_group(
        self,
        line_raw: str,
        group_stats: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any] | None:
        target = self._opposite_line_spec(line_raw)
        exact_key = self._line_raw_from_spec(target) if target else None
        if exact_key and exact_key in group_stats:
            return group_stats.get(exact_key)

        line = self._parse_line(line_raw)
        if not line or not target:
            return None
        exact_candidates: List[Dict[str, Any]] = []
        for candidate_key, candidate in group_stats.items():
            candidate_line = self._parse_line(candidate_key)
            if not candidate_line:
                continue
            if (
                candidate_line.get("room_short") == target.get("room_short")
                and candidate_line.get("col") == target.get("col")
                and candidate_line.get("type") == target.get("type")
                and candidate_line.get("num") == target.get("num")
                and (
                    not str(target.get("route") or "").strip()
                    or candidate_line.get("route") == target.get("route")
                )
            ):
                exact_candidates.append(candidate)
        if len(exact_candidates) == 1:
            return exact_candidates[0]
        if self._special_opposite_line_spec(line):
            return None

        opposite_type = "DC" if line.get("type") == "AC" else "AC"
        candidates: List[Dict[str, Any]] = []
        for candidate_key, candidate in group_stats.items():
            candidate_line = self._parse_line(candidate_key)
            if not candidate_line:
                continue
            if (
                candidate_line.get("room_short") == line.get("room_short")
                and candidate_line.get("col") == line.get("col")
                and candidate_line.get("type") == opposite_type
            ):
                candidates.append(candidate)
        return candidates[0] if len(candidates) == 1 else None

    def _generate_branch_rows(
        self,
        rows: List[_SourceRow],
        *,
        threshold: float,
        report_date: str,
        data_center_name: str = "EA118",
        emit_log: Callable[[str], None] | None = None,
    ) -> List[Dict[str, Any]]:
        index = self._build_branch_index(rows)
        output: List[Dict[str, Any]] = []
        for row in rows:
            object_key = self._power_alert_object_key(row.building, row.room, row.pdu, row.branch_no)
            stats = self._threshold_stats(
                row.powers,
                threshold,
                table_key="branch",
                object_key=object_key,
                report_date=report_date,
                source_hint=object_key,
                emit_log=emit_log,
            )
            if not int(stats["over_count"] or 0):
                continue
            opposite = self._find_opposite_branch(row, index)
            if opposite is None and callable(emit_log):
                expected_opposite = self._expected_opposite_pdu(row.pdu_info)
                self._emit(
                    emit_log,
                    "[动环功率统计][单支路] 未找到同柜同路对侧PDU，按空白上传: "
                    f"building={row.building}, room={row.room}, pdu={row.pdu}, "
                    f"branch_no={row.branch_no}, expected_opposite={expected_opposite or '-'}",
                )
            max_hour = int(stats["max_hour"])
            output.append(
                {
                    "序号": len(output) + 1,
                    "数据时间": report_date,
                    "机房": self._text(data_center_name) or "EA118",
                    "楼栋": row.building,
                    "房间": row.room,
                    "PDU编号": row.pdu,
                    "支路号": row.branch_no,
                    "支路编号": self._make_branch_code(row.line, row.branch_no),
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
        emit_log: Callable[[str], None] | None = None,
    ) -> List[Dict[str, Any]]:
        groups = self._group_by(rows, lambda row: f"{row.room}||{row.pdu_info.get('col')}{row.pdu_info.get('num_pad2')}")
        output: List[Dict[str, Any]] = []
        for group in groups.values():
            first = group[0]
            pdu = first.pdu_info
            cabinet_id = f"{pdu.get('col')}{pdu.get('num_pad2')}"
            object_key = self._power_alert_object_key(first.building, first.room, cabinet_id)
            stats = self._threshold_stats(
                self._sum_by_hour(group),
                threshold,
                table_key="cabinet",
                object_key=object_key,
                report_date=report_date,
                source_hint=object_key,
                emit_log=emit_log,
            )
            if not int(stats["over_count"] or 0):
                continue
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
        emit_log: Callable[[str], None] | None = None,
    ) -> List[Dict[str, Any]]:
        groups = self._group_by(rows, lambda row: row.line_raw)
        group_stats = {key: {"group": group, "totals": self._sum_by_hour(group)} for key, group in groups.items()}
        output: List[Dict[str, Any]] = []
        for data in group_stats.values():
            first = data["group"][0]
            object_key = self._power_alert_object_key(first.building, first.room_short, first.line_raw)
            stats = self._threshold_stats(
                data["totals"],
                threshold,
                table_key="line_head",
                object_key=object_key,
                report_date=report_date,
                source_hint=object_key,
                emit_log=emit_log,
            )
            if not int(stats["over_count"] or 0):
                continue
            opposite = self._find_opposite_line_group(first.line_raw, group_stats)
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
                    "对侧机列": self._line_display(opposite["group"][0].line) if opposite else "/",
                    "对侧机列最大功率": f"{self._fmt_trim(opposite_max, 3)}kw" if opposite else "/",
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
        emit_log: Callable[[str], None] | None = None,
    ) -> List[Dict[str, Any]]:
        groups = self._group_by(rows, lambda row: f"{row.room}||{row.line.get('col')}" if row.line else None)
        output: List[Dict[str, Any]] = []
        for group in groups.values():
            first = group[0]
            row_col = first.line.get("col") if first.line else ""
            object_key = self._power_alert_object_key(first.building, first.room_short, row_col)
            stats = self._threshold_stats(
                self._sum_by_hour(group),
                threshold,
                table_key="row_line",
                object_key=object_key,
                report_date=report_date,
                source_hint=object_key,
                emit_log=emit_log,
            )
            if not int(stats["over_count"] or 0):
                continue
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
        emit_log: Callable[[str], None] | None = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        by_key = {table.key: table for table in target_tables}
        output: Dict[str, List[Dict[str, Any]]] = {}
        if "branch" in by_key:
            output["branch"] = self._generate_branch_rows(
                rows,
                threshold=by_key["branch"].threshold,
                report_date=report_date,
                data_center_name=data_center_name,
                emit_log=emit_log,
            )
        if "cabinet" in by_key:
            output["cabinet"] = self._generate_cabinet_rows(
                rows,
                threshold=by_key["cabinet"].threshold,
                report_date=report_date,
                emit_log=emit_log,
            )
        if "line_head" in by_key:
            output["line_head"] = self._generate_line_head_rows(
                rows,
                threshold=by_key["line_head"].threshold,
                report_date=report_date,
                data_center_name=data_center_name,
                emit_log=emit_log,
            )
        if "row_line" in by_key:
            output["row_line"] = self._generate_row_line_rows(
                rows,
                threshold=by_key["row_line"].threshold,
                report_date=report_date,
                data_center_name=data_center_name,
                emit_log=emit_log,
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
            f"[动环功率统计同步] 目标表计划 date={report_date}, table={table.name}, table_id={table.table_id}, "
            f"generated={len(rows)}, same_date_existing={len(same_date_ids)}, "
            f"mode={'dry_run' if dry_run else 'replace'}",
        )
        if dry_run:
            self._emit(
                emit_log,
                f"[动环功率统计同步] 目标表完成 date={report_date}, table={table.name}, table_id={table.table_id}, "
                f"generated={len(rows)}, deleted=0, created=0, same_date_existing={len(same_date_ids)}, dry_run=true",
            )
            return {
                "table": table.name,
                "table_id": table.table_id,
                "generated": len(rows),
                "deleted": 0,
                "created": 0,
                "same_date_existing": len(same_date_ids),
                "dry_run": True,
            }
        converted_rows = self._convert_target_rows(rows=rows, field_meta=field_meta, field_names=field_names)
        # Create first so a transient Feishu create failure cannot wipe the
        # previous same-date records. If there are no new rows, deleting old
        # records is still the correct representation for "no over-limit data".
        if converted_rows:
            client.batch_create_records(
                table_id=table.table_id,
                fields_list=converted_rows,
                batch_size=batch_size,
            )
        deleted = 0
        if same_date_ids:
            deleted = client.batch_delete_records(
                table_id=table.table_id,
                record_ids=same_date_ids,
                batch_size=batch_size,
            )
        self._emit(
            emit_log,
            f"[动环功率统计同步] 目标表完成 date={report_date}, table={table.name}, table_id={table.table_id}, "
            f"generated={len(rows)}, deleted={deleted}, created={len(converted_rows)}, "
            f"same_date_existing={len(same_date_ids)}",
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
        only_keys: List[str] | None = None,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        return self._sync_impl(report_date=report_date, emit_log=emit_log, source_records=None, only_keys=only_keys)

    def sync_from_source_records(
        self,
        *,
        report_date: str,
        source_records: List[Dict[str, Any]],
        only_keys: List[str] | None = None,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        return self._sync_impl(
            report_date=report_date,
            emit_log=emit_log,
            source_records=source_records,
            only_keys=only_keys,
        )

    def _sync_impl(
        self,
        *,
        report_date: str,
        emit_log: Callable[[str], None],
        source_records: List[Dict[str, Any]] | None,
        only_keys: List[str] | None,
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
        if only_keys:
            key_set = {str(item or "").strip() for item in only_keys if str(item or "").strip()}
            target_tables = [table for table in target_tables if table.key in key_set]
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
            if source_records is None:
                source_rows = self._read_source_rows(
                    client=_client_for(source_table),
                    source_table=source_table,
                    page_size=page_size,
                    emit_log=emit_log,
                )
            else:
                source_rows = [
                    row
                    for row in (
                        self._normalize_source_row(item)
                        for item in source_records
                        if isinstance(item, dict)
                    )
                    if row is not None
                ]
                self._emit(
                    emit_log,
                    f"[动环功率统计同步] 使用本地解析主表数据 date={report_date_slash}, "
                    f"records={len(source_records)}, valid_rows={len(source_rows)}",
                )
            if not source_rows:
                raise RuntimeError("动环功率主表没有可用数据，拒绝覆盖目标统计表")
            generated = self._generate_all_targets(
                source_rows,
                target_tables=target_tables,
                report_date=report_date_slash,
                data_center_name=data_center_name,
                emit_log=emit_log,
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
            summary_parts = []
            for table in target_tables:
                item = results.get(table.key, {}) if isinstance(results.get(table.key, {}), dict) else {}
                summary_parts.append(
                    f"{table.name}: generated={int(item.get('generated', 0) or 0)}, "
                    f"deleted={int(item.get('deleted', 0) or 0)}, created={int(item.get('created', 0) or 0)}"
                )
            summary_text = "; ".join(summary_parts)
            self._emit(
                emit_log,
                f"[动环功率统计同步] 完成 date={report_date_slash}, source_rows={len(source_rows)}, "
                f"targets={{{summary_text}}}, elapsed_ms={elapsed_ms}",
            )
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
