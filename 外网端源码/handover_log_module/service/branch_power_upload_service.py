from __future__ import annotations

import re
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

import openpyxl

from app.config.config_adapter import normalize_role_mode
from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms
from pipeline_utils import get_app_dir


@dataclass(frozen=True)
class _MetricRow:
    source_row: int
    room: str
    machine_row: str
    point: str
    value: Any


@dataclass(frozen=True)
class _SourceBundle:
    building: str
    power_file: Path
    current_file: Path
    switch_file: Path
    metadata: Dict[str, Any]


class BranchPowerUploadService:
    """Collect branch power/current/switch values locally and upload once daily."""

    APP_TOKEN = "ASLxbfESPahdTKs0A9NccgbrnXc"
    TABLE_ID = "tblT5KbsxGCK1SwA"

    FIELD_BUILDING = "机楼"
    FIELD_ROOM = "包间"
    FIELD_ROW = "机列"
    FIELD_BRANCH_ID = "支路编号"
    FIELD_PDU_ID = "PDU编号"

    FAMILY_POWER = "branch_power_family"
    FAMILY_CURRENT = "branch_current_family"
    FAMILY_SWITCH = "branch_switch_family"

    TABLE_VALUES = "branch_circuit_daily_values"
    TABLE_HOUR_STATUS = "branch_circuit_hour_status"
    TABLE_UPLOAD_HISTORY = "branch_circuit_upload_history"
    EMPTY_PREVIEW_ROWS = 10
    SWITCH_UNKNOWN_VALUE = "未知"
    SWITCH_ALLOWED_VALUES = {"分闸", "合闸", SWITCH_UNKNOWN_VALUE}

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config if isinstance(config, dict) else {}

    def _emit(self, emit_log: Callable[[str], None], text: str) -> None:
        try:
            emit_log(text)
        except Exception:  # noqa: BLE001
            pass

    def _is_external_role(self) -> bool:
        deployment = self.config.get("deployment", {}) if isinstance(self.config.get("deployment", {}), dict) else {}
        return normalize_role_mode(deployment.get("role_mode")) == "external"

    @staticmethod
    def _elapsed_ms(start: float) -> int:
        return int((time.perf_counter() - start) * 1000)

    def _progress_logger(
        self,
        emit_log: Callable[[str], None],
        *,
        label: str,
        total: int,
        step: int = 1000,
    ) -> Callable[[int, int], None]:
        last_logged = {"done": 0}

        def _log(done: int, total_count: int) -> None:
            total_value = int(total_count or total or 0)
            done_value = int(done or 0)
            if done_value >= total_value or done_value - last_logged["done"] >= step:
                last_logged["done"] = done_value
                self._emit(emit_log, f"[支路功率上传] {label}进度 {done_value}/{total_value}")

        return _log

    def _client(self, emit_log: Callable[[str], None]) -> FeishuBitableClient:
        auth = require_feishu_auth_settings(self.config)
        return FeishuBitableClient(
            app_id=str(auth.get("app_id", "") or "").strip(),
            app_secret=str(auth.get("app_secret", "") or "").strip(),
            app_token=self.APP_TOKEN,
            calc_table_id=self.TABLE_ID,
            attachment_table_id=self.TABLE_ID,
            timeout=int(auth.get("timeout", 30) or 30),
            request_retry_count=int(auth.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(auth.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=date_text_to_timestamp_ms,
            canonical_metric_name_fn=lambda value: str(value or "").strip(),
            dimension_mapping={},
            emit_log=emit_log,
        )

    def _runtime_state_root(self) -> Path:
        common = self.config.get("common", {}) if isinstance(self.config.get("common", {}), dict) else {}
        paths = common.get("paths", {}) if isinstance(common.get("paths", {}), dict) else {}
        root_text = str(paths.get("runtime_state_root", "") or "").strip()
        root = Path(root_text) if root_text else Path(".runtime")
        if not root.is_absolute():
            root = get_app_dir() / root
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _db_path(self) -> Path:
        return self._runtime_state_root() / "branch_circuit_daily.db"

    @staticmethod
    def _q(name: str) -> str:
        return '"' + str(name or "").replace('"', '""') + '"'

    @staticmethod
    def _norm_text(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @classmethod
    def _hour_fields(cls, hour: int) -> tuple[str, str, str]:
        return (f"功率-{hour}:00", f"电流-{hour}:00", f"开关状态-{hour}:00")

    @classmethod
    def _upload_fields(cls) -> List[str]:
        output = [
            cls.FIELD_BUILDING,
            cls.FIELD_ROOM,
            cls.FIELD_ROW,
            cls.FIELD_BRANCH_ID,
            cls.FIELD_PDU_ID,
        ]
        for hour in range(24):
            output.extend(cls._hour_fields(hour))
        return output

    @staticmethod
    def _field_type(field_name: str) -> str:
        if field_name.startswith("功率-") or field_name.startswith("电流-"):
            return "REAL"
        return "TEXT"

    def _connect(self) -> sqlite3.Connection:
        path = self._db_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _ensure_schema(self, conn: sqlite3.Connection, upload_fields: List[str] | None = None) -> None:
        fields = upload_fields or self._upload_fields()
        columns = [
            f"{self._q('business_date')} TEXT NOT NULL",
            f"{self._q('created_at')} TEXT NOT NULL",
            f"{self._q('updated_at')} TEXT NOT NULL",
        ]
        for field in fields:
            columns.append(f"{self._q(field)} {self._field_type(field)}")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {self._q(self.TABLE_VALUES)} ({', '.join(columns)})")
        existing_columns = {
            str(row["name"])
            for row in conn.execute(f"PRAGMA table_info({self._q(self.TABLE_VALUES)})").fetchall()
        }
        for field in ["business_date", "created_at", "updated_at", *fields]:
            if field in existing_columns:
                continue
            field_type = "TEXT" if field in {"business_date", "created_at", "updated_at"} else self._field_type(field)
            conn.execute(f"ALTER TABLE {self._q(self.TABLE_VALUES)} ADD COLUMN {self._q(field)} {field_type}")
        key_cols = [
            "business_date",
            self.FIELD_BUILDING,
            self.FIELD_ROOM,
            self.FIELD_ROW,
            self.FIELD_BRANCH_ID,
            self.FIELD_PDU_ID,
        ]
        conn.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {self._q('idx_branch_circuit_daily_key')} "
            f"ON {self._q(self.TABLE_VALUES)} ({', '.join(self._q(col) for col in key_cols)})"
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._q(self.TABLE_HOUR_STATUS)} (
                {self._q('business_date')} TEXT NOT NULL,
                {self._q('hour')} INTEGER NOT NULL,
                {self._q('building')} TEXT NOT NULL,
                {self._q('storage_bucket_key')} TEXT NOT NULL,
                {self._q('data_bucket_key')} TEXT NOT NULL,
                {self._q('status')} TEXT NOT NULL,
                {self._q('power_file_path')} TEXT NOT NULL DEFAULT '',
                {self._q('current_file_path')} TEXT NOT NULL DEFAULT '',
                {self._q('switch_file_path')} TEXT NOT NULL DEFAULT '',
                {self._q('power_file_sig')} TEXT NOT NULL DEFAULT '',
                {self._q('current_file_sig')} TEXT NOT NULL DEFAULT '',
                {self._q('switch_file_sig')} TEXT NOT NULL DEFAULT '',
                {self._q('row_count')} INTEGER NOT NULL DEFAULT 0,
                {self._q('created_count')} INTEGER NOT NULL DEFAULT 0,
                {self._q('updated_count')} INTEGER NOT NULL DEFAULT 0,
                {self._q('error')} TEXT NOT NULL DEFAULT '',
                {self._q('updated_at')} TEXT NOT NULL,
                PRIMARY KEY ({self._q('business_date')}, {self._q('hour')}, {self._q('building')})
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._q(self.TABLE_UPLOAD_HISTORY)} (
                {self._q('business_date')} TEXT PRIMARY KEY,
                {self._q('status')} TEXT NOT NULL,
                {self._q('uploaded_at')} TEXT NOT NULL DEFAULT '',
                {self._q('record_count')} INTEGER NOT NULL DEFAULT 0,
                {self._q('last_error')} TEXT NOT NULL DEFAULT '',
                {self._q('updated_at')} TEXT NOT NULL
            )
            """
        )
        conn.commit()

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _parse_bucket_datetime(bucket_key: str) -> datetime:
        text = str(bucket_key or "").strip()
        for fmt in ("%Y-%m-%d %H", "%Y%m%d%H"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) >= 10:
            return datetime.strptime(digits[:10], "%Y%m%d%H")
        raise ValueError(f"支路功率小时桶无效: {bucket_key}")

    @classmethod
    def _resolve_data_bucket_datetime(cls, *, bucket_key: str, source_units: List[Dict[str, Any]]) -> datetime:
        data_bucket_keys: List[str] = []
        for unit in source_units:
            if not isinstance(unit, dict):
                continue
            metadata = unit.get("metadata", {}) if isinstance(unit.get("metadata", {}), dict) else {}
            for raw_value in (
                unit.get("data_hour_bucket", ""),
                metadata.get("data_hour_bucket", ""),
                metadata.get("data_bucket_key", ""),
            ):
                raw = str(raw_value or "").strip()
                if not raw:
                    continue
                parsed = cls._parse_bucket_datetime(raw)
                normalized = parsed.strftime("%Y-%m-%d %H")
                if normalized not in data_bucket_keys:
                    data_bucket_keys.append(normalized)
        if not data_bucket_keys:
            for unit in source_units:
                if not isinstance(unit, dict):
                    continue
                source_files = unit.get("source_files", {}) if isinstance(unit.get("source_files", {}), dict) else {}
                raw_path = (
                    unit.get("power_file")
                    or source_files.get("power_file")
                    or unit.get("source_file")
                    or unit.get("file_path")
                    or ""
                )
                detected = (
                    cls._detect_single_header_datetime(Path(str(raw_path).strip()))
                    if str(raw_path or "").strip()
                    else None
                )
                if detected is None:
                    continue
                normalized = detected.strftime("%Y-%m-%d %H")
                if normalized not in data_bucket_keys:
                    data_bucket_keys.append(normalized)
        if len(data_bucket_keys) > 1:
            raise RuntimeError(f"支路源文件数据小时不一致: {', '.join(data_bucket_keys)}")
        if data_bucket_keys:
            return cls._parse_bucket_datetime(data_bucket_keys[0])
        return cls._parse_bucket_datetime(bucket_key)

    @classmethod
    def _resolve_range_data_bucket_datetimes(
        cls,
        *,
        bucket_keys: List[str],
        source_units: List[Dict[str, Any]],
    ) -> List[datetime]:
        normalized_keys: List[str] = []

        def add_key(raw_value: Any) -> None:
            raw = str(raw_value or "").strip()
            if not raw:
                return
            parsed = cls._parse_bucket_datetime(raw)
            normalized = parsed.strftime("%Y-%m-%d %H")
            if normalized not in normalized_keys:
                normalized_keys.append(normalized)

        for item in bucket_keys or []:
            add_key(item)
        if not normalized_keys:
            for unit in source_units:
                if not isinstance(unit, dict):
                    continue
                metadata = unit.get("metadata", {}) if isinstance(unit.get("metadata", {}), dict) else {}
                covered = metadata.get("covered_data_hour_buckets")
                if isinstance(covered, list):
                    for item in covered:
                        add_key(item)
                for raw_value in (
                    unit.get("data_hour_bucket", ""),
                    metadata.get("data_hour_bucket", ""),
                    metadata.get("data_bucket_key", ""),
                ):
                    add_key(raw_value)
        if not normalized_keys:
            raise RuntimeError("支路范围入库缺少目标数据小时")
        normalized_keys.sort(key=lambda item: cls._parse_bucket_datetime(item))
        return [cls._parse_bucket_datetime(item) for item in normalized_keys]

    @staticmethod
    def _parse_header_datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value.replace(minute=0, second=0, microsecond=0)
        text = str(value or "").strip().replace("/", "-")
        if not text:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H"):
            try:
                return datetime.strptime(text, fmt).replace(minute=0, second=0, microsecond=0)
            except ValueError:
                continue
        return None

    @classmethod
    def _detect_single_header_datetime(cls, file_path: Path) -> datetime | None:
        try:
            workbook = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        except OSError:
            return None
        try:
            sheet = workbook.active
            if hasattr(sheet, "reset_dimensions"):
                sheet.reset_dimensions()
            header = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True), ())
            detected: List[datetime] = []
            for value in header:
                parsed = cls._parse_header_datetime(value)
                if parsed is not None and parsed not in detected:
                    detected.append(parsed)
            return detected[0] if len(detected) == 1 else None
        finally:
            workbook.close()

    @classmethod
    def _detect_header_bucket_keys(cls, file_path: Path) -> List[str]:
        try:
            workbook = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        except OSError:
            return []
        try:
            sheet = workbook.active
            if hasattr(sheet, "reset_dimensions"):
                sheet.reset_dimensions()
            header = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True), ())
            detected: List[str] = []
            for value in header:
                parsed = cls._parse_header_datetime(value)
                if parsed is None:
                    continue
                normalized = parsed.strftime("%Y-%m-%d %H")
                if normalized not in detected:
                    detected.append(normalized)
            return detected
        finally:
            workbook.close()

    def _resolve_hour_column(self, sheet: Any, *, data_bucket_dt: datetime, file_path: Path) -> int:
        header = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True), ())
        target = data_bucket_dt.replace(minute=0, second=0, microsecond=0)
        for index, value in enumerate(header, start=1):
            parsed = self._parse_header_datetime(value)
            if parsed == target:
                return index
        available = [
            str(value).strip()
            for value in header
            if str(value or "").strip()
        ][:12]
        raise RuntimeError(
            f"{file_path.name} 未找到数据小时列 {target.strftime('%Y-%m-%d %H:%M:%S')}，"
            f"表头可见值={available}"
        )

    def _resolve_hour_columns(
        self,
        sheet: Any,
        *,
        data_bucket_dts: List[datetime],
        file_path: Path,
    ) -> Dict[str, int]:
        header = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True), ())
        target_by_key = {
            item.strftime("%Y-%m-%d %H"): item.replace(minute=0, second=0, microsecond=0)
            for item in data_bucket_dts
        }
        parsed_header: Dict[datetime, int] = {}
        for index, value in enumerate(header, start=1):
            parsed = self._parse_header_datetime(value)
            if parsed is not None and parsed not in parsed_header:
                parsed_header[parsed] = index
        columns: Dict[str, int] = {}
        for key, target in target_by_key.items():
            column = parsed_header.get(target)
            if column:
                columns[key] = column
        return columns

    def _row_has_data(self, row: tuple[Any, ...], value_index: int) -> bool:
        room = self._norm_text(row[0] if len(row) > 0 else "")
        machine_row = self._norm_text(row[1] if len(row) > 1 else "")
        point = self._norm_text(row[2] if len(row) > 2 else "")
        raw_value = row[value_index] if len(row) > value_index else None
        return any([room, machine_row, point, self._norm_text(raw_value)])

    def _load_metric_rows(
        self,
        *,
        file_path: Path,
        data_bucket_dt: datetime,
        metric_label: str,
    ) -> List[_MetricRow]:
        rows: List[_MetricRow] = []
        workbook = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        try:
            sheet = workbook.active
            if hasattr(sheet, "reset_dimensions"):
                sheet.reset_dimensions()
            hour_col = self._resolve_hour_column(sheet, data_bucket_dt=data_bucket_dt, file_path=file_path)
            value_index = hour_col - 1
            data_rows = sheet.iter_rows(min_row=4, max_col=hour_col, values_only=True)
            preview_rows: List[tuple[Any, ...]] = []
            preview_has_data = False
            for _ in range(self.EMPTY_PREVIEW_ROWS):
                try:
                    row = tuple(next(data_rows) or ())
                except StopIteration:
                    break
                preview_rows.append(row)
                if self._row_has_data(row, value_index):
                    preview_has_data = True
            if not preview_has_data:
                return []

            def append_row(index: int, row: tuple[Any, ...]) -> None:
                if not self._row_has_data(row, value_index):
                    return
                room = self._norm_text(row[0] if len(row) > 0 else "")
                machine_row = self._norm_text(row[1] if len(row) > 1 else "")
                point = self._norm_text(row[2] if len(row) > 2 else "")
                value = row[value_index] if len(row) > value_index else None
                if not room or not machine_row or not point:
                    raise RuntimeError(f"{file_path.name} 第{index}行缺少包间/机列/测点")
                if value is None or self._norm_text(value) == "":
                    return
                rows.append(
                    _MetricRow(
                        source_row=index,
                        room=room,
                        machine_row=machine_row,
                        point=point,
                        value=value,
                    )
                )

            for index, row in enumerate(preview_rows, start=4):
                append_row(index, row)
            for index, row in enumerate(data_rows, start=4 + len(preview_rows)):
                append_row(index, tuple(row or ()))
        finally:
            workbook.close()
        return rows

    def _load_metric_rows_by_hour(
        self,
        *,
        file_path: Path,
        data_bucket_dts: List[datetime],
        metric_label: str,
    ) -> Dict[str, List[_MetricRow]]:
        rows_by_bucket = {item.strftime("%Y-%m-%d %H"): [] for item in data_bucket_dts}
        workbook = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        try:
            sheet = workbook.active
            if hasattr(sheet, "reset_dimensions"):
                sheet.reset_dimensions()
            hour_columns = self._resolve_hour_columns(
                sheet,
                data_bucket_dts=data_bucket_dts,
                file_path=file_path,
            )
            max_col = max(hour_columns.values()) if hour_columns else 0
            data_rows = sheet.iter_rows(min_row=4, max_col=max_col, values_only=True)
            for index, raw_row in enumerate(data_rows, start=4):
                row = tuple(raw_row or ())
                room = self._norm_text(row[0] if len(row) > 0 else "")
                machine_row = self._norm_text(row[1] if len(row) > 1 else "")
                point = self._norm_text(row[2] if len(row) > 2 else "")
                for bucket_key, hour_col in hour_columns.items():
                    value_index = hour_col - 1
                    if not self._row_has_data(row, value_index):
                        continue
                    value = row[value_index] if len(row) > value_index else None
                    if not room or not machine_row or not point:
                        raise RuntimeError(f"{file_path.name} 第{index}行缺少包间/机列/测点")
                    if value is None or self._norm_text(value) == "":
                        continue
                    rows_by_bucket[bucket_key].append(
                        _MetricRow(
                            source_row=index,
                            room=room,
                            machine_row=machine_row,
                            point=point,
                            value=value,
                        )
                    )
        finally:
            workbook.close()
        return rows_by_bucket

    @staticmethod
    def _number_value(value: Any, *, label: str, source_row: int) -> float:
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value or "").strip().replace(",", "")
        if not text:
            raise RuntimeError(f"第{source_row}行{label}为空")
        try:
            return float(text)
        except ValueError as exc:
            raise RuntimeError(f"第{source_row}行{label}不是数字: {value}") from exc

    @staticmethod
    def _extract_pdu_id(raw_point: str, *, source_row: int) -> str:
        def normalize_number(text: str) -> str:
            normalized = str(text or "").lstrip("0")
            return normalized or "0"

        text = str(raw_point or "")
        match = re.match(r"\s*(\d+)", text)
        if match:
            return normalize_number(match.group(1))
        match = re.search(r"支路\s*(\d+)\s*路", text)
        if match:
            return normalize_number(match.group(1))
        match = re.search(r"支路\s*(\d+)", text)
        if not match:
            raise RuntimeError(f"第{source_row}行无法从功率测点提取PDU编号: {raw_point}")
        return normalize_number(match.group(1))

    @staticmethod
    def _extract_branch_id(raw_point: str, *, source_row: int) -> str:
        text = str(raw_point or "").strip()
        if not text:
            raise RuntimeError(f"第{source_row}行支路开关测点为空")
        return text.split("_", 1)[0].strip()

    @classmethod
    def _normalize_switch_value(cls, value: Any) -> str:
        if isinstance(value, (int, float)) and float(value) == 0:
            return cls.SWITCH_UNKNOWN_VALUE
        text = cls._norm_text(value)
        if text:
            try:
                if float(text.replace(",", "")) == 0:
                    return cls.SWITCH_UNKNOWN_VALUE
            except ValueError:
                pass
        return text

    def _combine_rows(
        self,
        *,
        building: str,
        power_rows: List[_MetricRow],
        current_rows: List[_MetricRow],
        switch_rows: List[_MetricRow],
        hour: int,
    ) -> List[Dict[str, Any]]:
        if not power_rows or not current_rows or not switch_rows:
            raise RuntimeError(
                f"{building} 三类支路源文件数据为空: 功率={len(power_rows)}, 电流={len(current_rows)}, 开关={len(switch_rows)}"
            )
        if len(power_rows) != len(current_rows) or len(power_rows) != len(switch_rows):
            raise RuntimeError(
                f"{building} 三类支路源文件行数不一致: 功率={len(power_rows)}, 电流={len(current_rows)}, 开关={len(switch_rows)}"
            )
        power_field, current_field, switch_field = self._hour_fields(hour)
        output: List[Dict[str, Any]] = []
        seen_keys: set[tuple[str, str, str, str, str]] = set()
        for index, (power, current, switch) in enumerate(zip(power_rows, current_rows, switch_rows), start=1):
            if power.room != current.room or power.room != switch.room:
                raise RuntimeError(
                    f"{building} 第{index}条包间不一致: 功率={power.room}, 电流={current.room}, 开关={switch.room}"
                )
            if power.machine_row != current.machine_row or power.machine_row != switch.machine_row:
                raise RuntimeError(
                    f"{building} 第{index}条机列不一致: 功率={power.machine_row}, 电流={current.machine_row}, 开关={switch.machine_row}"
                )
            switch_value = self._normalize_switch_value(switch.value)
            if switch_value not in self.SWITCH_ALLOWED_VALUES:
                raise RuntimeError(f"{building} 第{switch.source_row}行开关状态无效: {switch.value}")
            power_pdu_id = self._extract_pdu_id(power.point, source_row=power.source_row)
            current_pdu_id = self._extract_pdu_id(current.point, source_row=current.source_row)
            if power_pdu_id != current_pdu_id:
                raise RuntimeError(
                    f"{building} 第{index}条功率/电流PDU编号不一致: "
                    f"功率第{power.source_row}行={power_pdu_id}, 电流第{current.source_row}行={current_pdu_id}"
                )
            branch_id = self._extract_branch_id(switch.point, source_row=switch.source_row)
            row_key = (building, power.room, power.machine_row, branch_id, power_pdu_id)
            if row_key in seen_keys:
                raise RuntimeError(f"{building} 支路源文件存在重复支路记录: {'/'.join(row_key)}")
            seen_keys.add(row_key)
            output.append(
                {
                    self.FIELD_BUILDING: building,
                    self.FIELD_ROOM: power.room,
                    self.FIELD_ROW: power.machine_row,
                    self.FIELD_BRANCH_ID: branch_id,
                    self.FIELD_PDU_ID: power_pdu_id,
                    power_field: self._number_value(power.value, label="功率", source_row=power.source_row),
                    current_field: self._number_value(current.value, label="电流", source_row=current.source_row),
                    switch_field: switch_value,
                }
            )
        return output

    @staticmethod
    def _is_no_data_error(error_text: Any) -> bool:
        text = str(error_text or "").strip()
        return "三类支路源文件数据为空" in text

    @staticmethod
    def _file_signature(path: Path) -> str:
        try:
            stat = path.stat()
            return f"{path.resolve()}|{stat.st_size}|{stat.st_mtime_ns}"
        except OSError:
            return str(path)

    @staticmethod
    def _replace_label_path(path: Path, target_label: str) -> Path:
        text = str(path)
        if "支路功率源文件" in text:
            text = text.replace("支路功率源文件", target_label)
        if "支路功率" in text:
            text = text.replace("支路功率", "支路电流" if target_label == "支路电流源文件" else "支路开关")
        return Path(text)

    def _derive_sibling_file(self, power_file: Path, *, target_label: str) -> Path:
        candidate = self._replace_label_path(power_file, target_label)
        if self._is_external_role():
            return candidate
        if candidate.exists():
            return candidate
        short_name = "支路电流" if target_label == "支路电流源文件" else "支路开关"
        for item in sorted(power_file.parent.glob(f"*{short_name}*.xlsx")):
            if item.is_file():
                return item
        return candidate

    def _normalize_source_bundles(self, source_units: List[Dict[str, Any]]) -> List[_SourceBundle]:
        grouped: Dict[str, Dict[str, Any]] = {}
        for unit in source_units:
            if not isinstance(unit, dict):
                continue
            building = self._norm_text(unit.get("building"))
            if not building:
                continue
            bucket = grouped.setdefault(building, {"building": building, "metadata": {}})
            metadata = unit.get("metadata", {}) if isinstance(unit.get("metadata", {}), dict) else {}
            if metadata and not bucket.get("metadata"):
                bucket["metadata"] = metadata
            source_files = unit.get("source_files", {}) if isinstance(unit.get("source_files", {}), dict) else {}
            for key in ("power_file", "current_file", "switch_file"):
                raw = unit.get(key) or source_files.get(key)
                if raw:
                    bucket[key] = Path(str(raw).strip())
            raw_source = unit.get("source_file") or unit.get("file_path")
            if raw_source:
                source_family = self._norm_text(unit.get("source_family") or metadata.get("family"))
                target_key = "power_file"
                if source_family == self.FAMILY_CURRENT:
                    target_key = "current_file"
                elif source_family == self.FAMILY_SWITCH:
                    target_key = "switch_file"
                bucket[target_key] = Path(str(raw_source).strip())
        bundles: List[_SourceBundle] = []
        for payload in grouped.values():
            building = self._norm_text(payload.get("building"))
            power_file = payload.get("power_file")
            if not isinstance(power_file, Path):
                raise RuntimeError(f"{building} 缺少支路功率源文件")
            current_file = payload.get("current_file")
            switch_file = payload.get("switch_file")
            if not isinstance(current_file, Path):
                current_file = self._derive_sibling_file(power_file, target_label="支路电流源文件")
            if not isinstance(switch_file, Path):
                switch_file = self._derive_sibling_file(power_file, target_label="支路开关源文件")
            bundles.append(
                _SourceBundle(
                    building=building,
                    power_file=power_file,
                    current_file=current_file,
                    switch_file=switch_file,
                    metadata=payload.get("metadata", {}) if isinstance(payload.get("metadata", {}), dict) else {},
                )
            )
        return bundles

    def _choose_range_source_file(
        self,
        candidates: List[Path],
        *,
        target_bucket_keys: List[str],
        building: str,
        label: str,
    ) -> Path:
        unique: List[Path] = []
        seen: set[str] = set()
        for path in candidates:
            if not isinstance(path, Path):
                continue
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            unique.append(path)
        if not unique:
            raise RuntimeError(f"{building} 缺少{label}")

        target_set = set(target_bucket_keys)
        scored: List[tuple[int, int, float, Path, List[str]]] = []
        for path in unique:
            detected = self._detect_header_bucket_keys(path)
            coverage = len(target_set.intersection(detected))
            covers_all = 1 if target_set and target_set.issubset(set(detected)) else 0
            try:
                mtime = path.stat().st_mtime
            except OSError:
                mtime = 0.0
            scored.append((covers_all, coverage, mtime, path, detected))
        scored.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
        best = scored[0]
        if best[1] <= 0:
            raise RuntimeError(f"{building} {label}未覆盖目标小时: {','.join(target_bucket_keys)}")
        return best[3]

    def _normalize_range_source_bundles(
        self,
        source_units: List[Dict[str, Any]],
        *,
        data_bucket_dts: List[datetime],
    ) -> List[_SourceBundle]:
        target_bucket_keys = [item.strftime("%Y-%m-%d %H") for item in data_bucket_dts]
        grouped: Dict[str, Dict[str, Any]] = {}
        for unit in source_units:
            if not isinstance(unit, dict):
                continue
            building = self._norm_text(unit.get("building"))
            if not building:
                continue
            bucket = grouped.setdefault(
                building,
                {"building": building, "metadata": {}, "power_files": [], "current_files": [], "switch_files": []},
            )
            metadata = unit.get("metadata", {}) if isinstance(unit.get("metadata", {}), dict) else {}
            if metadata and not bucket.get("metadata"):
                bucket["metadata"] = metadata
            source_files = unit.get("source_files", {}) if isinstance(unit.get("source_files", {}), dict) else {}
            for key, list_key in (
                ("power_file", "power_files"),
                ("current_file", "current_files"),
                ("switch_file", "switch_files"),
            ):
                raw = unit.get(key) or source_files.get(key)
                if raw:
                    bucket[list_key].append(Path(str(raw).strip()))
            raw_source = unit.get("source_file") or unit.get("file_path")
            if raw_source:
                source_family = self._norm_text(unit.get("source_family") or metadata.get("family"))
                list_key = "power_files"
                if source_family == self.FAMILY_CURRENT:
                    list_key = "current_files"
                elif source_family == self.FAMILY_SWITCH:
                    list_key = "switch_files"
                bucket[list_key].append(Path(str(raw_source).strip()))

        bundles: List[_SourceBundle] = []
        for payload in grouped.values():
            building = self._norm_text(payload.get("building"))
            power_file = self._choose_range_source_file(
                payload.get("power_files", []) if isinstance(payload.get("power_files", []), list) else [],
                target_bucket_keys=target_bucket_keys,
                building=building,
                label="支路功率源文件",
            )
            current_candidates = payload.get("current_files", []) if isinstance(payload.get("current_files", []), list) else []
            switch_candidates = payload.get("switch_files", []) if isinstance(payload.get("switch_files", []), list) else []
            current_candidates.append(self._derive_sibling_file(power_file, target_label="支路电流源文件"))
            switch_candidates.append(self._derive_sibling_file(power_file, target_label="支路开关源文件"))
            current_file = self._choose_range_source_file(
                current_candidates,
                target_bucket_keys=target_bucket_keys,
                building=building,
                label="支路电流源文件",
            )
            switch_file = self._choose_range_source_file(
                switch_candidates,
                target_bucket_keys=target_bucket_keys,
                building=building,
                label="支路开关源文件",
            )
            bundles.append(
                _SourceBundle(
                    building=building,
                    power_file=power_file,
                    current_file=current_file,
                    switch_file=switch_file,
                    metadata=payload.get("metadata", {}) if isinstance(payload.get("metadata", {}), dict) else {},
                )
            )
        return bundles

    def _existing_keys_for_building(
        self,
        conn: sqlite3.Connection,
        *,
        business_date: str,
        building: str,
    ) -> set[tuple[str, str, str, str, str]]:
        rows = conn.execute(
            f"""
            SELECT {self._q(self.FIELD_ROOM)}, {self._q(self.FIELD_ROW)}, {self._q(self.FIELD_BRANCH_ID)}, {self._q(self.FIELD_PDU_ID)}
            FROM {self._q(self.TABLE_VALUES)}
            WHERE {self._q('business_date')}=? AND {self._q(self.FIELD_BUILDING)}=?
            """,
            (business_date, building),
        ).fetchall()
        return {
            (
                building,
                self._norm_text(row[self.FIELD_ROOM]),
                self._norm_text(row[self.FIELD_ROW]),
                self._norm_text(row[self.FIELD_BRANCH_ID]),
                self._norm_text(row[self.FIELD_PDU_ID]),
            )
            for row in rows
        }

    def _upsert_hour_rows(
        self,
        conn: sqlite3.Connection,
        *,
        business_date: str,
        hour: int,
        rows: List[Dict[str, Any]],
    ) -> tuple[int, int]:
        if not rows:
            return 0, 0
        existing_keys = self._existing_keys_for_building(
            conn,
            business_date=business_date,
            building=self._norm_text(rows[0].get(self.FIELD_BUILDING)),
        )
        created = 0
        updated = 0
        now_text = self._now_text()
        key_fields = [
            self.FIELD_BUILDING,
            self.FIELD_ROOM,
            self.FIELD_ROW,
            self.FIELD_BRANCH_ID,
            self.FIELD_PDU_ID,
        ]
        hour_fields = list(self._hour_fields(hour))
        insert_fields = ["business_date", "created_at", "updated_at", *key_fields, *hour_fields]
        placeholders = ", ".join("?" for _ in insert_fields)
        update_clause = ", ".join(
            f"{self._q(field)}=excluded.{self._q(field)}"
            for field in ["updated_at", *hour_fields]
        )
        conflict_fields = ["business_date", *key_fields]
        sql = (
            f"INSERT INTO {self._q(self.TABLE_VALUES)} ({', '.join(self._q(field) for field in insert_fields)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT({', '.join(self._q(field) for field in conflict_fields)}) DO UPDATE SET {update_clause}"
        )
        for row in rows:
            key = tuple(self._norm_text(row.get(field)) for field in key_fields)
            if key in existing_keys:
                updated += 1
            else:
                created += 1
                existing_keys.add(key)  # duplicate source rows count as updates after the first one.
            values = [business_date, now_text, now_text]
            values.extend(self._norm_text(row.get(field)) for field in key_fields)
            values.extend(row.get(field) for field in hour_fields)
            conn.execute(sql, values)
        return created, updated

    def _mark_hour_status(
        self,
        conn: sqlite3.Connection,
        *,
        business_date: str,
        hour: int,
        building: str,
        storage_bucket_key: str,
        data_bucket_key: str,
        status: str,
        bundle: _SourceBundle | None,
        row_count: int = 0,
        created_count: int = 0,
        updated_count: int = 0,
        error: str = "",
    ) -> None:
        now_text = self._now_text()
        power_file = bundle.power_file if bundle else Path("")
        current_file = bundle.current_file if bundle else Path("")
        switch_file = bundle.switch_file if bundle else Path("")
        conn.execute(
            f"""
            INSERT INTO {self._q(self.TABLE_HOUR_STATUS)} (
                {self._q('business_date')}, {self._q('hour')}, {self._q('building')},
                {self._q('storage_bucket_key')}, {self._q('data_bucket_key')}, {self._q('status')},
                {self._q('power_file_path')}, {self._q('current_file_path')}, {self._q('switch_file_path')},
                {self._q('power_file_sig')}, {self._q('current_file_sig')}, {self._q('switch_file_sig')},
                {self._q('row_count')}, {self._q('created_count')}, {self._q('updated_count')},
                {self._q('error')}, {self._q('updated_at')}
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT({self._q('business_date')}, {self._q('hour')}, {self._q('building')}) DO UPDATE SET
                {self._q('storage_bucket_key')}=excluded.{self._q('storage_bucket_key')},
                {self._q('data_bucket_key')}=excluded.{self._q('data_bucket_key')},
                {self._q('status')}=excluded.{self._q('status')},
                {self._q('power_file_path')}=excluded.{self._q('power_file_path')},
                {self._q('current_file_path')}=excluded.{self._q('current_file_path')},
                {self._q('switch_file_path')}=excluded.{self._q('switch_file_path')},
                {self._q('power_file_sig')}=excluded.{self._q('power_file_sig')},
                {self._q('current_file_sig')}=excluded.{self._q('current_file_sig')},
                {self._q('switch_file_sig')}=excluded.{self._q('switch_file_sig')},
                {self._q('row_count')}=excluded.{self._q('row_count')},
                {self._q('created_count')}=excluded.{self._q('created_count')},
                {self._q('updated_count')}=excluded.{self._q('updated_count')},
                {self._q('error')}=excluded.{self._q('error')},
                {self._q('updated_at')}=excluded.{self._q('updated_at')}
            """,
            (
                business_date,
                int(hour),
                building,
                storage_bucket_key,
                data_bucket_key,
                status,
                str(power_file),
                str(current_file),
                str(switch_file),
                self._file_signature(power_file) if power_file else "",
                self._file_signature(current_file) if current_file else "",
                self._file_signature(switch_file) if switch_file else "",
                int(row_count),
                int(created_count),
                int(updated_count),
                str(error or "").strip(),
                now_text,
            ),
        )

    def _delete_hour_status(
        self,
        conn: sqlite3.Connection,
        *,
        business_date: str,
        hour: int,
        building: str,
    ) -> None:
        conn.execute(
            f"""
            DELETE FROM {self._q(self.TABLE_HOUR_STATUS)}
            WHERE {self._q('business_date')}=?
              AND {self._q('hour')}=?
              AND {self._q('building')}=?
            """,
            (business_date, int(hour), building),
        )

    def _clear_hour_values(
        self,
        conn: sqlite3.Connection,
        *,
        business_date: str,
        hour: int,
        building: str,
    ) -> None:
        hour_fields = list(self._hour_fields(int(hour)))
        set_parts = [f"{self._q(field)}=NULL" for field in hour_fields]
        set_parts.append(f"{self._q('updated_at')}=?")
        set_clause = ", ".join(set_parts)
        conn.execute(
            f"""
            UPDATE {self._q(self.TABLE_VALUES)}
            SET {set_clause}
            WHERE {self._q('business_date')}=?
              AND {self._q(self.FIELD_BUILDING)}=?
            """,
            (self._now_text(), business_date, building),
        )

    def _configured_buildings(self) -> List[str]:
        common_buildings = ["A楼", "B楼", "C楼", "D楼", "E楼"]
        sites = self.config.get("common", {}).get("internal_source_sites", [])
        if not isinstance(sites, list):
            sites = self.config.get("internal_source_sites", [])
        output: List[str] = []
        if isinstance(sites, list):
            for site in sites:
                if not isinstance(site, dict) or not bool(site.get("enabled", True)):
                    continue
                building = self._norm_text(site.get("building"))
                if building and building not in output:
                    output.append(building)
        return output or common_buildings

    def _missing_day_hours(
        self,
        conn: sqlite3.Connection,
        *,
        business_date: str,
        expected_buildings: List[str],
        max_hour: int = 23,
    ) -> List[str]:
        return [
            f"{building}/{hour}:00"
            for hour, building in self._missing_day_hour_units(
                conn,
                business_date=business_date,
                expected_buildings=expected_buildings,
                max_hour=max_hour,
            )
        ]

    def _missing_day_hour_units(
        self,
        conn: sqlite3.Connection,
        *,
        business_date: str,
        expected_buildings: List[str],
        max_hour: int = 23,
    ) -> List[tuple[int, str]]:
        max_hour = max(0, min(23, int(max_hour)))
        buildings = [self._norm_text(item) for item in expected_buildings if self._norm_text(item)]
        value_columns = {
            str(row["name"])
            for row in conn.execute(f"PRAGMA table_info({self._q(self.TABLE_VALUES)})").fetchall()
        }
        row_counts = {
            self._norm_text(row["building"]): int(row["row_count"] or 0)
            for row in conn.execute(
                f"""
                SELECT {self._q(self.FIELD_BUILDING)} AS building, COUNT(*) AS row_count
                FROM {self._q(self.TABLE_VALUES)}
                WHERE {self._q('business_date')}=?
                GROUP BY {self._q(self.FIELD_BUILDING)}
                """,
                (business_date,),
            ).fetchall()
        }
        complete_cache: Dict[tuple[int, str], bool] = {}

        def _value_complete(hour: int, building: str) -> bool:
            cache_key = (hour, building)
            if cache_key in complete_cache:
                return complete_cache[cache_key]
            total_rows = int(row_counts.get(building, 0) or 0)
            if total_rows <= 0:
                complete_cache[cache_key] = False
                return False
            hour_fields = self._hour_fields(hour)
            if any(field not in value_columns for field in hour_fields):
                complete_cache[cache_key] = False
                return False
            non_empty = " AND ".join(
                f"{self._q(field)} IS NOT NULL AND TRIM(CAST({self._q(field)} AS TEXT)) != ''"
                for field in hour_fields
            )
            row = conn.execute(
                f"""
                SELECT COUNT(*) AS complete_rows
                FROM {self._q(self.TABLE_VALUES)}
                WHERE {self._q('business_date')}=?
                  AND {self._q(self.FIELD_BUILDING)}=?
                  AND {non_empty}
                """,
                (business_date, building),
            ).fetchone()
            complete_rows = int(row["complete_rows"] if row else 0)
            complete_cache[cache_key] = complete_rows == total_rows
            return complete_cache[cache_key]

        missing: List[tuple[int, str]] = []
        for hour in range(max_hour + 1):
            for building in buildings:
                if not _value_complete(hour, building):
                    missing.append((hour, building))
        return missing

    def get_missing_hour_numbers(
        self,
        *,
        business_date: str,
        expected_buildings: List[str],
        max_hour: int = 23,
    ) -> List[int]:
        with closing(self._connect()) as conn:
            self._ensure_schema(conn)
            units = self._missing_day_hour_units(
                conn,
                business_date=business_date,
                expected_buildings=expected_buildings,
                max_hour=max_hour,
            )
        return sorted({hour for hour, _building in units})

    def _target_fields_from_bitable(
        self,
        client: FeishuBitableClient,
        emit_log: Callable[[str], None],
    ) -> List[str]:
        fields = client.list_fields(self.TABLE_ID)
        names = [self._norm_text(field.get("field_name") or field.get("name")) for field in fields if isinstance(field, dict)]
        required = self._upload_fields()
        missing = [field for field in required if field not in names]
        if missing:
            raise RuntimeError(f"支路功率目标多维表缺少字段: {', '.join(missing[:20])}")
        self._emit(emit_log, f"[支路功率上传] 目标表字段校验完成 fields={len(names)}, upload_fields={len(required)}")
        return required

    def _daily_records_for_upload(
        self,
        conn: sqlite3.Connection,
        *,
        business_date: str,
        upload_fields: List[str],
    ) -> List[Dict[str, Any]]:
        rows = conn.execute(
            f"""
            SELECT {', '.join(self._q(field) for field in upload_fields)}
            FROM {self._q(self.TABLE_VALUES)}
            WHERE {self._q('business_date')}=?
            ORDER BY {self._q(self.FIELD_BUILDING)}, {self._q(self.FIELD_ROOM)}, {self._q(self.FIELD_ROW)},
                     {self._q(self.FIELD_BRANCH_ID)}, {self._q(self.FIELD_PDU_ID)}
            """,
            (business_date,),
        ).fetchall()
        output: List[Dict[str, Any]] = []
        for row in rows:
            fields: Dict[str, Any] = {}
            for field in upload_fields:
                value = row[field]
                if value is None:
                    continue
                if isinstance(value, str) and not value.strip():
                    continue
                fields[field] = value
            if all(self._norm_text(fields.get(field)) for field in [
                self.FIELD_BUILDING,
                self.FIELD_ROOM,
                self.FIELD_ROW,
                self.FIELD_BRANCH_ID,
                self.FIELD_PDU_ID,
            ]):
                output.append(fields)
        return output

    def _missing_day_row_fields(
        self,
        conn: sqlite3.Connection,
        *,
        business_date: str,
        upload_fields: List[str],
        expected_buildings: List[str],
        sample_limit: int = 100,
    ) -> Dict[str, Any]:
        key_fields = [
            self.FIELD_BUILDING,
            self.FIELD_ROOM,
            self.FIELD_ROW,
            self.FIELD_BRANCH_ID,
            self.FIELD_PDU_ID,
        ]
        hourly_fields = [field for hour in range(24) for field in self._hour_fields(hour) if field in upload_fields]
        selected_fields = [*key_fields, *hourly_fields]
        rows = conn.execute(
            f"""
            SELECT {', '.join(self._q(field) for field in selected_fields)}
            FROM {self._q(self.TABLE_VALUES)}
            WHERE {self._q('business_date')}=?
            ORDER BY {self._q(self.FIELD_BUILDING)}, {self._q(self.FIELD_ROOM)}, {self._q(self.FIELD_ROW)},
                     {self._q(self.FIELD_BRANCH_ID)}, {self._q(self.FIELD_PDU_ID)}
            """,
            (business_date,),
        ).fetchall()
        missing_count = 0
        sample: List[str] = []
        row_count_by_building: Dict[str, int] = {}

        def _add_missing(text: str) -> None:
            nonlocal missing_count
            missing_count += 1
            if len(sample) < max(1, int(sample_limit or 100)):
                sample.append(text)

        for row in rows:
            building = self._norm_text(row[self.FIELD_BUILDING])
            if building:
                row_count_by_building[building] = row_count_by_building.get(building, 0) + 1
            key_parts = [self._norm_text(row[field]) for field in key_fields]
            key_label = "/".join(part or "-" for part in key_parts)
            for field, value in zip(key_fields, key_parts):
                if not value:
                    _add_missing(f"{key_label}/{field}为空")
            for field in hourly_fields:
                value = row[field]
                if value is None or (isinstance(value, str) and not value.strip()):
                    _add_missing(f"{key_label}/{field}为空")

        for building in expected_buildings:
            if row_count_by_building.get(building, 0) <= 0:
                _add_missing(f"{building}/无暂存记录")
        return {
            "missing_count": missing_count,
            "sample": sample,
            "row_count": len(rows),
            "building_rows": row_count_by_building,
        }

    def _clear_local_day(self, conn: sqlite3.Connection, *, business_date: str) -> None:
        conn.execute(
            f"DELETE FROM {self._q(self.TABLE_VALUES)} WHERE {self._q('business_date')}=?",
            (business_date,),
        )
        conn.execute(
            f"DELETE FROM {self._q(self.TABLE_HOUR_STATUS)} WHERE {self._q('business_date')}=?",
            (business_date,),
        )
        conn.commit()

    def _is_business_date_uploaded(self, conn: sqlite3.Connection, *, business_date: str) -> bool:
        row = conn.execute(
            f"""
            SELECT {self._q('status')}
            FROM {self._q(self.TABLE_UPLOAD_HISTORY)}
            WHERE {self._q('business_date')}=?
            """,
            (business_date,),
        ).fetchone()
        return row is not None and self._norm_text(row["status"]).lower() == "success"

    def get_hour_status_summary(
        self,
        *,
        business_date: str,
        expected_buildings: List[str] | None = None,
        max_hour: int = 23,
    ) -> Dict[str, Any]:
        buildings = [
            self._norm_text(item)
            for item in (expected_buildings if isinstance(expected_buildings, list) else [])
            if self._norm_text(item)
        ] or self._configured_buildings()
        max_hour = max(0, min(23, int(max_hour)))
        with closing(self._connect()) as conn:
            self._ensure_schema(conn)
            rows = conn.execute(
                f"""
                SELECT {self._q('hour')}, {self._q('building')}, {self._q('status')},
                       {self._q('row_count')}, {self._q('error')}, {self._q('updated_at')}
                FROM {self._q(self.TABLE_HOUR_STATUS)}
                WHERE {self._q('business_date')}=?
                """,
                (business_date,),
            ).fetchall()
            upload_row = conn.execute(
                f"""
                SELECT {self._q('status')}, {self._q('uploaded_at')}, {self._q('record_count')},
                       {self._q('last_error')}, {self._q('updated_at')}
                FROM {self._q(self.TABLE_UPLOAD_HISTORY)}
                WHERE {self._q('business_date')}=?
                """,
                (business_date,),
            ).fetchone()

        row_map: Dict[int, Dict[str, Dict[str, Any]]] = {}
        for row in rows:
            try:
                hour = int(row["hour"])
            except Exception:  # noqa: BLE001
                continue
            building = self._norm_text(row["building"])
            if not building:
                continue
            row_map.setdefault(hour, {})[building] = {
                "status": self._norm_text(row["status"]),
                "row_count": int(row["row_count"] or 0),
                "error": self._norm_text(row["error"]),
                "updated_at": self._norm_text(row["updated_at"]),
            }

        hour_items: List[Dict[str, Any]] = []
        complete_hours = 0
        partial_hours = 0
        failed_hours = 0
        missing_hours = 0
        incomplete_hours: List[int] = []
        for hour in range(max_hour + 1):
            by_building = row_map.get(hour, {})
            success_buildings = [
                building
                for building in buildings
                if self._norm_text(by_building.get(building, {}).get("status")).lower() == "success"
            ]
            failed_buildings = [
                building
                for building in buildings
                if self._norm_text(by_building.get(building, {}).get("status")).lower() == "failed"
            ]
            missing_buildings = [building for building in buildings if building not in success_buildings]
            row_count = sum(int(by_building.get(building, {}).get("row_count") or 0) for building in success_buildings)
            updated_values = [
                self._norm_text(item.get("updated_at"))
                for item in by_building.values()
                if self._norm_text(item.get("updated_at"))
            ]
            if len(success_buildings) == len(buildings):
                status = "complete"
                complete_hours += 1
            elif success_buildings:
                status = "partial"
                partial_hours += 1
                incomplete_hours.append(hour)
            elif failed_buildings:
                status = "failed"
                failed_hours += 1
                incomplete_hours.append(hour)
            else:
                status = "missing"
                missing_hours += 1
                incomplete_hours.append(hour)
            hour_items.append(
                {
                    "hour": hour,
                    "hour_text": f"{hour:02d}:00",
                    "bucket_key": f"{business_date} {hour:02d}",
                    "status": status,
                    "success_count": len(success_buildings),
                    "expected_count": len(buildings),
                    "row_count": row_count,
                    "success_buildings": success_buildings,
                    "failed_buildings": failed_buildings,
                    "missing_buildings": missing_buildings,
                    "updated_at": max(updated_values) if updated_values else "",
                }
            )

        upload_history = {}
        if upload_row is not None:
            upload_history = {
                "status": self._norm_text(upload_row["status"]),
                "uploaded_at": self._norm_text(upload_row["uploaded_at"]),
                "record_count": int(upload_row["record_count"] or 0),
                "last_error": self._norm_text(upload_row["last_error"]),
                "updated_at": self._norm_text(upload_row["updated_at"]),
            }
        return {
            "business_date": business_date,
            "max_hour": max_hour,
            "buildings": buildings,
            "expected_building_count": len(buildings),
            "hours": hour_items,
            "summary": {
                "total_hours": len(hour_items),
                "complete_hours": complete_hours,
                "partial_hours": partial_hours,
                "failed_hours": failed_hours,
                "missing_hours": missing_hours,
                "incomplete_hours": incomplete_hours,
                "incomplete_count": len(incomplete_hours),
            },
            "upload_history": upload_history,
        }

    def _mark_business_date_uploaded(
        self,
        conn: sqlite3.Connection,
        *,
        business_date: str,
        record_count: int,
    ) -> None:
        now_text = self._now_text()
        conn.execute(
            f"""
            INSERT INTO {self._q(self.TABLE_UPLOAD_HISTORY)} (
                {self._q('business_date')}, {self._q('status')}, {self._q('uploaded_at')},
                {self._q('record_count')}, {self._q('last_error')}, {self._q('updated_at')}
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT({self._q('business_date')}) DO UPDATE SET
                {self._q('status')}=excluded.{self._q('status')},
                {self._q('uploaded_at')}=excluded.{self._q('uploaded_at')},
                {self._q('record_count')}=excluded.{self._q('record_count')},
                {self._q('last_error')}=excluded.{self._q('last_error')},
                {self._q('updated_at')}=excluded.{self._q('updated_at')}
            """,
            (business_date, "success", now_text, int(record_count), "", now_text),
        )

    def _mark_business_date_upload_failed(
        self,
        conn: sqlite3.Connection,
        *,
        business_date: str,
        error_text: str,
    ) -> None:
        now_text = self._now_text()
        conn.execute(
            f"""
            INSERT INTO {self._q(self.TABLE_UPLOAD_HISTORY)} (
                {self._q('business_date')}, {self._q('status')}, {self._q('uploaded_at')},
                {self._q('record_count')}, {self._q('last_error')}, {self._q('updated_at')}
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT({self._q('business_date')}) DO UPDATE SET
                {self._q('status')}=excluded.{self._q('status')},
                {self._q('last_error')}=excluded.{self._q('last_error')},
                {self._q('updated_at')}=excluded.{self._q('updated_at')}
            """,
            (business_date, "failed", "", 0, str(error_text or "").strip(), now_text),
        )

    def _upload_daily_if_complete(
        self,
        conn: sqlite3.Connection,
        *,
        business_date: str,
        hour: int,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        expected_buildings = self._configured_buildings()
        missing = self._missing_day_hours(conn, business_date=business_date, expected_buildings=expected_buildings)
        if hour != 23:
            self._emit(
                emit_log,
                f"[支路功率上传] 已入库但未到日终上传 business_date={business_date}, hour={hour}:00, missing={len(missing)}",
            )
            return {"uploaded": False, "reason": "not_final_hour", "missing": missing[:50]}
        if missing:
            self._emit(
                emit_log,
                f"[支路功率上传] 23点已入库但全天未齐全 business_date={business_date}, missing_count={len(missing)}, "
                f"missing_sample={missing[:20]}",
            )
            return {"uploaded": False, "reason": "day_incomplete", "missing": missing[:100]}

        upload_start = time.perf_counter()
        self._emit(emit_log, f"[支路功率上传] 日终整表上传开始 business_date={business_date}")
        client = self._client(emit_log)
        upload_fields = self._target_fields_from_bitable(client, emit_log)
        self._ensure_schema(conn, upload_fields=upload_fields)
        row_missing = self._missing_day_row_fields(
            conn,
            business_date=business_date,
            upload_fields=upload_fields,
            expected_buildings=expected_buildings,
        )
        if int(row_missing.get("missing_count", 0) or 0) > 0:
            self._emit(
                emit_log,
                f"[支路功率上传] 23点已入库但明细字段未齐全 business_date={business_date}, "
                f"row_count={row_missing.get('row_count', 0)}, "
                f"missing_count={row_missing.get('missing_count', 0)}, "
                f"missing_sample={row_missing.get('sample', [])[:20]}",
            )
            return {
                "uploaded": False,
                "reason": "row_fields_incomplete",
                "missing_count": int(row_missing.get("missing_count", 0) or 0),
                "missing": list(row_missing.get("sample", []) or [])[:100],
                "row_count": int(row_missing.get("row_count", 0) or 0),
            }
        records = self._daily_records_for_upload(conn, business_date=business_date, upload_fields=upload_fields)
        if not records:
            raise RuntimeError(f"支路功率本地暂存为空，无法上传 business_date={business_date}")

        clear_start = time.perf_counter()
        self._emit(emit_log, f"[支路功率上传] 清空目标多维表开始 table_id={self.TABLE_ID}")
        deleted = client.clear_table(
            self.TABLE_ID,
            progress_callback=self._progress_logger(emit_log, label="清空目标表", total=1),
        )
        self._emit(emit_log, f"[支路功率上传] 清空目标多维表完成 deleted={deleted}, elapsed_ms={self._elapsed_ms(clear_start)}")

        create_start = time.perf_counter()
        self._emit(emit_log, f"[支路功率上传] 整表批量上传开始 records={len(records)}")
        client.batch_create_records(
            self.TABLE_ID,
            records,
            progress_callback=self._progress_logger(emit_log, label="整表上传", total=len(records)),
        )
        self._emit(emit_log, f"[支路功率上传] 整表批量上传完成 records={len(records)}, elapsed_ms={self._elapsed_ms(create_start)}")
        self._mark_business_date_uploaded(conn, business_date=business_date, record_count=len(records))
        self._clear_local_day(conn, business_date=business_date)
        self._emit(
            emit_log,
            f"[支路功率上传] 日终整表上传完成 business_date={business_date}, records={len(records)}, "
            f"elapsed_ms={self._elapsed_ms(upload_start)}",
        )
        return {
            "uploaded": True,
            "records": len(records),
            "deleted": deleted,
            "elapsed_ms": self._elapsed_ms(upload_start),
        }

    def _parse_bundle_rows(
        self,
        *,
        bundle: _SourceBundle,
        data_bucket_dt: datetime,
        emit_log: Callable[[str], None],
    ) -> List[Dict[str, Any]]:
        building = bundle.building
        data_bucket_key = data_bucket_dt.strftime("%Y-%m-%d %H")
        hour = data_bucket_dt.hour
        for label, path in (
            ("支路功率源文件", bundle.power_file),
            ("支路电流源文件", bundle.current_file),
            ("支路开关源文件", bundle.switch_file),
        ):
            if not path.exists():
                raise FileNotFoundError(f"{building} {label}不存在或不可访问: {path}")

        read_start = time.perf_counter()
        power_rows = self._load_metric_rows(
            file_path=bundle.power_file,
            data_bucket_dt=data_bucket_dt,
            metric_label="功率",
        )
        current_rows = self._load_metric_rows(
            file_path=bundle.current_file,
            data_bucket_dt=data_bucket_dt,
            metric_label="电流",
        )
        switch_rows = self._load_metric_rows(
            file_path=bundle.switch_file,
            data_bucket_dt=data_bucket_dt,
            metric_label="开关状态",
        )
        self._emit(
            emit_log,
            f"[支路功率上传] 三源文件读取完成 building={building}, data_bucket={data_bucket_key}, "
            f"功率={len(power_rows)}, 电流={len(current_rows)}, 开关={len(switch_rows)}, elapsed_ms={self._elapsed_ms(read_start)}",
        )
        rows = self._combine_rows(
            building=building,
            power_rows=power_rows,
            current_rows=current_rows,
            switch_rows=switch_rows,
            hour=hour,
        )
        return rows

    def _parse_bundle_rows_by_hour(
        self,
        *,
        bundle: _SourceBundle,
        data_bucket_dts: List[datetime],
        emit_log: Callable[[str], None],
    ) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, str]]:
        building = bundle.building
        for label, path in (
            ("支路功率源文件", bundle.power_file),
            ("支路电流源文件", bundle.current_file),
            ("支路开关源文件", bundle.switch_file),
        ):
            if not path.exists():
                raise FileNotFoundError(f"{building} {label}不存在或不可访问: {path}")

        read_start = time.perf_counter()
        power_rows_by_hour = self._load_metric_rows_by_hour(
            file_path=bundle.power_file,
            data_bucket_dts=data_bucket_dts,
            metric_label="功率",
        )
        current_rows_by_hour = self._load_metric_rows_by_hour(
            file_path=bundle.current_file,
            data_bucket_dts=data_bucket_dts,
            metric_label="电流",
        )
        switch_rows_by_hour = self._load_metric_rows_by_hour(
            file_path=bundle.switch_file,
            data_bucket_dts=data_bucket_dts,
            metric_label="开关状态",
        )
        output: Dict[str, List[Dict[str, Any]]] = {}
        failures: Dict[str, str] = {}
        summary: List[str] = []
        for data_bucket_dt in data_bucket_dts:
            data_bucket_key = data_bucket_dt.strftime("%Y-%m-%d %H")
            power_rows = power_rows_by_hour.get(data_bucket_key, [])
            current_rows = current_rows_by_hour.get(data_bucket_key, [])
            switch_rows = switch_rows_by_hour.get(data_bucket_key, [])
            try:
                output[data_bucket_key] = self._combine_rows(
                    building=building,
                    power_rows=power_rows,
                    current_rows=current_rows,
                    switch_rows=switch_rows,
                    hour=data_bucket_dt.hour,
                )
            except Exception as exc:  # noqa: BLE001
                failures[data_bucket_key] = str(exc)
            summary.append(
                f"{data_bucket_key}:功率={len(power_rows)},电流={len(current_rows)},开关={len(switch_rows)}"
            )
        self._emit(
            emit_log,
            f"[支路功率上传] 范围三源文件读取完成 building={building}, "
            f"hours={';'.join(summary)}, elapsed_ms={self._elapsed_ms(read_start)}",
        )
        return output, failures

    def _process_bundle(
        self,
        conn: sqlite3.Connection,
        *,
        bundle: _SourceBundle,
        storage_bucket_key: str,
        data_bucket_dt: datetime,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        building = bundle.building
        business_date = data_bucket_dt.strftime("%Y-%m-%d")
        data_bucket_key = data_bucket_dt.strftime("%Y-%m-%d %H")
        hour = data_bucket_dt.hour
        rows = self._parse_bundle_rows(
            bundle=bundle,
            data_bucket_dt=data_bucket_dt,
            emit_log=emit_log,
        )
        created, updated = self._upsert_hour_rows(
            conn,
            business_date=business_date,
            hour=hour,
            rows=rows,
        )
        self._mark_hour_status(
            conn,
            business_date=business_date,
            hour=hour,
            building=building,
            storage_bucket_key=storage_bucket_key,
            data_bucket_key=data_bucket_key,
            status="success",
            bundle=bundle,
            row_count=len(rows),
            created_count=created,
            updated_count=updated,
        )
        conn.commit()
        self._emit(
            emit_log,
            f"[支路功率上传] 本地入库完成 building={building}, business_date={business_date}, hour={hour}:00, "
            f"rows={len(rows)}, created={created}, updated={updated}",
        )
        return {"building": building, "rows": len(rows), "created": created, "updated": updated}

    @classmethod
    def _row_key(cls, row: Dict[str, Any]) -> tuple[str, str, str, str, str]:
        return (
            cls._norm_text(row.get(cls.FIELD_BUILDING)),
            cls._norm_text(row.get(cls.FIELD_ROOM)),
            cls._norm_text(row.get(cls.FIELD_ROW)),
            cls._norm_text(row.get(cls.FIELD_BRANCH_ID)),
            cls._norm_text(row.get(cls.FIELD_PDU_ID)),
        )

    @classmethod
    def _feishu_cell_text(cls, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, dict):
            for key in ("text", "name", "value"):
                text = cls._norm_text(value.get(key))
                if text:
                    return text
            return cls._norm_text(value)
        if isinstance(value, list):
            parts = [cls._feishu_cell_text(item) for item in value]
            return " ".join(part for part in parts if part).strip()
        return cls._norm_text(value)

    def _target_record_index(
        self,
        client: FeishuBitableClient,
        emit_log: Callable[[str], None],
    ) -> tuple[Dict[tuple[str, str, str, str, str], str], int]:
        list_start = time.perf_counter()
        records = client.list_records(self.TABLE_ID, page_size=500, max_records=0)
        index: Dict[tuple[str, str, str, str, str], str] = {}
        duplicates = 0
        for item in records:
            if not isinstance(item, dict):
                continue
            record_id = self._norm_text(item.get("record_id") or item.get("id"))
            fields = item.get("fields", {}) if isinstance(item.get("fields", {}), dict) else {}
            key = (
                self._feishu_cell_text(fields.get(self.FIELD_BUILDING)),
                self._feishu_cell_text(fields.get(self.FIELD_ROOM)),
                self._feishu_cell_text(fields.get(self.FIELD_ROW)),
                self._feishu_cell_text(fields.get(self.FIELD_BRANCH_ID)),
                self._feishu_cell_text(fields.get(self.FIELD_PDU_ID)),
            )
            if not record_id or not all(key):
                continue
            if key in index:
                duplicates += 1
                continue
            index[key] = record_id
        self._emit(
            emit_log,
            f"[支路功率上传] 目标多维表索引完成 records={len(records)}, indexed={len(index)}, "
            f"duplicates={duplicates}, elapsed_ms={self._elapsed_ms(list_start)}",
        )
        return index, duplicates

    def _direct_merge_uploaded_day_hour(
        self,
        *,
        rows: List[Dict[str, Any]],
        business_date: str,
        hour: int,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        if not rows:
            raise RuntimeError(f"支路信息指定小时没有可直传的数据 business_date={business_date}, hour={hour}:00")
        total_start = time.perf_counter()
        self._emit(
            emit_log,
            f"[支路功率上传] 已上传日期指定小时直传开始 business_date={business_date}, "
            f"hour={hour}:00, rows={len(rows)}",
        )
        client = self._client(emit_log)
        self._target_fields_from_bitable(client, emit_log)
        target_index, target_duplicates = self._target_record_index(client, emit_log)
        hour_fields = list(self._hour_fields(hour))

        source_by_key: Dict[tuple[str, str, str, str, str], Dict[str, Any]] = {}
        source_duplicates = 0
        for row in rows:
            key = self._row_key(row)
            if not all(key):
                continue
            if key in source_by_key:
                source_duplicates += 1
            source_by_key[key] = row

        create_fields: List[Dict[str, Any]] = []
        update_records: List[Dict[str, Any]] = []
        for key, row in source_by_key.items():
            hour_payload = {
                field: row.get(field)
                for field in hour_fields
                if row.get(field) is not None and self._norm_text(row.get(field)) != ""
            }
            if not hour_payload:
                continue
            record_id = target_index.get(key)
            if record_id:
                update_records.append({"record_id": record_id, "fields": hour_payload})
            else:
                fields = {
                    self.FIELD_BUILDING: key[0],
                    self.FIELD_ROOM: key[1],
                    self.FIELD_ROW: key[2],
                    self.FIELD_BRANCH_ID: key[3],
                    self.FIELD_PDU_ID: key[4],
                }
                fields.update(hour_payload)
                create_fields.append(fields)

        if update_records:
            update_start = time.perf_counter()
            self._emit(emit_log, f"[支路功率上传] 指定小时批量更新开始 records={len(update_records)}")
            client.batch_update_records(
                self.TABLE_ID,
                update_records,
                progress_callback=self._progress_logger(emit_log, label="指定小时更新", total=len(update_records)),
            )
            self._emit(
                emit_log,
                f"[支路功率上传] 指定小时批量更新完成 records={len(update_records)}, "
                f"elapsed_ms={self._elapsed_ms(update_start)}",
            )
        if create_fields:
            create_start = time.perf_counter()
            self._emit(emit_log, f"[支路功率上传] 指定小时批量新增开始 records={len(create_fields)}")
            client.batch_create_records(
                self.TABLE_ID,
                create_fields,
                progress_callback=self._progress_logger(emit_log, label="指定小时新增", total=len(create_fields)),
            )
            self._emit(
                emit_log,
                f"[支路功率上传] 指定小时批量新增完成 records={len(create_fields)}, "
                f"elapsed_ms={self._elapsed_ms(create_start)}",
            )
        result = {
            "uploaded": True,
            "mode": "direct_uploaded_day_hour",
            "business_date": business_date,
            "hour_field": f"{hour}:00",
            "parsed": len(rows),
            "unique": len(source_by_key),
            "updated": len(update_records),
            "created": len(create_fields),
            "target_duplicates": target_duplicates,
            "source_duplicates": source_duplicates,
            "elapsed_ms": self._elapsed_ms(total_start),
        }
        self._emit(
            emit_log,
            f"[支路功率上传] 已上传日期指定小时直传完成 business_date={business_date}, hour={hour}:00, "
            f"parsed={result['parsed']}, unique={result['unique']}, updated={result['updated']}, "
            f"created={result['created']}, target_duplicates={target_duplicates}, source_duplicates={source_duplicates}, "
            f"elapsed_ms={result['elapsed_ms']}",
        )
        return result

    def continue_manual_hour_from_source_files(
        self,
        *,
        bucket_key: str,
        source_units: List[Dict[str, Any]],
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        units = [unit for unit in (source_units or []) if isinstance(unit, dict)]
        if not units:
            raise RuntimeError(f"没有可处理的支路源文件 bucket={bucket_key}")
        storage_bucket_dt = self._parse_bucket_datetime(bucket_key)
        data_bucket_dt = self._resolve_data_bucket_datetime(bucket_key=bucket_key, source_units=units)
        business_date = data_bucket_dt.strftime("%Y-%m-%d")
        hour = data_bucket_dt.hour
        bundles = self._normalize_source_bundles(units)
        if not bundles:
            raise RuntimeError(f"没有可处理的支路源文件 bundle bucket={bucket_key}")
        with closing(self._connect()) as conn:
            self._ensure_schema(conn)
            already_uploaded = self._is_business_date_uploaded(conn, business_date=business_date)
        if not already_uploaded:
            self._emit(
                emit_log,
                f"[支路功率上传] 指定小时日期未上传，改为写入本地暂存 business_date={business_date}, "
                f"storage_bucket={storage_bucket_dt.strftime('%Y-%m-%d %H')}, hour={hour}:00",
            )
            return self.continue_from_source_files(
                bucket_key=bucket_key,
                source_units=source_units,
                emit_log=emit_log,
            )

        parse_start = time.perf_counter()
        all_rows: List[Dict[str, Any]] = []
        failed: List[Dict[str, Any]] = []
        no_data: List[Dict[str, Any]] = []
        self._emit(
            emit_log,
            f"[支路功率上传] 指定小时日期已上传，跳过本地暂存并直传多维 business_date={business_date}, "
            f"hour={hour}:00, buildings={len(bundles)}",
        )
        for bundle in bundles:
            try:
                rows = self._parse_bundle_rows(
                    bundle=bundle,
                    data_bucket_dt=data_bucket_dt,
                    emit_log=emit_log,
                )
                all_rows.extend(rows)
            except Exception as exc:  # noqa: BLE001
                error_text = str(exc)
                if self._is_no_data_error(error_text):
                    no_data.append({"building": bundle.building, "reason": error_text})
                    self._emit(
                        emit_log,
                        f"[支路功率上传] 指定小时直传源文件无数据，跳过 building={bundle.building}, "
                        f"bucket={data_bucket_dt.strftime('%Y-%m-%d %H')}, reason={error_text}",
                    )
                    continue
                failed.append({"building": bundle.building, "error": error_text})
                self._emit(emit_log, f"[支路功率上传][失败] 指定小时直传解析失败 building={bundle.building}, error={exc}")
        if failed:
            raise RuntimeError(f"支路信息指定小时直传存在失败楼栋: {failed}")
        if not all_rows:
            return {
                "ok": True,
                "status": "no_data",
                "bucket_key": bucket_key,
                "storage_bucket_key": storage_bucket_dt.strftime("%Y-%m-%d %H"),
                "data_bucket_key": data_bucket_dt.strftime("%Y-%m-%d %H"),
                "business_date": business_date,
                "hour_field": f"{hour}:00",
                "mode": "direct_uploaded_day_hour_no_data",
                "no_data_buildings": no_data,
                "sqlite_path": str(self._db_path()),
            }
        self._emit(
            emit_log,
            f"[支路功率上传] 指定小时直传解析完成 business_date={business_date}, hour={hour}:00, "
            f"rows={len(all_rows)}, elapsed_ms={self._elapsed_ms(parse_start)}",
        )
        upload_result = self._direct_merge_uploaded_day_hour(
            rows=all_rows,
            business_date=business_date,
            hour=hour,
            emit_log=emit_log,
        )
        return {
            "ok": True,
            "status": "success",
            "bucket_key": bucket_key,
            "storage_bucket_key": storage_bucket_dt.strftime("%Y-%m-%d %H"),
            "data_bucket_key": data_bucket_dt.strftime("%Y-%m-%d %H"),
            "business_date": business_date,
            "hour_field": f"{hour}:00",
            "mode": "direct_uploaded_day_hour",
            "upload_result": upload_result,
            "sqlite_path": str(self._db_path()),
        }

    def continue_range_from_source_files(
        self,
        *,
        bucket_keys: List[str],
        source_units: List[Dict[str, Any]],
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        total_start = time.perf_counter()
        units = [unit for unit in (source_units or []) if isinstance(unit, dict)]
        if not units:
            raise RuntimeError("没有可入库的支路范围源文件")
        data_bucket_dts = self._resolve_range_data_bucket_datetimes(
            bucket_keys=bucket_keys,
            source_units=units,
        )
        if len(data_bucket_dts) <= 1:
            bucket_key = data_bucket_dts[0].strftime("%Y-%m-%d %H")
            return self.continue_manual_hour_from_source_files(
                bucket_key=bucket_key,
                source_units=units,
                emit_log=emit_log,
            )
        bundles = self._normalize_range_source_bundles(units, data_bucket_dts=data_bucket_dts)
        if not bundles:
            raise RuntimeError("没有可入库的支路范围源文件 bundle")

        date_groups: Dict[str, List[datetime]] = {}
        for data_bucket_dt in data_bucket_dts:
            date_groups.setdefault(data_bucket_dt.strftime("%Y-%m-%d"), []).append(data_bucket_dt)
        for items in date_groups.values():
            items.sort()

        target_bucket_keys = [item.strftime("%Y-%m-%d %H") for item in data_bucket_dts]
        self._emit(
            emit_log,
            f"[支路功率上传] 范围入库开始 buckets={','.join(target_bucket_keys)}, buildings={len(bundles)}",
        )
        processed: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []
        no_data: List[Dict[str, Any]] = []
        upload_results: List[Dict[str, Any]] = []

        with closing(self._connect()) as conn:
            self._ensure_schema(conn)
            for business_date, bucket_dts in sorted(date_groups.items()):
                already_uploaded = self._is_business_date_uploaded(conn, business_date=business_date)
                mode = "direct_uploaded_day_hour" if already_uploaded else "local_hour_rows"
                self._emit(
                    emit_log,
                    f"[支路功率上传] 范围日期处理 business_date={business_date}, mode={mode}, "
                    f"hours={','.join(item.strftime('%H:00') for item in bucket_dts)}",
                )
                rows_for_direct: Dict[str, List[Dict[str, Any]]] = {
                    item.strftime("%Y-%m-%d %H"): [] for item in bucket_dts
                }
                date_failed = False
                for bundle in bundles:
                    try:
                        rows_by_bucket, parse_failures = self._parse_bundle_rows_by_hour(
                            bundle=bundle,
                            data_bucket_dts=bucket_dts,
                            emit_log=emit_log,
                        )
                        for data_bucket_dt in bucket_dts:
                            data_bucket_key = data_bucket_dt.strftime("%Y-%m-%d %H")
                            hour = data_bucket_dt.hour
                            parse_error = str(parse_failures.get(data_bucket_key, "") or "").strip()
                            if parse_error:
                                if self._is_no_data_error(parse_error):
                                    no_data.append(
                                        {
                                            "bucket_key": data_bucket_key,
                                            "building": bundle.building,
                                            "reason": parse_error,
                                        }
                                    )
                                    if not already_uploaded:
                                        self._clear_hour_values(
                                            conn,
                                            business_date=business_date,
                                            hour=hour,
                                            building=bundle.building,
                                        )
                                        self._delete_hour_status(
                                            conn,
                                            business_date=business_date,
                                            hour=hour,
                                            building=bundle.building,
                                        )
                                    self._emit(
                                        emit_log,
                                        f"[支路功率上传] 范围小时无数据，保持未入库 "
                                        f"building={bundle.building}, bucket={data_bucket_key}, reason={parse_error}",
                                    )
                                    continue
                                date_failed = True
                                failures.append(
                                    {
                                        "bucket_key": data_bucket_key,
                                        "building": bundle.building,
                                        "error": parse_error,
                                    }
                                )
                                if not already_uploaded:
                                    self._mark_hour_status(
                                        conn,
                                        business_date=business_date,
                                        hour=hour,
                                        building=bundle.building,
                                        storage_bucket_key=data_bucket_key,
                                        data_bucket_key=data_bucket_key,
                                        status="failed",
                                        bundle=bundle,
                                        error=parse_error,
                                    )
                                self._emit(
                                    emit_log,
                                    f"[支路功率上传][失败] 范围小时解析失败 building={bundle.building}, "
                                    f"bucket={data_bucket_key}, error={parse_error}",
                                )
                                continue
                            rows = rows_by_bucket.get(data_bucket_key, [])
                            if already_uploaded:
                                rows_for_direct[data_bucket_key].extend(rows)
                                continue
                            created, updated = self._upsert_hour_rows(
                                conn,
                                business_date=business_date,
                                hour=hour,
                                rows=rows,
                            )
                            self._mark_hour_status(
                                conn,
                                business_date=business_date,
                                hour=hour,
                                building=bundle.building,
                                storage_bucket_key=data_bucket_key,
                                data_bucket_key=data_bucket_key,
                                status="success",
                                bundle=bundle,
                                row_count=len(rows),
                                created_count=created,
                                updated_count=updated,
                            )
                            processed.append(
                                {
                                    "bucket_key": data_bucket_key,
                                    "building": bundle.building,
                                    "row_count": len(rows),
                                    "created": created,
                                    "updated": updated,
                                    "mode": "local_hour_rows",
                                }
                            )
                        conn.commit()
                    except Exception as exc:  # noqa: BLE001
                        date_failed = True
                        error_text = str(exc)
                        self._emit(
                            emit_log,
                            f"[支路功率上传][失败] 范围解析失败 building={bundle.building}, error={error_text}",
                        )
                        for data_bucket_dt in bucket_dts:
                            data_bucket_key = data_bucket_dt.strftime("%Y-%m-%d %H")
                            failures.append(
                                {
                                    "bucket_key": data_bucket_key,
                                    "building": bundle.building,
                                    "error": error_text,
                                }
                            )
                            if already_uploaded:
                                continue
                            self._mark_hour_status(
                                conn,
                                business_date=business_date,
                                hour=data_bucket_dt.hour,
                                building=bundle.building,
                                storage_bucket_key=data_bucket_key,
                                data_bucket_key=data_bucket_key,
                                status="failed",
                                bundle=bundle,
                                error=error_text,
                            )
                        conn.commit()

                if already_uploaded:
                    if date_failed:
                        continue
                    for data_bucket_dt in bucket_dts:
                        data_bucket_key = data_bucket_dt.strftime("%Y-%m-%d %H")
                        if not rows_for_direct.get(data_bucket_key):
                            self._emit(
                                emit_log,
                                f"[支路功率上传] 范围直传跳过无数据小时 bucket={data_bucket_key}",
                            )
                            continue
                        try:
                            upload_result = self._direct_merge_uploaded_day_hour(
                                rows=rows_for_direct.get(data_bucket_key, []),
                                business_date=business_date,
                                hour=data_bucket_dt.hour,
                                emit_log=emit_log,
                            )
                            processed.append(
                                {
                                    "bucket_key": data_bucket_key,
                                    "mode": "direct_uploaded_day_hour",
                                    "result": upload_result,
                                }
                            )
                            upload_results.append({"bucket_key": data_bucket_key, "result": upload_result})
                        except Exception as exc:  # noqa: BLE001
                            error_text = str(exc)
                            failures.append({"bucket_key": data_bucket_key, "building": "", "error": error_text})
                            self._emit(
                                emit_log,
                                f"[支路功率上传][失败] 范围直传失败 bucket={data_bucket_key}, error={error_text}",
                            )
                    continue

                upload_result: Dict[str, Any] = {"uploaded": False, "reason": "has_failures" if date_failed else ""}
                if not date_failed:
                    try:
                        upload_result = self._upload_daily_if_complete(
                            conn,
                            business_date=business_date,
                            hour=bucket_dts[-1].hour,
                            emit_log=emit_log,
                        )
                        conn.commit()
                    except Exception as exc:  # noqa: BLE001
                        if bucket_dts[-1].hour == 23:
                            self._mark_business_date_upload_failed(
                                conn,
                                business_date=business_date,
                                error_text=str(exc),
                            )
                            conn.commit()
                        raise
                upload_results.append({"business_date": business_date, "result": upload_result})

        if failures:
            raise RuntimeError(f"支路信息范围入库存在失败: {failures}")
        result = {
            "ok": True,
            "status": "success",
            "mode": "range_from_source_files",
            "bucket_key": target_bucket_keys[0] if target_bucket_keys else "",
            "target_bucket_keys": target_bucket_keys,
            "processed": processed,
            "no_data": no_data,
            "failed": [],
            "upload_results": upload_results,
            "sqlite_path": str(self._db_path()),
            "elapsed_ms": self._elapsed_ms(total_start),
        }
        self._emit(
            emit_log,
            f"[支路功率上传] 范围入库完成 buckets={','.join(target_bucket_keys)}, "
            f"processed={len(processed)}, elapsed_ms={result['elapsed_ms']}",
        )
        return result

    def continue_from_source_files(
        self,
        *,
        bucket_key: str,
        source_units: List[Dict[str, Any]],
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        total_start = time.perf_counter()
        units = [unit for unit in (source_units or []) if isinstance(unit, dict)]
        if not units:
            raise RuntimeError(f"没有可入库的支路源文件 bucket={bucket_key}")
        storage_bucket_dt = self._parse_bucket_datetime(bucket_key)
        data_bucket_dt = self._resolve_data_bucket_datetime(bucket_key=bucket_key, source_units=units)
        business_date = data_bucket_dt.strftime("%Y-%m-%d")
        hour = data_bucket_dt.hour
        storage_bucket_key = storage_bucket_dt.strftime("%Y-%m-%d %H")
        data_bucket_key = data_bucket_dt.strftime("%Y-%m-%d %H")
        bundles = self._normalize_source_bundles(units)
        if not bundles:
            raise RuntimeError(f"没有可入库的支路源文件 bundle bucket={bucket_key}")
        self._emit(
            emit_log,
            f"[支路功率上传] 每小时本地入库开始 storage_bucket={storage_bucket_key}, "
            f"data_bucket={data_bucket_key}, business_date={business_date}, hour={hour}:00, buildings={len(bundles)}",
        )

        successes: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []
        no_data: List[Dict[str, Any]] = []
        with closing(self._connect()) as conn:
            self._ensure_schema(conn)
            for bundle in bundles:
                self._emit(
                    emit_log,
                    f"[支路功率上传] 开始处理 building={bundle.building}, power={bundle.power_file}, "
                    f"current={bundle.current_file}, switch={bundle.switch_file}",
                )
                try:
                    successes.append(
                        self._process_bundle(
                            conn,
                            bundle=bundle,
                            storage_bucket_key=storage_bucket_key,
                            data_bucket_dt=data_bucket_dt,
                            emit_log=emit_log,
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    error_text = str(exc)
                    if self._is_no_data_error(error_text):
                        no_data.append({"building": bundle.building, "reason": error_text})
                        self._clear_hour_values(
                            conn,
                            business_date=business_date,
                            hour=hour,
                            building=bundle.building,
                        )
                        self._delete_hour_status(
                            conn,
                            business_date=business_date,
                            hour=hour,
                            building=bundle.building,
                        )
                        conn.commit()
                        self._emit(
                            emit_log,
                            f"[支路功率上传] 目标小时无数据，保持未入库 "
                            f"building={bundle.building}, data_bucket={data_bucket_key}, reason={error_text}",
                        )
                        continue
                    failures.append({"building": bundle.building, "error": error_text})
                    self._mark_hour_status(
                        conn,
                        business_date=business_date,
                        hour=hour,
                        building=bundle.building,
                        storage_bucket_key=storage_bucket_key,
                        data_bucket_key=data_bucket_key,
                        status="failed",
                        bundle=bundle,
                        error=error_text,
                    )
                    conn.commit()
                    self._emit(emit_log, f"[支路功率上传][失败] building={bundle.building}, error={error_text}")

            upload_result: Dict[str, Any] = {"uploaded": False, "reason": "has_failures" if failures else ""}
            if not failures:
                try:
                    upload_result = self._upload_daily_if_complete(
                        conn,
                        business_date=business_date,
                        hour=hour,
                        emit_log=emit_log,
                    )
                except Exception as exc:  # noqa: BLE001
                    if hour == 23:
                        self._mark_business_date_upload_failed(
                            conn,
                            business_date=business_date,
                            error_text=str(exc),
                        )
                        conn.commit()
                    raise

        result = {
            "ok": not failures,
            "status": "failed" if failures else "success",
            "bucket_key": bucket_key,
            "storage_bucket_key": storage_bucket_key,
            "data_bucket_key": data_bucket_key,
            "business_date": business_date,
            "hour_field": f"{hour}:00",
            "ingested_buildings": successes,
            "no_data_buildings": no_data,
            "failed_buildings": failures,
            "upload_result": upload_result,
            "elapsed_ms": self._elapsed_ms(total_start),
            "sqlite_path": str(self._db_path()),
        }
        self._emit(
            emit_log,
            f"[支路功率上传] 处理完成 status={result['status']}, storage_bucket={storage_bucket_key}, "
            f"data_bucket={data_bucket_key}, success={len(successes)}, failed={len(failures)}, "
            f"uploaded={bool(upload_result.get('uploaded'))}, elapsed_ms={result['elapsed_ms']}",
        )
        if failures:
            raise RuntimeError(f"支路信息本地入库存在失败楼栋: {failures}")
        return result
