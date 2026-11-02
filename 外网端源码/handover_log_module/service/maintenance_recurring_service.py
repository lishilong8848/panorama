from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import date, datetime
from typing import Any

from app.config.handover_segment_store import building_code_from_name
from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore


VALUE_KEYS = ("maintenance_item", "maintenance_party", "completion", "executor")


def normalize_maintenance_item(value: str) -> str:
    return "".join(value.split()).casefold()


class MaintenanceRecurringService:
    """Building-local recurring rows; never modifies an existing review document.

    Records contain id, values, mode, from_date, from_shift, revision and local
    created_at/updated_at timestamps. Start context is immutable. Updates and
    deletes require the current integer revision; identical creates are no-ops.
    """

    DEFAULT_KEY = "maintenance_recurring"
    MAX_RECORDS = 200
    MAX_VALUE_LENGTH = 2000
    MODES = {"every_shift", "day_only"}

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    def _store(self, building: str) -> ReviewBuildingDocumentStore:
        if not isinstance(building, str):
            raise ValueError("invalid_building")
        building = building.strip()
        if building_code_from_name(building) not in {"A", "B", "C", "D", "E"}:
            raise ValueError("invalid_building")
        return ReviewBuildingDocumentStore(config=self.config, building=building)

    @staticmethod
    def _context(duty_date: str, duty_shift: str) -> tuple[str, str]:
        if not isinstance(duty_date, str) or not isinstance(duty_shift, str):
            raise ValueError("invalid_duty_context")
        day, shift = duty_date.strip(), duty_shift.strip().lower()
        if date.fromisoformat(day).isoformat() != day or shift not in {"day", "night"}:
            raise ValueError("invalid_duty_context")
        return day, shift

    @classmethod
    def _values(cls, values: dict[str, str]) -> dict[str, str]:
        if not isinstance(values, dict) or set(values) != set(VALUE_KEYS):
            raise ValueError("invalid_values")
        result = {}
        for key in VALUE_KEYS:
            value = values[key]
            if not isinstance(value, str) or len(value) > cls.MAX_VALUE_LENGTH or "\x00" in value:
                raise ValueError("invalid_values")
            result[key] = value.strip()
        if not normalize_maintenance_item(result["maintenance_item"]):
            raise ValueError("maintenance_item_required")
        return result

    @classmethod
    def _read(cls, conn: sqlite3.Connection) -> tuple[list[dict[str, Any]], int]:
        row = conn.execute(
            "SELECT value_json, revision FROM building_defaults WHERE default_key=?",
            (cls.DEFAULT_KEY,),
        ).fetchone()
        if row is None:
            return [], 0
        # Do not use get_default's tolerant JSON fallback: corruption must abort writes.
        try:
            records = json.loads(row["value_json"])
            if not isinstance(records, list) or len(records) > cls.MAX_RECORDS:
                raise ValueError
            if type(row["revision"]) is not int or row["revision"] < 1:
                raise ValueError
            ids, items = set(), set()
            for record in records:
                if not isinstance(record, dict):
                    raise ValueError
                record_id = record["id"]
                if not isinstance(record_id, str) or str(uuid.UUID(record_id)) != record_id:
                    raise ValueError
                values = cls._values(record["values"])
                item = normalize_maintenance_item(values["maintenance_item"])
                if record_id in ids or item in items or record["mode"] not in cls.MODES:
                    raise ValueError
                if type(record["revision"]) is not int or record["revision"] < 1:
                    raise ValueError
                context = cls._context(record["from_date"], record["from_shift"])
                if context != (record["from_date"], record["from_shift"]) or values != record["values"]:
                    raise ValueError
                for key in ("created_at", "updated_at"):
                    stamp = record[key]
                    if not isinstance(stamp, str) or len(stamp) != 19:
                        raise ValueError
                    datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S")
                ids.add(record_id)
                items.add(item)
            return records, row["revision"]
        except (ValueError, TypeError, KeyError) as exc:
            raise ValueError("maintenance_recurring_corrupt") from exc

    @classmethod
    def _write(cls, conn: sqlite3.Connection, records: list[dict[str, Any]], version: int) -> None:
        conn.execute(
            """
            INSERT INTO building_defaults(default_key, value_json, revision, updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(default_key) DO UPDATE SET
                value_json=excluded.value_json,
                revision=excluded.revision,
                updated_at=excluded.updated_at
            """,
            (
                cls.DEFAULT_KEY,
                json.dumps(records, ensure_ascii=False, separators=(",", ":")),
                version + 1,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

    @staticmethod
    def _check_revision(record: dict[str, Any], revision: int | None) -> None:
        if type(revision) is not int or revision != record["revision"]:
            raise ValueError("revision_conflict")

    def list_records(self, building: str) -> list[dict[str, Any]]:
        store = self._store(building)
        if not store.db_path.exists():
            return []
        store.ensure_read_ready()
        with store.connect(read_only=True) as conn:
            records, _ = self._read(conn)
        return records

    def upsert(
        self,
        building: str,
        *,
        values: dict[str, str],
        mode: str,
        duty_date: str,
        duty_shift: str,
        record_id: str = "",
        revision: int | None = None,
    ) -> dict[str, Any]:
        values = self._values(values)
        if not isinstance(mode, str) or mode not in self.MODES:
            raise ValueError("invalid_mode")
        from_date, from_shift = self._context(duty_date, duty_shift)
        if not isinstance(record_id, str):
            raise ValueError("invalid_record_id")
        record_id = record_id.strip()
        store = self._store(building)
        store.ensure_ready()
        with store.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            records, version = self._read(conn)
            item = normalize_maintenance_item(values["maintenance_item"])
            duplicate = next(
                (r for r in records if normalize_maintenance_item(r["values"]["maintenance_item"]) == item),
                None,
            )
            if record_id:
                record = next((r for r in records if r["id"] == record_id), None)
                if record is None:
                    raise KeyError(record_id)
                self._check_revision(record, revision)
                if duplicate is not None and duplicate is not record:
                    raise ValueError("duplicate_item")
            else:
                record = duplicate

            if record is not None:
                same_values = all(
                    values[key] == record["values"][key] for key in VALUE_KEYS if key != "maintenance_item"
                ) and item == normalize_maintenance_item(record["values"]["maintenance_item"])
                unchanged = same_values and mode == record["mode"]
                if revision is not None or not unchanged or record_id:
                    self._check_revision(record, revision)
                if unchanged:
                    return record
                record.update(values=values, mode=mode, revision=record["revision"] + 1)
                record["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                if revision is not None:
                    raise ValueError("revision_conflict")
                if len(records) >= self.MAX_RECORDS:
                    raise ValueError("maintenance_recurring_limit")
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                record = {
                    "id": str(uuid.uuid4()),
                    "values": values,
                    "mode": mode,
                    "from_date": from_date,
                    "from_shift": from_shift,
                    "revision": 1,
                    "created_at": now,
                    "updated_at": now,
                }
                records.append(record)
            self._write(conn, records, version)
            return record

    def delete(self, building: str, *, record_id: str, revision: int | None = None) -> None:
        if not isinstance(record_id, str):
            raise ValueError("invalid_record_id")
        store = self._store(building)
        store.ensure_ready()
        with store.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            records, version = self._read(conn)
            record = next((r for r in records if r["id"] == record_id.strip()), None)
            if record is None:
                raise KeyError(record_id)
            self._check_revision(record, revision)
            records.remove(record)
            self._write(conn, records, version)

    def applicable_rows(
        self, building: str, duty_date: str, duty_shift: str, column_mapping: dict[str, str]
    ) -> list[dict[str, Any]]:
        context = self._context(duty_date, duty_shift)
        if not isinstance(column_mapping, dict):
            raise ValueError("invalid_column_mapping")
        columns = {}
        for key in VALUE_KEYS:
            col = column_mapping.get(key)
            if not isinstance(col, str):
                raise ValueError("invalid_column_mapping")
            col = col.strip().upper()
            if not 1 <= len(col) <= 3 or any(c < "A" or c > "Z" for c in col):
                raise ValueError("invalid_column_mapping")
            if len(col) == 3 and col > "XFD":
                raise ValueError("invalid_column_mapping")
            columns[key] = col
        if len(set(columns.values())) != len(VALUE_KEYS):
            raise ValueError("invalid_column_mapping")
        rows = []
        for record in self.list_records(building):
            # ISO dates and day < night give the existing duty-context order.
            if context < (record["from_date"], record["from_shift"]):
                continue
            if record["mode"] == "day_only" and context[1] != "day":
                continue
            rows.append({
                "row_id": f"maintenance-recurring:{record['id']}",
                "cells": {columns[key]: record["values"][key] for key in VALUE_KEYS},
            })
        return rows
