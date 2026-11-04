from __future__ import annotations

import copy
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import openpyxl

from handover_log_module.service.branch_power_upload_service import BranchPowerUploadService
from handover_log_module.service.full_cabinet_power_stats_sync_service import FullCabinetPowerStatsSyncService
from handover_log_module.service.power_alert_sync_service import _PowerAlertTable


DAY = datetime(2026, 5, 31)
BUILDING = "E楼"
FAMILIES = {
    "power_file": "branch_power_family",
    "current_file": "branch_current_family",
    "switch_file": "branch_switch_family",
    "full_cabinet_power_file": "building_full_cabinet_power_family",
}


def _hour_headers(day: datetime) -> list[datetime]:
    headers = [day - timedelta(hours=1)] + [day + timedelta(hours=hour) for hour in range(25)]
    headers[6] = day.replace(hour=5, minute=37, second=45, microsecond=123000)
    return headers


def _branch_rows(day: datetime, kind: str) -> list[list]:
    header = [None, None, None, *_hour_headers(day)]
    rows = [["报表"], header, []]
    for number, pdu, current in ((1, "A01-A1", 15.125), (2, "A01-B1", 21.5)):
        if kind == "power_file":
            point = f"{number} 支路功率"
            values = [6.249, 6.25, 6.251, 0, 0.0, 8.0] + [1.0] * 18
        elif kind == "current_file":
            point = f"{number} 支路电流"
            values = [1.0, 2.0, 3.0, None, "", current] + [4.0] * 18
        else:
            point = f"{pdu}_支路开关"
            values = ["合闸", "分闸", "合闸", None, 0, "合闸"] + ["合闸"] * 18
        rows.append(["E-202包间", "E-202-A列A路-DC019", point, 9999, *values, 9999])
    return rows


def _full_rows(day: datetime) -> list[list]:
    return [
        ["报表"],
        [None, None, None, None, *_hour_headers(day)],
        [],
        [None, "E-202", "202包间A列功率和", "A01机柜功率和", 9999,
         18, 17.999, 18.001, 0, 0.0, 22, *([1] * 18), 9999],
        [None, None, "E-202-A列A路-DC019", "总_负载功率_KW", 9999,
         117.5, 117.499, 117.501, *([1] * 21), 9999],
        [None, None, "E-202列头柜功率和", "A列功率和", 9999,
         235, 234.999, 235.001, *([1] * 21), 9999],
    ]


def _write_pair(root: Path, kind: str, rows: list[list]) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    xlsx = root / f"{kind}.xlsx"
    json_path = root / (f"{kind}.JSON" if kind == "current_file" else f"{kind}.json")
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "source"
    for row in rows:
        sheet.append(row)
    workbook.create_sheet("unused").append(["unused"])
    workbook.save(xlsx)
    workbook.close()
    payload = {
        "schema_version": 1,
        "source_family": FAMILIES[kind],
        "building": BUILDING,
        "sheets": [
            {"name": "source", "rows": rows},
            {"name": "unused", "rows": [["unused"]]},
        ],
    }
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, default=lambda value: value.isoformat()), encoding="utf-8"
    )
    return xlsx, json_path


def _write_sources(root: Path, day: datetime = DAY) -> dict[str, dict[str, Path]]:
    output: dict[str, dict[str, Path]] = {"xlsx": {}, "json": {}}
    for kind in FAMILIES:
        rows = _full_rows(day) if kind == "full_cabinet_power_file" else _branch_rows(day, kind)
        xlsx, json_path = _write_pair(root, kind, rows)
        output["xlsx"][kind] = xlsx
        output["json"][kind] = json_path
    return output


def _unit(paths: dict[str, Path]) -> dict:
    return {"building": BUILDING, **{key: str(value) for key, value in paths.items()}}


def _alert_tables() -> list[_PowerAlertTable]:
    return [
        _PowerAlertTable(key=key, name=key, table_id=f"test-{key}", view_id="", threshold=threshold)
        for key, threshold in (("cabinet", 18), ("line_head", 107.5), ("row_line", 215))
    ]


class BranchAndCabinetJsonSourceTests(unittest.TestCase):
    def test_empty_json_branch_power_is_uploaded_as_zero(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = _write_sources(Path(temp_dir))["json"]
            path = paths["power_file"]
            original = json.loads(path.read_text(encoding="utf-8"))
            for value in (None, "", "  "):
                with self.subTest(value=value):
                    payload = copy.deepcopy(original)
                    payload["sheets"][0]["rows"][3][7] = value
                    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                    records = self._daily_records(paths)
                    self.assertEqual(records[0]["功率-3:00"], 0)

    def test_empty_json_cabinet_power_is_counted_as_zero(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paths = _write_sources(root / "sources")["json"]
            path = paths["full_cabinet_power_file"]
            original = json.loads(path.read_text(encoding="utf-8"))
            # Each recognized statistic type must be checked before any table is touched.
            for row_index in (3, 4, 5):
                for value in (None, "", "  "):
                    with self.subTest(row=row_index + 1, value=value):
                        payload = copy.deepcopy(original)
                        payload["sheets"][0]["rows"][row_index][8] = value
                        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                        full = FullCabinetPowerStatsSyncService({"paths": {"runtime_state_root": str(root / "state")}})
                        parsed = full._parse_metric_file(file_path=path, building=BUILDING, business_date="2026-05-31")
                        key = ("cabinet", "line_head", "row_line")[row_index - 3]
                        self.assertEqual(parsed[key][0].powers[3], 0)

    def test_excel_empty_power_keeps_existing_zero_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = _write_sources(Path(temp_dir))["xlsx"]
            for kind in ("power_file", "full_cabinet_power_file"):
                workbook = openpyxl.load_workbook(paths[kind])
                workbook.active.cell(row=4, column=8 if kind == "power_file" else 9).value = None
                workbook.save(paths[kind])
                workbook.close()
            records = self._daily_records(paths)
            self.assertEqual(records[0]["功率-3:00"], 0)
            parsed = FullCabinetPowerStatsSyncService({})._parse_metric_file(
                file_path=paths["full_cabinet_power_file"], building=BUILDING, business_date="2026-05-31"
            )
            self.assertEqual(parsed["cabinet"][0].powers[3], 0)

    def test_json_truncated_row_uses_zero_and_empty_cabinet_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = _write_sources(Path(temp_dir))["json"]
            for kind in ("power_file", "full_cabinet_power_file"):
                path = paths[kind]
                original = json.loads(path.read_text(encoding="utf-8"))
                payload = copy.deepcopy(original)
                payload["sheets"][0]["rows"][3] = payload["sheets"][0]["rows"][3][:8 if kind == "power_file" else 9]
                path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                if kind == "power_file":
                    self.assertEqual(self._daily_records(paths)[0]["功率-4:00"], 0)
                else:
                    parsed = FullCabinetPowerStatsSyncService({})._parse_metric_file(
                        file_path=path, building=BUILDING, business_date="2026-05-31")
                    self.assertEqual(parsed["cabinet"][0].powers[4], 0)
                path.write_text(json.dumps(original, ensure_ascii=False), encoding="utf-8")

            path = paths["full_cabinet_power_file"]
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["sheets"][0]["rows"] = payload["sheets"][0]["rows"][:3]
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            service = BranchPowerUploadService({})
            with (
                patch.object(service, "_client", side_effect=AssertionError("cloud access")) as client,
                self.assertRaisesRegex(RuntimeError, "未解析出任何全机柜统计测点"),
            ):
                service.upload_day_from_source_files(
                    business_date="2026-05-31", source_units=[_unit(paths)], emit_log=lambda _text: None
                )
            client.assert_not_called()

    def test_invalid_json_power_values_are_blocked_before_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = _write_sources(Path(temp_dir))["json"]
            for kind, column in (("power_file", 7), ("full_cabinet_power_file", 8)):
                path = paths[kind]
                original = json.loads(path.read_text(encoding="utf-8"))
                for value in ("-", "NaN", "Infinity", True):
                    with self.subTest(kind=kind, value=value):
                        payload = copy.deepcopy(original)
                        payload["sheets"][0]["rows"][3][column] = value
                        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                        service = BranchPowerUploadService({})
                        with (
                            patch.object(service, "_client", side_effect=AssertionError("cloud access")) as client,
                            patch("handover_log_module.service.branch_power_upload_service.PowerAlertSyncService") as alert,
                            self.assertRaisesRegex(RuntimeError, "功率不是有效数字"),
                        ):
                            service.upload_day_from_source_files(
                                business_date="2026-05-31", source_units=[_unit(paths)], emit_log=lambda _text: None
                            )
                        client.assert_not_called()
                        alert.assert_not_called()
                path.write_text(json.dumps(original, ensure_ascii=False), encoding="utf-8")

    def test_later_building_cabinet_failure_blocks_manual_statistics_sync(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            first = _write_sources(root / "first")["json"]
            second = _write_sources(root / "second")["json"]
            for path in second.values():
                payload = json.loads(path.read_text(encoding="utf-8"))
                payload["building"] = "F楼"
                if path == second["full_cabinet_power_file"]:
                    payload["sheets"][0]["rows"][4][5] = "NaN"
                path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            second_unit = {**_unit(second), "building": "F楼"}
            service = BranchPowerUploadService({})
            with (
                patch.object(service, "_client", side_effect=AssertionError("cloud access")) as client,
                patch("handover_log_module.service.branch_power_upload_service.PowerAlertSyncService") as alert,
                patch.object(FullCabinetPowerStatsSyncService, "_new_client", side_effect=AssertionError("cloud access")) as full_client,
                self.assertRaisesRegex(RuntimeError, r"第5行.*00:00.*功率不是有效数字"),
            ):
                service.upload_day_from_source_files(
                    business_date="2026-05-31", source_units=[_unit(first), second_unit],
                    upload_main_table=False, emit_log=lambda _text: None,
                )
            client.assert_not_called()
            alert.assert_not_called()
            full_client.assert_not_called()

    def test_fallback_cabinet_row_is_checked_but_unrelated_rows_are_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = _write_sources(Path(temp_dir))["json"]
            path = paths["full_cabinet_power_file"]
            payload = json.loads(path.read_text(encoding="utf-8"))
            rows = payload["sheets"][0]["rows"]
            rows.append([None, "E-202", "202包间A列功率和", "无关测点", *([None] * 26)])
            rows.append([None, "E-202", "202包间A列功率和", "202包间A列列头柜总功率和", 9999, *(["0"] * 24), 9999])
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            service = FullCabinetPowerStatsSyncService({})
            parsed = service._parse_metric_file(file_path=path, building=BUILDING, business_date="2026-05-31")
            self.assertEqual(parsed["row_line"][-1].powers, [0.0] * 24)
            rows[-1][28] = None
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            parsed = service._parse_metric_file(file_path=path, building=BUILDING, business_date="2026-05-31")
            self.assertEqual(parsed["row_line"][-1].powers[-1], 0)

    def _daily_records(self, paths: dict[str, Path], units: list[dict] | None = None) -> list[dict]:
        branch_sync = Mock()
        branch_sync.sync_from_source_records.return_value = {
            "status": "success", "targets": {"branch": {"dry_run": True}}
        }
        full_sync = Mock()
        full_sync.sync_from_source_units.return_value = {
            "status": "success", "targets": {key: {} for key in ("cabinet", "line_head", "row_line")}
        }
        full_sync._parse_metric_file.side_effect = FullCabinetPowerStatsSyncService({})._parse_metric_file
        service = BranchPowerUploadService({})
        with (
            patch("handover_log_module.service.branch_power_upload_service.PowerAlertSyncService", return_value=branch_sync),
            patch("handover_log_module.service.branch_power_upload_service.FullCabinetPowerStatsSyncService", return_value=full_sync),
            patch.object(service, "_client", side_effect=AssertionError("cloud client must not be created")),
        ):
            result = service.upload_day_from_source_files(
                business_date="2026-05-31", source_units=units or [_unit(paths)],
                upload_main_table=False, emit_log=lambda _text: None,
            )
        self.assertEqual(result["parsed_hours"], 24)
        self.assertEqual(
            full_sync.sync_from_source_units.call_args.kwargs["source_units"],
            [{"building": BUILDING, "full_cabinet_power_file": str(paths["full_cabinet_power_file"])}],
        )
        return branch_sync.sync_from_source_records.call_args.kwargs["source_records"]

    def _full_results(
        self, path: Path, details: list[dict], state_root: Path, *, as_family: bool = False
    ) -> dict[str, list[dict]]:
        service = FullCabinetPowerStatsSyncService({"paths": {"runtime_state_root": str(state_root)}})
        captured: dict[str, list[dict]] = {}

        def replace(**kwargs):
            captured[kwargs["table"].key] = kwargs["rows"]
            return {"generated": len(kwargs["rows"])}

        with (
            patch.object(service, "_iter_target_tables", return_value=(_alert_tables(), [])),
            patch.object(service, "_new_client", return_value=object()),
            patch.object(service, "_replace_target_rows", side_effect=replace),
        ):
            result = service.sync_from_source_units(
                report_date="2026-05-31",
                source_units=[
                    {"building": BUILDING, "source_family": FAMILIES["full_cabinet_power_file"], "source_file": str(path)}
                    if as_family else {"building": BUILDING, "full_cabinet_power_file": str(path)}
                ],
                detail_records=details, emit_log=lambda _text: None,
            )
        self.assertEqual(result["status"], "success")
        return captured

    def test_json_and_xlsx_match_daily_records_and_full_statistics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            sources = _write_sources(root / "sources")
            xlsx_records = self._daily_records(sources["xlsx"])
            json_records = self._daily_records(sources["json"])
            self.assertEqual(json_records, xlsx_records)
            self.assertEqual(len(json_records), 2)
            self.assertEqual(json_records[0]["功率-0:00"], 6.249)
            self.assertEqual(json_records[0]["功率-1:00"], 6.25)
            self.assertEqual(json_records[0]["功率-3:00"], 0)
            self.assertEqual(json_records[0]["电流-4:00"], 0)
            self.assertEqual(json_records[0]["开关状态-3:00"], "未知")
            self.assertEqual(json_records[0]["开关状态-4:00"], "未知")
            self.assertEqual(json_records[0]["功率-23:00"], 1.0)

            xlsx_stats = self._full_results(
                sources["xlsx"]["full_cabinet_power_file"], xlsx_records, root / "xlsx-state"
            )
            json_stats = self._full_results(
                sources["json"]["full_cabinet_power_file"], json_records, root / "json-state", as_family=True
            )
            self.assertEqual(json_stats, xlsx_stats)
            self.assertEqual([row["PDU编号"] for row in json_stats["cabinet"]], ["A01-A1", "A01-B1"])
            self.assertEqual([row["电流值"] for row in json_stats["cabinet"]], [15.125, 21.5])
            self.assertEqual(json_stats["cabinet"][0]["机柜功率"], "22kw")
            self.assertEqual(json_stats["cabinet"][0]["时长"], "3h")
            self.assertEqual(json_stats["line_head"][0]["时长"], "2h")
            self.assertEqual(json_stats["row_line"][0]["时长"], "2h")

            branch_stats = []
            for file_type, records in (("xlsx", xlsx_records), ("json", json_records)):
                service = FullCabinetPowerStatsSyncService({
                    "paths": {"runtime_state_root": str(root / f"{file_type}-branch-state")}
                })
                normalized = [service._normalize_source_row(record) for record in records]
                self.assertTrue(all(row is not None for row in normalized))
                branch_stats.append(service._generate_branch_rows(
                    normalized, threshold=6.25, report_date="2026/05/31", data_center_name="EA118"
                ))
            self.assertEqual(branch_stats[0], branch_stats[1])
            self.assertEqual([row["时长"] for row in branch_stats[1]], ["3h", "3h"])
            self.assertEqual(branch_stats[1][0]["采集时间点"], "5:00")

    def test_family_source_units_pass_selected_cabinet_file_to_sync(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = _write_sources(Path(temp_dir))["json"]
            units = [
                {"building": BUILDING, "source_family": family, "source_file": str(paths[key])}
                for key, family in FAMILIES.items()
            ]
            self.assertEqual(len(self._daily_records(paths, units)), 2)

    def test_mixed_source_formats_flow_through_both_services_offline(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            sources = _write_sources(root / "sources")
            paths = dict(sources["json"])
            paths["current_file"] = sources["xlsx"]["current_file"]
            units = [
                {"building": BUILDING, "source_family": family, "source_file": str(paths[key])}
                for key, family in FAMILIES.items()
            ]
            branch_sync = Mock()
            branch_sync.sync_from_source_records.return_value = {
                "status": "success", "targets": {"branch": {"dry_run": True}}
            }
            captured: dict[str, list[dict]] = {}

            def replace(**kwargs):
                captured[kwargs["table"].key] = kwargs["rows"]
                return {"generated": len(kwargs["rows"])}

            service = BranchPowerUploadService({"paths": {"runtime_state_root": str(root / "state")}})
            with (
                patch("handover_log_module.service.branch_power_upload_service.PowerAlertSyncService", return_value=branch_sync),
                patch.object(service, "_client", side_effect=AssertionError("cloud access")),
                patch.object(FullCabinetPowerStatsSyncService, "_iter_target_tables", return_value=(_alert_tables(), [])),
                patch.object(FullCabinetPowerStatsSyncService, "_new_client", return_value=object()),
                patch.object(FullCabinetPowerStatsSyncService, "_replace_target_rows", side_effect=replace),
            ):
                result = service.upload_day_from_source_files(
                    business_date="2026-05-31", source_units=units, upload_main_table=False, emit_log=lambda _text: None
                )
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["parsed_hours"], 24)
            self.assertEqual([row["电流值"] for row in captured["cabinet"]], [15.125, 21.5])
            self.assertEqual(captured["line_head"][0]["时长"], "2h")
            self.assertEqual(captured["row_line"][0]["时长"], "2h")

    def test_wrong_identity_and_invalid_json_rejected_before_cloud_access(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = _write_sources(Path(temp_dir))["json"]
            cases = {
                "building": lambda data: data.update(building="A楼"),
                "family": lambda data: data.update(source_family="wrong_family"),
                "missing version": lambda data: data.pop("schema_version"),
                "boolean version": lambda data: data.update(schema_version=True),
                "float version": lambda data: data.update(schema_version=1.0),
                "future version": lambda data: data.update(schema_version=2),
                "empty sheets": lambda data: data.update(sheets=[]),
                "bad sheet name": lambda data: data["sheets"][0].update(name=""),
                "bad rows": lambda data: data["sheets"][0].update(rows="bad"),
                "bad row": lambda data: data["sheets"][0]["rows"].append("bad"),
                "nested cell": lambda data: data["sheets"][0]["rows"].append([[1]]),
                "bad second sheet": lambda data: data["sheets"][1]["rows"].append([{}]),
            }
            service = BranchPowerUploadService({})
            for kind, path in paths.items():
                original = path.read_text(encoding="utf-8")
                payload = json.loads(original)
                for case, mutate in cases.items():
                    with self.subTest(kind=kind, case=case):
                        modified = copy.deepcopy(payload)
                        mutate(modified)
                        path.write_text(json.dumps(modified, ensure_ascii=False), encoding="utf-8")
                        with (
                            patch.object(service, "_client", side_effect=AssertionError("cloud access")) as client,
                            self.assertRaises(ValueError),
                        ):
                            service.upload_day_from_source_files(
                                business_date="2026-05-31", source_units=[_unit(paths)], emit_log=lambda _text: None
                            )
                        client.assert_not_called()
                for invalid in ("{", '{"schema_version":1,"schema_version":1}', original.replace('"schema_version": 1', '"schema_version": NaN')):
                    with self.subTest(kind=kind, case="malformed"):
                        path.write_text(invalid, encoding="utf-8")
                        with self.assertRaises(ValueError):
                            service.upload_day_from_source_files(
                                business_date="2026-05-31", source_units=[_unit(paths)], emit_log=lambda _text: None
                            )
                path.write_text(original, encoding="utf-8")

    def test_selected_sources_are_revalidated_when_read(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paths = _write_sources(root / "sources")["json"]
            service = BranchPowerUploadService({})
            hours = [DAY + timedelta(hours=hour) for hour in range(24)]
            bundle = service._normalize_range_source_bundles([_unit(paths)], data_bucket_dts=hours)[0]
            for kind in ("power_file", "current_file", "switch_file"):
                path = paths[kind]
                original = path.read_text(encoding="utf-8")
                payload = json.loads(original)
                payload["building"] = "A楼"
                path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, "楼栋"):
                    service._parse_bundle_rows_by_hour(bundle=bundle, data_bucket_dts=hours, emit_log=lambda _text: None)
                path.write_text(original, encoding="utf-8")

            full_service = FullCabinetPowerStatsSyncService({"paths": {"runtime_state_root": str(root / "state")}})
            full_path = paths["full_cabinet_power_file"]
            payload = json.loads(full_path.read_text(encoding="utf-8"))
            payload["source_family"] = FAMILIES["power_file"]
            full_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            with (
                patch.object(full_service, "_iter_target_tables", return_value=(_alert_tables(), [])),
                patch.object(full_service, "_new_client", side_effect=AssertionError("cloud access")) as client,
                self.assertRaisesRegex(ValueError, "来源"),
            ):
                full_service.sync_from_source_units(
                    report_date="2026-05-31", source_units=[_unit(paths)], detail_records=[],
                    emit_log=lambda _text: None,
                )
            client.assert_not_called()

    def test_cabinet_entry_rejects_invalid_json_before_client_creation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = _write_sources(root / "sources")["json"]["full_cabinet_power_file"]
            original = path.read_text(encoding="utf-8")
            payload = json.loads(original)
            cases = {
                "building": lambda data: data.update(building="A楼"),
                "family": lambda data: data.update(source_family=FAMILIES["power_file"]),
                "version": lambda data: data.update(schema_version=2),
                "rows": lambda data: data["sheets"][0].update(rows=[{}]),
            }
            service = FullCabinetPowerStatsSyncService({"paths": {"runtime_state_root": str(root / "state")}})
            for case, mutate in cases.items():
                modified = copy.deepcopy(payload)
                mutate(modified)
                with self.subTest(case=case):
                    path.write_text(json.dumps(modified, ensure_ascii=False), encoding="utf-8")
                    with (
                        patch.object(service, "_iter_target_tables", return_value=(_alert_tables(), [])),
                        patch.object(service, "_new_client", side_effect=AssertionError("cloud access")) as client,
                        self.assertRaises(ValueError),
                    ):
                        service.sync_from_source_units(
                            report_date="2026-05-31", detail_records=[], emit_log=lambda _text: None,
                            source_units=[{"building": BUILDING, "full_cabinet_power_file": str(path)}],
                        )
                    client.assert_not_called()
            path.write_text("{", encoding="utf-8")
            with (
                patch.object(service, "_iter_target_tables", return_value=(_alert_tables(), [])),
                patch.object(service, "_new_client", side_effect=AssertionError("cloud access")) as client,
                self.assertRaises(ValueError),
            ):
                service.sync_from_source_units(
                    report_date="2026-05-31", detail_records=[], emit_log=lambda _text: None,
                    source_units=[{"building": BUILDING, "full_cabinet_power_file": str(path)}],
                )
            client.assert_not_called()

    def test_other_day_cannot_fill_missing_business_hour(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            sources = _write_sources(root / "sources")
            for kind in ("current_file", "full_cabinet_power_file"):
                json_path = sources["json"][kind]
                payload = json.loads(json_path.read_text(encoding="utf-8"))
                offset = 3 if kind == "current_file" else 4
                payload["sheets"][0]["rows"][1][offset + 24] = "2026-06-01T23:00:00"
                json_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                xlsx_path = sources["xlsx"][kind]
                workbook = openpyxl.load_workbook(xlsx_path)
                workbook.active.cell(row=2, column=offset + 25).value = datetime(2026, 6, 1, 23)
                workbook.save(xlsx_path)
                workbook.close()

            for file_type, paths in sources.items():
                with self.subTest(file_type=file_type, entry="branch"):
                    service = BranchPowerUploadService({})
                    with (
                        patch.object(service, "_client", side_effect=AssertionError("cloud access")) as client,
                        self.assertRaisesRegex(RuntimeError, "23"),
                    ):
                        service.upload_day_from_source_files(
                            business_date="2026-05-31", source_units=[_unit(paths)], emit_log=lambda _text: None
                        )
                    client.assert_not_called()
                with self.subTest(file_type=file_type, entry="cabinet"):
                    service = FullCabinetPowerStatsSyncService({"paths": {"runtime_state_root": str(root / file_type)}})
                    with self.assertRaisesRegex(RuntimeError, "缺少小时列: 23"):
                        service._parse_metric_file(
                            file_path=paths["full_cabinet_power_file"], building=BUILDING, business_date="2026-05-31"
                        )

    def test_json_current_day_can_reuse_previous_day_xlsx_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            previous_path = root / "202605" / "20260531--整日" / "20260531--full.xlsx"
            current_path = root / "202606" / "20260601--整日" / "20260601--full.json"
            previous_rows = _full_rows(DAY)
            previous_rows[3][5:29] = [1] * 23 + [20]
            previous_path.parent.mkdir(parents=True)
            previous_workbook = openpyxl.Workbook()
            for row in previous_rows:
                previous_workbook.active.append(row)
            previous_workbook.save(previous_path)
            previous_workbook.close()
            current_rows = _full_rows(DAY + timedelta(days=1))
            current_rows[3][5:29] = [18] + [1] * 23
            current_path.parent.mkdir(parents=True)
            current_path.write_text(json.dumps({
                "schema_version": 1, "source_family": FAMILIES["full_cabinet_power_file"],
                "building": BUILDING, "sheets": [{"name": "source", "rows": current_rows}],
            }, ensure_ascii=False, default=lambda value: value.isoformat()), encoding="utf-8")

            service = FullCabinetPowerStatsSyncService({"paths": {"runtime_state_root": str(root / "state")}})
            self.assertEqual(service._previous_source_candidate(current_path, "2026/06/01"), previous_path)
            parsed = service._parse_metric_file(file_path=current_path, building=BUILDING, business_date="2026-06-01")
            service._preload_previous_day_states(
                building_file_map={BUILDING: current_path}, report_date="2026/06/01", data_center_name="EA118",
                thresholds={"cabinet": 18, "line_head": 107.5, "row_line": 215},
                current_rows=parsed, emit_log=lambda _text: None,
            )
            rows = service._generate_cabinet_rows(
                parsed["cabinet"], detail_index={}, threshold=18, report_date="2026/06/01", data_center_name="EA118"
            )
            self.assertEqual(rows[0]["次数"], 0)
            self.assertEqual(rows[0]["时长"], "1h")


if __name__ == "__main__":
    unittest.main()
