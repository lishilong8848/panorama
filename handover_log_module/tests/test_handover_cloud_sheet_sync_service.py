from __future__ import annotations

from pathlib import Path

import openpyxl

from handover_log_module.service.handover_cloud_sheet_sync_service import HandoverCloudSheetSyncService


class FakeSheetsClient:
    def __init__(self) -> None:
        self.sheets = [
            {"sheet_id": "sheet_a_keep", "title": "A楼", "index": 0, "row_count": 30, "column_count": 12},
            {"sheet_id": "sheet_a_dup", "title": "A楼", "index": 3, "row_count": 30, "column_count": 12},
        ]
        self.sheet_counter = 10
        self.deleted: list[dict] = []
        self.added: list[dict] = []
        self.dimension_adds: list[dict] = []
        self.value_updates: list[dict] = []
        self.merge_updates: list[dict] = []
        self.unmerge_updates: list[dict] = []
        self.query_calls: list[dict] = []

    def find_or_create_date_spreadsheet(self, **kwargs):
        return {
            "spreadsheet_token": "sheet_token_1",
            "title": kwargs.get("spreadsheet_title", ""),
            "url": "https://vnet.feishu.cn/wiki/wiki_token_1",
        }

    def query_sheets(self, spreadsheet_token: str, *, sheet_cache=None, force_refresh: bool = False):  # noqa: ARG002, ANN001
        self.query_calls.append(
            {
                "spreadsheet_token": spreadsheet_token,
                "has_cache": isinstance(sheet_cache, dict),
                "force_refresh": force_refresh,
            }
        )
        return [dict(item) for item in self.sheets]

    def delete_sheet(self, spreadsheet_token: str, sheet_id: str, *, sheet_cache=None) -> None:  # noqa: ARG002, ANN001
        self.deleted.append({"spreadsheet_token": spreadsheet_token, "sheet_id": sheet_id})
        self.sheets = [item for item in self.sheets if item["sheet_id"] != sheet_id]

    def add_sheet(self, spreadsheet_token: str, title: str, index: int = 0, *, sheet_cache=None):  # noqa: ANN001
        self.sheet_counter += 1
        new_sheet = {
            "sheet_id": f"sheet_{self.sheet_counter}",
            "title": title,
            "index": index,
            "row_count": 20,
            "column_count": 20,
        }
        self.added.append({"spreadsheet_token": spreadsheet_token, "title": title, "index": index})
        self.sheets.append(new_sheet)
        self.sheets.sort(key=lambda item: int(item.get("index", 0) or 0))
        return dict(new_sheet)

    def get_or_create_named_sheet(self, spreadsheet_token: str, title: str, index: int = 0, *, sheet_cache=None):  # noqa: ANN001
        matched = [item for item in self.sheets if item["title"] == title]
        if matched:
            matched.sort(key=lambda item: int(item.get("index", 0) or 0))
            return dict(matched[0])
        return self.add_sheet(spreadsheet_token, title, index=index, sheet_cache=sheet_cache)

    def dedupe_named_sheets(self, spreadsheet_token: str, title: str, *, sheet_cache=None):  # noqa: ANN001
        matched = [item for item in self.sheets if item["title"] == title]
        if not matched:
            return {}
        matched.sort(key=lambda item: int(item.get("index", 0) or 0))
        keep = matched[0]
        for duplicate in matched[1:]:
            self.delete_sheet(spreadsheet_token, duplicate["sheet_id"], sheet_cache=sheet_cache)
        return dict(keep)

    def batch_update_values(self, spreadsheet_token: str, value_ranges: list[dict]):
        self.value_updates.append({"spreadsheet_token": spreadsheet_token, "value_ranges": value_ranges})
        return {}

    def add_dimension(self, spreadsheet_token: str, *, sheet_id: str, major_dimension: str, length: int):
        self.dimension_adds.append(
            {
                "spreadsheet_token": spreadsheet_token,
                "sheet_id": sheet_id,
                "major_dimension": major_dimension,
                "length": length,
            }
        )
        for item in self.sheets:
            if item["sheet_id"] != sheet_id:
                continue
            if str(major_dimension).upper() == "ROWS":
                item["row_count"] = int(item.get("row_count", 0) or 0) + int(length or 0)
            else:
                item["column_count"] = int(item.get("column_count", 0) or 0) + int(length or 0)
        return {"addCount": length, "majorDimension": major_dimension}

    def batch_merge_cells(self, spreadsheet_token: str, sheet_id: str, merges: list[dict]):
        self.merge_updates.append(
            {
                "spreadsheet_token": spreadsheet_token,
                "sheet_id": sheet_id,
                "merges": merges,
            }
        )
        return {}

    def batch_unmerge_cells(self, spreadsheet_token: str, sheet_id: str, merges: list[dict]):
        self.unmerge_updates.append(
            {
                "spreadsheet_token": spreadsheet_token,
                "sheet_id": sheet_id,
                "merges": merges,
            }
        )
        return {}


def create_workbook(path: Path, *, add_cross_boundary_merge: bool = False) -> None:
    workbook = openpyxl.Workbook()
    cover = workbook.active
    cover.title = "封面"
    cover["A1"] = "cover"

    sheet = workbook.create_sheet("交接班日志")
    sheet["A1"] = "标题"
    sheet["F2"] = "白班"
    sheet["C3"] = "张三/李四"
    sheet["A3"] = "固定合并标题"
    sheet.merge_cells("A3:B3")
    sheet["B18"] = '=IF(F2="白班","9:00","21:00")'
    sheet["B19"] = '=IF(F2="白班","16:00","4:00")'
    sheet["H18"] = '=C3'
    sheet["H19"] = '=C3'
    sheet["A23"] = "动态合并标题"
    sheet["C23"] = "动态右侧值"
    sheet.merge_cells("A23:B23")
    if add_cross_boundary_merge:
        sheet.merge_cells("A21:B22")
    workbook.save(path)
    workbook.close()


def build_service(monkeypatch, fake_client: FakeSheetsClient) -> HandoverCloudSheetSyncService:
    service = HandoverCloudSheetSyncService(
        {
            "_global_feishu": {"app_id": "app_id", "app_secret": "app_secret"},
            "cloud_sheet_sync": {
                "enabled": True,
                "source_sheet_name": "交接班日志",
                "sheet_names": {
                    "A楼": "A楼",
                    "B楼": "B楼",
                    "C楼": "C楼",
                    "D楼": "D楼",
                    "E楼": "E楼",
                },
            },
        }
    )
    monkeypatch.setattr(service, "_build_client", lambda: fake_client)  # noqa: SLF001
    return service


def test_select_source_sheet_requires_exact_handover_sheet(tmp_path: Path) -> None:
    file_path = tmp_path / "handover.xlsx"
    create_workbook(file_path)
    workbook = openpyxl.load_workbook(file_path)
    try:
        service = HandoverCloudSheetSyncService({"cloud_sheet_sync": {"source_sheet_name": "交接班日志"}})
        worksheet = service._select_source_sheet(workbook)  # noqa: SLF001
    finally:
        workbook.close()

    assert worksheet.title == "交接班日志"


def test_prepare_batch_spreadsheet_creates_missing_target_sheets_and_dedupes_titles(monkeypatch) -> None:
    fake_client = FakeSheetsClient()
    service = build_service(monkeypatch, fake_client)

    result = service.prepare_batch_spreadsheet(
        duty_date="2026-03-22",
        duty_date_text="3月22日",
        shift_text="夜班",
        emit_log=lambda *_args: None,
    )

    assert result["success"] is True
    assert result["status"] == "prepared"
    assert {item["title"] for item in fake_client.sheets} >= {"A楼", "B楼", "C楼", "D楼", "E楼"}
    assert fake_client.deleted == [{"spreadsheet_token": "sheet_token_1", "sheet_id": "sheet_a_dup"}]
    assert fake_client.value_updates == []


def test_collect_sheet_snapshot_splits_fixed_and_dynamic_merges_and_resolves_formula_values(tmp_path: Path) -> None:
    file_path = tmp_path / "handover.xlsx"
    create_workbook(file_path)

    service = HandoverCloudSheetSyncService({"cloud_sheet_sync": {"source_sheet_name": "交接班日志"}})
    workbook = openpyxl.load_workbook(file_path, data_only=False)
    try:
        worksheet = workbook["交接班日志"]
        snapshot = service._collect_sheet_snapshot(worksheet)  # noqa: SLF001
    finally:
        workbook.close()

    assert snapshot["values"][17][1] == "9:00"
    assert snapshot["values"][18][1] == "16:00"
    assert snapshot["values"][17][7] == "张三/李四"
    assert snapshot["values"][18][7] == "张三/李四"
    assert snapshot["fixed_header_merges"] == [
        {
            "start_row_index": 2,
            "end_row_index": 3,
            "start_column_index": 0,
            "end_column_index": 2,
        }
    ]
    assert snapshot["dynamic_merges"] == [
        {
            "start_row_index": 22,
            "end_row_index": 23,
            "start_column_index": 0,
            "end_column_index": 2,
        }
    ]
    assert snapshot["dynamic_merge_signature"]


def test_collect_sheet_snapshot_rejects_cross_boundary_merge(tmp_path: Path) -> None:
    file_path = tmp_path / "handover.xlsx"
    create_workbook(file_path, add_cross_boundary_merge=True)

    service = HandoverCloudSheetSyncService({"cloud_sheet_sync": {"source_sheet_name": "交接班日志"}})
    workbook = openpyxl.load_workbook(file_path, data_only=False)
    try:
        worksheet = workbook["交接班日志"]
        try:
            service._collect_sheet_snapshot(worksheet)  # noqa: SLF001
        except RuntimeError as exc:
            error = str(exc)
        else:  # pragma: no cover - defensive
            error = ""
    finally:
        workbook.close()

    assert "unsupported_cross_boundary_merge" in error


def test_sync_confirmed_buildings_skips_dynamic_merge_calls_when_signature_unchanged(tmp_path: Path, monkeypatch) -> None:
    file_path = tmp_path / "handover.xlsx"
    create_workbook(file_path)
    fake_client = FakeSheetsClient()
    service = build_service(monkeypatch, fake_client)

    workbook = openpyxl.load_workbook(file_path, data_only=False)
    try:
        expected_signature = service._collect_sheet_snapshot(workbook["交接班日志"])["dynamic_merge_signature"]  # noqa: SLF001
    finally:
        workbook.close()

    result = service.sync_confirmed_buildings(
        batch_meta={
            "spreadsheet_token": "sheet_token_1",
            "spreadsheet_url": "https://vnet.feishu.cn/wiki/wiki_token_1",
            "spreadsheet_title": "南通园区交接班日志-3月22日夜班",
        },
        building_items=[
            {
                "building": "A楼",
                "output_file": str(file_path),
                "revision": 3,
                "cloud_sheet_sync": {
                    "synced_row_count": 25,
                    "synced_column_count": 8,
                    "synced_merges": [
                        {
                            "start_row_index": 22,
                            "end_row_index": 23,
                            "start_column_index": 0,
                            "end_column_index": 2,
                        }
                    ],
                    "dynamic_merge_signature": expected_signature,
                },
            }
        ],
        emit_log=lambda *_args: None,
    )

    assert result["status"] == "ok"
    assert fake_client.unmerge_updates == []
    assert fake_client.merge_updates == []
    assert fake_client.dimension_adds == []
    value_ranges = fake_client.value_updates[0]["value_ranges"]
    assert any(item["range"] == "sheet_a_keep!A18:H18" and item["values"] == [["", "9:00", "", "", "", "", "", "张三/李四"]] for item in value_ranges)
    assert any(item["range"] == "sheet_a_keep!A23:A23" and item["values"] == [["动态合并标题"]] for item in value_ranges)
    assert not any(item["range"] == "sheet_a_keep!B23:B23" for item in value_ranges)
    assert result["details"]["A楼"]["dynamic_merge_signature"] == expected_signature


def test_sync_confirmed_buildings_syncs_dynamic_merges_and_expands_grid_when_needed(tmp_path: Path, monkeypatch) -> None:
    file_path = tmp_path / "handover.xlsx"
    create_workbook(file_path)
    fake_client = FakeSheetsClient()
    fake_client.sheets[0]["row_count"] = 10
    fake_client.sheets[0]["column_count"] = 4
    service = build_service(monkeypatch, fake_client)

    result = service.sync_confirmed_buildings(
        batch_meta={
            "spreadsheet_token": "sheet_token_1",
            "spreadsheet_url": "https://vnet.feishu.cn/wiki/wiki_token_1",
            "spreadsheet_title": "南通园区交接班日志-3月22日夜班",
        },
        building_items=[
            {
                "building": "A楼",
                "output_file": str(file_path),
                "revision": 1,
                "cloud_sheet_sync": {
                    "synced_row_count": 5,
                    "synced_column_count": 2,
                    "synced_merges": [
                        {
                            "start_row_index": 24,
                            "end_row_index": 25,
                            "start_column_index": 0,
                            "end_column_index": 2,
                        }
                    ],
                    "dynamic_merge_signature": "old-signature",
                },
            }
        ],
        emit_log=lambda *_args: None,
    )

    assert result["status"] == "ok"
    assert fake_client.dimension_adds == [
        {"spreadsheet_token": "sheet_token_1", "sheet_id": "sheet_a_keep", "major_dimension": "ROWS", "length": 13},
        {"spreadsheet_token": "sheet_token_1", "sheet_id": "sheet_a_keep", "major_dimension": "COLUMNS", "length": 4},
    ]
    assert fake_client.unmerge_updates[0]["merges"] == [
        {
            "start_row_index": 24,
            "end_row_index": 25,
            "start_column_index": 0,
            "end_column_index": 2,
        }
    ]
    assert fake_client.merge_updates[0]["merges"] == [
        {
            "start_row_index": 22,
            "end_row_index": 23,
            "start_column_index": 0,
            "end_column_index": 2,
        }
    ]
    assert result["details"]["A楼"]["synced_merges"] == [
        {
            "start_row_index": 22,
            "end_row_index": 23,
            "start_column_index": 0,
            "end_column_index": 2,
        }
    ]


def test_sync_confirmed_buildings_returns_failed_when_exact_handover_sheet_missing(tmp_path: Path, monkeypatch) -> None:
    file_path = tmp_path / "handover.xlsx"
    workbook = openpyxl.Workbook()
    workbook.active.title = "封面"
    workbook.create_sheet("其他页面")
    workbook.save(file_path)
    workbook.close()

    service = build_service(monkeypatch, FakeSheetsClient())

    result = service.sync_confirmed_buildings(
        batch_meta={
            "spreadsheet_token": "sheet_token_1",
            "spreadsheet_url": "https://vnet.feishu.cn/wiki/wiki_token_1",
            "spreadsheet_title": "南通园区交接班日志-3月22日夜班",
        },
        building_items=[{"building": "A楼", "output_file": str(file_path), "revision": 1, "cloud_sheet_sync": {}}],
        emit_log=lambda *_args: None,
    )

    assert result["status"] == "failed"
    assert result["failed_buildings"][0]["building"] == "A楼"
    assert "missing_source_sheet: 交接班日志" in result["failed_buildings"][0]["error"]



def test_sync_confirmed_buildings_runs_serial_and_retries_rate_limit(monkeypatch) -> None:
    service = HandoverCloudSheetSyncService({"cloud_sheet_sync": {"enabled": True}})
    monkeypatch.setattr(service, "_build_base_sheet_cache", lambda **_kwargs: {})  # noqa: SLF001
    monkeypatch.setattr("handover_log_module.service.handover_cloud_sheet_sync_service.time.sleep", lambda _sec: None)
    call_order = []
    attempts_by_building = {}

    def _fake_sync_one_building(*, spreadsheet_token, item, emit_log, base_sheet_cache=None):  # noqa: ARG001
        building = str(item.get("building", "")).strip()
        call_order.append(building)
        attempts_by_building[building] = attempts_by_building.get(building, 0) + 1
        if building == "B" and attempts_by_building[building] == 1:
            return {
                "building": building,
                "success": False,
                "error": "飞书接口调用失败: {'code': 90217, 'msg': 'too many request'}",
                "detail": {
                    "status": "failed",
                    "sheet_title": building,
                    "synced_revision": 0,
                    "rows": 0,
                    "cols": 0,
                    "merged": 0,
                    "synced_row_count": 0,
                    "synced_column_count": 0,
                    "synced_merges": [],
                    "dynamic_merge_signature": "",
                    "error": "飞书接口调用失败: {'code': 90217, 'msg': 'too many request'}",
                },
            }
        return {
            "building": building,
            "success": True,
            "error": "",
            "detail": {
                "status": "success",
                "sheet_title": building,
                "synced_revision": int(item.get("revision", 0) or 0),
                "rows": 57,
                "cols": 9,
                "merged": 10,
                "synced_row_count": 57,
                "synced_column_count": 9,
                "synced_merges": [],
                "dynamic_merge_signature": f"sig-{building}",
                "error": "",
            },
        }

    monkeypatch.setattr(service, "_sync_one_building", _fake_sync_one_building)  # noqa: SLF001

    result = service.sync_confirmed_buildings(
        batch_meta={
            "batch_key": "2026-03-23|night",
            "spreadsheet_token": "sheet_token_1",
            "spreadsheet_url": "https://example.test/wiki",
            "spreadsheet_title": "demo",
        },
        building_items=[
            {"building": "A", "output_file": "A.xlsx", "revision": 1, "cloud_sheet_sync": {}},
            {"building": "B", "output_file": "B.xlsx", "revision": 2, "cloud_sheet_sync": {}},
            {"building": "C", "output_file": "C.xlsx", "revision": 3, "cloud_sheet_sync": {}},
        ],
        emit_log=lambda *_args: None,
    )

    assert result["status"] == "ok"
    assert result["uploaded_buildings"] == ["A", "B", "C"]
    assert list(result["details"].keys()) == ["A", "B", "C"]
    assert call_order == ["A", "B", "B", "C"]
    assert attempts_by_building == {"A": 1, "B": 2, "C": 1}
