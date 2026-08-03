from __future__ import annotations

import openpyxl

from handover_log_module.service.handover_cloud_sheet_sync_service import HandoverCloudSheetSyncService


def _write_station_h_template(path) -> None:
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.title = "H楼"
    worksheet["H50"] = "template-boundary"
    workbook.save(path)
    workbook.close()


class _FakeSheetsClient:
    def __init__(self, target_sheet):
        self.target_sheet = dict(target_sheet) if target_sheet else {}
        self.value_batches = []

    def query_sheets(self, _spreadsheet_token, *, sheet_cache=None, force_refresh=False):
        del sheet_cache, force_refresh
        return [dict(self.target_sheet)] if self.target_sheet else []

    def dedupe_named_sheets(self, _spreadsheet_token, _sheet_title, *, sheet_cache=None):
        del sheet_cache
        return dict(self.target_sheet)

    def batch_update_values(self, _spreadsheet_token, value_ranges):
        self.value_batches.append(list(value_ranges))
        return {}

    def batch_unmerge_cells(self, *_args, **_kwargs):
        raise AssertionError("H楼值同步不应取消合并单元格")

    def batch_merge_cells(self, *_args, **_kwargs):
        raise AssertionError("H楼值同步不应重建合并单元格")

    def batch_clear_values(self, *_args, **_kwargs):
        raise AssertionError("H楼值同步不应清空整张表")

    def delete_dimension(self, *_args, **_kwargs):
        raise AssertionError("H楼值同步不应删除行列")

    def add_dimension(self, *_args, **_kwargs):
        raise AssertionError("H楼值同步不应增补行列")


def _service_with_client(tmp_path, client):
    template_path = tmp_path / "H楼交接班日志空模板.xlsx"
    _write_station_h_template(template_path)
    service = HandoverCloudSheetSyncService(
        {"cloud_sheet_sync": {"station_h_template_path": str(template_path)}}
    )
    service._build_client = lambda: client
    return service


def test_station_h_sync_updates_only_allowed_values_and_preserves_cloud_layout(tmp_path):
    target_sheet = {
        "sheet_id": "h_sheet",
        "title": "H楼",
        "index": 5,
        "row_count": 200,
        "column_count": 20,
        "merges": [
            {
                "start_row_index": 1,
                "end_row_index": 2,
                "start_column_index": 1,
                "end_column_index": 4,
            }
        ],
    }
    client = _FakeSheetsClient(target_sheet)
    service = _service_with_client(tmp_path, client)

    result = service.sync_station_h_sheet(
        batch_meta={"batch_key": "2026-08-03|day", "spreadsheet_token": "sheet_token"},
        cell_values={"B2": "2026-08-03", "H12": 7.5, "H7": "不得覆盖的静态值"},
        emit_log=lambda _message: None,
    )

    assert result["status"] == "success"
    assert result["rebuild_mode"] == "values_only_preserve_layout"
    assert result["synced_row_count"] == 200
    assert result["synced_column_count"] == 20
    assert len(client.value_batches) == 1

    value_ranges = client.value_batches[0]
    assert len(value_ranges) == len(service.STATION_H_ALLOWED_CELLS)
    values_by_range = {item["range"]: item["values"][0][0] for item in value_ranges}
    assert values_by_range["h_sheet!B2:B2"] == "2026-08-03"
    assert values_by_range["h_sheet!H12:H12"] == 7.5
    assert "h_sheet!H7:H7" not in values_by_range
    assert "h_sheet!H8:H8" not in values_by_range
    assert "h_sheet!H9:H9" not in values_by_range


def test_station_h_sync_rejects_sheet_without_template_merge_structure(tmp_path):
    client = _FakeSheetsClient(
        {
            "sheet_id": "h_sheet",
            "title": "H楼",
            "index": 5,
            "row_count": 200,
            "column_count": 20,
            "merges": [],
        }
    )
    service = _service_with_client(tmp_path, client)

    result = service.sync_station_h_sheet(
        batch_meta={"batch_key": "2026-08-03|day", "spreadsheet_token": "sheet_token"},
        cell_values={"B2": "2026-08-03"},
        emit_log=lambda _message: None,
    )

    assert result["status"] == "failed"
    assert "未检测到模板合并结构" in result["error"]
    assert client.value_batches == []
