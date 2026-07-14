from __future__ import annotations

from app.modules.feishu.service.sheets_client_runtime import FeishuSheetsClientRuntime
from handover_log_module.service.handover_cloud_sheet_sync_service import HandoverCloudSheetSyncService


def test_dimension_range_payloads_use_zero_based_half_open_indexes():
    client = object.__new__(FeishuSheetsClientRuntime)
    calls = []

    def request(method, url, *, payload=None, **_kwargs):
        calls.append((method, url, payload))
        return {"code": 0, "data": {}}

    client._request_json_with_auth_retry = request

    client.update_dimension_range(
        "sheet_token",
        sheet_id="sheet_1",
        major_dimension="ROWS",
        start_index=0,
        end_index=5,
        pixel_size=24,
    )
    client.delete_dimension(
        "sheet_token",
        sheet_id="sheet_1",
        major_dimension="ROWS",
        start_index=193,
        end_index=199,
    )

    assert calls[0][2]["dimension"]["startIndex"] == 0
    assert calls[0][2]["dimension"]["endIndex"] == 5
    assert calls[1][2]["dimension"]["startIndex"] == 193
    assert calls[1][2]["dimension"]["endIndex"] == 199


def test_resize_rechecks_actual_sheet_size_after_stale_end_index_error():
    service = object.__new__(HandoverCloudSheetSyncService)

    class FakeClient:
        def __init__(self):
            self.deletes = []

        def delete_dimension(self, _token, **kwargs):
            self.deletes.append(dict(kwargs))
            if len(self.deletes) == 1:
                raise RuntimeError("code=90202 dimension endIndex wrong")
            return {}

        def query_sheets(self, _token, *, sheet_cache=None, force_refresh=False):
            assert force_refresh is True
            return [
                {
                    "sheet_id": "sheet_1",
                    "row_count": 195,
                    "column_count": 10,
                }
            ]

    client = FakeClient()
    service._resize_rebuild_sheet(
        client=client,
        spreadsheet_token="sheet_token",
        sheet_id="sheet_1",
        current_rows=199,
        current_cols=10,
        target_rows=193,
        target_cols=10,
    )

    assert [(item["start_index"], item["end_index"]) for item in client.deletes] == [
        (193, 199),
        (193, 195),
    ]


def test_resize_skips_second_delete_when_sheet_is_already_target_size():
    service = object.__new__(HandoverCloudSheetSyncService)

    class FakeClient:
        def __init__(self):
            self.deletes = []

        def delete_dimension(self, _token, **kwargs):
            self.deletes.append(dict(kwargs))
            raise RuntimeError("code=90202 dimension endIndex wrong")

        def query_sheets(self, _token, *, sheet_cache=None, force_refresh=False):
            return [{"sheet_id": "sheet_1", "row_count": 193, "column_count": 10}]

    client = FakeClient()
    service._resize_rebuild_sheet(
        client=client,
        spreadsheet_token="sheet_token",
        sheet_id="sheet_1",
        current_rows=199,
        current_cols=10,
        target_rows=193,
        target_cols=10,
    )

    assert len(client.deletes) == 1
