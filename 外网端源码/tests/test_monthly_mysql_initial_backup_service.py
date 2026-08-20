from __future__ import annotations

from types import SimpleNamespace

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.report_pipeline.service import monthly_mysql_initial_backup_service as module


def test_initial_backup_reads_all_records_once_and_reports_progress(monkeypatch):
    captured = {}
    insert_calls = []

    class Client:
        def __init__(self, **_kwargs):
            pass

        def list_records(self, **kwargs):
            assert kwargs["page_size"] == 500
            kwargs["progress_callback"](2, 2)
            return [
                {"record_id": "rec1", "fields": {"楼栋": "A楼", "日期": 1787241600000, "用电量": 1.2}},
                {"record_id": "rec2", "fields": {"楼栋": "B楼", "日期": 1787241600000, "用电量": 2.3}},
            ]

    monkeypatch.setattr(module, "load_calc_module", lambda: SimpleNamespace(FeishuBitableClient=Client))

    def fake_insert(**kwargs):
        insert_calls.append(kwargs)
        captured.update(kwargs)
        return len(kwargs["fields_list"])

    monkeypatch.setattr(module, "insert_calc_fields_to_local_mysql", fake_insert)
    progress = []
    result = module.MonthlyMysqlInitialBackupService(
        {
            "feishu": {
                "app_id": "id",
                "app_secret": "secret",
                "app_token": "app",
                "calc_table_id": "table",
                "attachment_table_id": "attachment",
                "local_mysql": {"enabled": True},
            }
        }
    ).run(emit_log=lambda _line: None, progress_callback=progress.append)

    assert result == {
        "status": "success",
        "fetched_records": 2,
        "total_records": 2,
        "written_records": 2,
    }
    assert len(captured["fields_list"]) == 2
    assert captured["require_building"] is False
    assert len(insert_calls) == 2
    assert insert_calls[0]["fields_list"] == []
    assert progress[-1]["progress"] == 100
    assert progress[-1]["written_records"] == 2


def test_list_records_reports_each_page_without_changing_pagination(monkeypatch):
    client = object.__new__(FeishuBitableClient)
    client.app_token = "app"
    responses = iter(
        [
            {"data": {"items": [{"record_id": "1"}, {"record_id": "2"}], "total": 3, "has_more": True, "page_token": "next"}},
            {"data": {"items": [{"record_id": "3"}], "total": 3, "has_more": False}},
        ]
    )
    monkeypatch.setattr(client, "_get_json", lambda *_args, **_kwargs: next(responses))
    progress = []

    records = client.list_records("table", progress_callback=lambda fetched, total: progress.append((fetched, total)))

    assert [item["record_id"] for item in records] == ["1", "2", "3"]
    assert progress == [(2, 3), (3, 3)]
