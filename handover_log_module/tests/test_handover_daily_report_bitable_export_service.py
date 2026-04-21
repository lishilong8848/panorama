import copy

from handover_log_module.service.handover_daily_report_bitable_export_service import (
    HandoverDailyReportBitableExportService,
)


class _FakeClient:
    def __init__(self, *, report_link_ui_type="15", existing_records=None):
        self.deleted = []
        self.uploaded = []
        self.created = []
        self.updated = []
        self.list_calls = []
        self.report_link_ui_type = str(report_link_ui_type)
        self.existing_records = list(existing_records) if existing_records is not None else [
            {
                "record_id": "rec_old",
                "fields": {
                    "年度": "2026年度",
                    "日期": "2026-03-24",
                    "班次": "夜班",
                },
            }
        ]

    def list_fields(self, **_kwargs):
        return [
            {"field_name": "年度", "ui_type": "3"},
            {"field_name": "日期", "ui_type": "5"},
            {"field_name": "班次", "ui_type": "3"},
            {"field_name": "交接班日报", "ui_type": self.report_link_ui_type},
            {"field_name": "日报截图", "ui_type": "17"},
        ]

    def list_records(self, **kwargs):
        self.list_calls.append(kwargs)
        return list(self.existing_records)

    def batch_delete_records(self, **kwargs):
        self.deleted.append(kwargs)

    def upload_attachment_bytes(self, *, file_name, content, mime_type):
        self.uploaded.append({"file_name": file_name, "content": content, "mime_type": mime_type})
        return f"token_{file_name}"

    def batch_create_records(self, **kwargs):
        self.created.append(copy.deepcopy(kwargs))
        return [{"data": {"records": [{"record_id": "rec_new"}]}}]

    def update_record(self, **kwargs):
        self.updated.append(copy.deepcopy(kwargs))
        return {"code": 0}


class _RetryUrlFieldClient(_FakeClient):
    def __init__(self, *, report_link_ui_type="1"):
        super().__init__(report_link_ui_type=report_link_ui_type, existing_records=[])
        self._attempt = 0

    def batch_create_records(self, **kwargs):
        self.created.append(copy.deepcopy(kwargs))
        self._attempt += 1
        if self._attempt == 1:
            raise RuntimeError(
                "飞书接口调用失败: {'code': 1254068, 'msg': 'URLFieldConvFail', 'error': {'message': 'invalid'}}"
            )
        return [{"data": {"records": [{"record_id": "rec_retry"}]}}]


def test_daily_report_bitable_export_replaces_existing_record(tmp_path, monkeypatch):
    summary = tmp_path / "summary_sheet.png"
    external = tmp_path / "external_page.png"
    summary.write_bytes(b"summary")
    external.write_bytes(b"external")

    fake_client = _FakeClient()
    service = HandoverDailyReportBitableExportService(
        {
            "_global_feishu": {"app_id": "app", "app_secret": "secret"},
            "daily_report_bitable_export": {"enabled": True},
        }
    )
    monkeypatch.setattr(service, "_new_client", lambda _cfg: fake_client)

    result = service.export_record(
        duty_date="2026-03-24",
        duty_shift="night",
        spreadsheet_url="https://example.com/wiki",
        summary_screenshot_path=str(summary),
        external_screenshot_path=str(external),
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert result["status"] == "success"
    assert fake_client.deleted == []
    assert fake_client.created == []
    assert "CurrentValue.[日期]>=" in fake_client.list_calls[0]["filter_formula"]
    assert "CurrentValue.[日期]<" in fake_client.list_calls[0]["filter_formula"]
    assert 'TODATE("2026-03-24")' in fake_client.list_calls[0]["filter_formula"]
    assert 'TODATE("2026-03-25")' in fake_client.list_calls[0]["filter_formula"]
    assert fake_client.updated[0]["record_id"] == "rec_old"
    payload_fields = fake_client.updated[0]["fields"]
    assert payload_fields["年度"] == "2026年度"
    assert payload_fields["班次"] == "夜班"
    assert payload_fields["交接班日报"] == {"text": "https://example.com/wiki", "link": "https://example.com/wiki"}
    assert payload_fields["日报截图"] == [
        {"file_token": "token_summary_sheet.png"},
        {"file_token": "token_external_page.png"},
    ]


def test_daily_report_bitable_export_falls_back_to_plain_text_for_text_field(tmp_path, monkeypatch):
    summary = tmp_path / "summary_sheet.png"
    external = tmp_path / "external_page.png"
    summary.write_bytes(b"summary")
    external.write_bytes(b"external")

    fake_client = _FakeClient(report_link_ui_type="1")
    service = HandoverDailyReportBitableExportService(
        {
            "_global_feishu": {"app_id": "app", "app_secret": "secret"},
            "daily_report_bitable_export": {"enabled": True},
        }
    )
    monkeypatch.setattr(service, "_new_client", lambda _cfg: fake_client)

    service.export_record(
        duty_date="2026-03-24",
        duty_shift="night",
        spreadsheet_url="https://example.com/wiki",
        summary_screenshot_path=str(summary),
        external_screenshot_path=str(external),
        emit_log=lambda *_args, **_kwargs: None,
    )

    payload_fields = fake_client.updated[0]["fields"]
    assert payload_fields["交接班日报"] == "https://example.com/wiki"


def test_daily_report_bitable_export_retries_url_object_after_url_field_conv_fail(tmp_path, monkeypatch):
    summary = tmp_path / "summary_sheet.png"
    external = tmp_path / "external_page.png"
    summary.write_bytes(b"summary")
    external.write_bytes(b"external")

    fake_client = _RetryUrlFieldClient(report_link_ui_type="1")
    service = HandoverDailyReportBitableExportService(
        {
            "_global_feishu": {"app_id": "app", "app_secret": "secret"},
            "daily_report_bitable_export": {"enabled": True},
        }
    )
    monkeypatch.setattr(service, "_new_client", lambda _cfg: fake_client)
    logs = []

    result = service.export_record(
        duty_date="2026-03-24",
        duty_shift="night",
        spreadsheet_url="https://example.com/wiki",
        summary_screenshot_path=str(summary),
        external_screenshot_path=str(external),
        emit_log=logs.append,
    )

    assert result["status"] == "success"
    assert len(fake_client.created) == 2
    first_payload = fake_client.created[0]["fields_list"][0]
    second_payload = fake_client.created[1]["fields_list"][0]
    assert first_payload["交接班日报"] == "https://example.com/wiki"
    assert second_payload["交接班日报"] == {"text": "https://example.com/wiki", "link": "https://example.com/wiki"}
    assert any("URL 字段写入回退重试" in line for line in logs)


def test_daily_report_bitable_export_falls_back_to_year_shift_scope_when_exact_filter_misses(tmp_path, monkeypatch):
    summary = tmp_path / "summary_sheet.png"
    external = tmp_path / "external_page.png"
    summary.write_bytes(b"summary")
    external.write_bytes(b"external")

    fake_client = _FakeClient(existing_records=[])

    def _list_records(**kwargs):
        fake_client.list_calls.append(kwargs)
        formula = str(kwargs.get("filter_formula", ""))
        if "CurrentValue.[日期]>=" in formula:
            return []
        return [
            {
                "record_id": "rec_old",
                "fields": {
                    "年度": "2026年度",
                    "日期": "2026-03-24 12:00:00",
                    "班次": "夜班",
                },
            }
        ]

    fake_client.list_records = _list_records  # type: ignore[method-assign]
    service = HandoverDailyReportBitableExportService(
        {
            "_global_feishu": {"app_id": "app", "app_secret": "secret"},
            "daily_report_bitable_export": {"enabled": True},
        }
    )
    monkeypatch.setattr(service, "_new_client", lambda _cfg: fake_client)
    logs = []

    result = service.export_record(
        duty_date="2026-03-24",
        duty_shift="night",
        spreadsheet_url="https://example.com/wiki",
        summary_screenshot_path=str(summary),
        external_screenshot_path=str(external),
        emit_log=logs.append,
    )

    assert result["status"] == "success"
    assert len(fake_client.list_calls) == 2
    assert "CurrentValue.[日期]>=" in fake_client.list_calls[0]["filter_formula"]
    assert 'TODATE("2026-03-24")' in fake_client.list_calls[0]["filter_formula"]
    assert "CurrentValue.[日期]" not in fake_client.list_calls[1]["filter_formula"]
    assert fake_client.updated[0]["record_id"] == "rec_old"
    assert any("范围过滤匹配旧记录" in line for line in logs)
