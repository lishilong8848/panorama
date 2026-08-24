from types import SimpleNamespace

from app.modules.report_pipeline.service.feishu_upload_runtime import upload_results_to_feishu


def test_daily_electricity_replaces_all_matching_business_keys_after_create():
    events = []

    class Client:
        _canonical_metric_name_fn = staticmethod(lambda value: str(value))
        _dimension_mapping = {}

        def list_records(self, table_id, **kwargs):
            assert kwargs["max_records"] == 0
            if table_id == "calc":
                return [
                    {
                        "record_id": "old-1",
                        "fields": {
                            "类型": "用电",
                            "分类": "总量",
                            "项目": "市电",
                            "楼栋": "A楼",
                            "日期": "2026-08-23",
                        },
                    },
                    {
                        "record_id": "unrelated",
                        "fields": {
                            "类型": "用电",
                            "分类": "总量",
                            "项目": "其他",
                            "楼栋": "A楼",
                            "日期": "2026-08-23",
                        },
                    },
                ]
            return []

        def upload_calc_records(self, *_args, **_kwargs):
            events.append("create-calc")

        def batch_delete_records(self, table_id, record_ids, **_kwargs):
            events.append(("delete", table_id, list(record_ids)))
            return len(record_ids)

        def upload_attachment(self, _path):
            return "token"

        def upload_attachment_record(self, **_kwargs):
            return {}

    record = SimpleNamespace(
        type_name="用电",
        category_name="总量",
        item_name="市电",
        building="A楼",
        month="2026-08-23",
        calc_method="sum",
        value=1.0,
    )
    result = SimpleNamespace(
        building="A楼",
        source_file="A.xlsx",
        month="2026-08-23",
        values={"PUE": 1.2},
        records=[record],
    )
    config = {
        "feishu": {
            "enable_upload": True,
            "app_id": "id",
            "app_secret": "secret",
            "app_token": "app",
            "calc_table_id": "calc",
            "attachment_table_id": "attachment",
            "date_field_mode": "text",
            "date_field_day": 1,
            "date_tz_offset_hours": 8,
            "timeout": 30,
            "report_type": "全景平台月报",
            "skip_zero_records": False,
        }
    }

    upload_results_to_feishu(
        [result],
        config,
        resolve_upload_date_from_runtime=lambda _config: "2026-08-23",
        client_factory=lambda **_kwargs: Client(),
        emit_log=lambda _message: None,
    )

    assert events == ["create-calc", ("delete", "calc", ["old-1"])]
