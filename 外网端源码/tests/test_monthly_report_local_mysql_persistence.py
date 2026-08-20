from __future__ import annotations

from types import SimpleNamespace

from app.modules.report_pipeline.service import feishu_upload_runtime


class _Cursor:
    def __init__(self) -> None:
        self.sql = ""
        self.executemany_calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql):
        self.sql = sql

    def fetchall(self):
        return [{"Field": name} for name in feishu_upload_runtime._MYSQL_REQUIRED_COLUMNS]

    def executemany(self, sql, rows):
        self.executemany_calls.append((sql, list(rows)))


class _Connection:
    def __init__(self) -> None:
        self.cursor_value = _Cursor()
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self.cursor_value

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def _mysql_config():
    return {
        "enabled": True,
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "password": "123456",
        "database": "feishu_copy",
        "table": "electricity_consumption_details",
        "charset": "utf8mb4",
    }


def test_local_mysql_uses_one_batch_and_one_commit(monkeypatch):
    connection = _Connection()
    monkeypatch.setattr("pymysql.connect", lambda **_kwargs: connection)

    count = feishu_upload_runtime.insert_calc_fields_to_local_mysql(
        mysql_cfg=_mysql_config(),
        fields_list=[
            {"类型": "电量", "分类": "市电", "项目": "总电量", "计算方式": "差值", "用电量": 12.5},
            {"类型": "PUE", "分类": "指标", "项目": "PUE", "计算方式": "比值", "用电量": 1.3},
        ],
        default_building="A楼",
        default_date="2026-08-20",
    )

    assert count == 2
    assert len(connection.cursor_value.executemany_calls) == 1
    assert len(connection.cursor_value.executemany_calls[0][1]) == 2
    assert "ON DUPLICATE KEY" not in connection.cursor_value.executemany_calls[0][0]
    assert "CURRENT_TIMESTAMP" in connection.cursor_value.executemany_calls[0][0]
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closed is True


def test_mysql_date_accepts_feishu_timestamp_string():
    assert feishu_upload_runtime._mysql_data_time("1787241600000") == feishu_upload_runtime._mysql_data_time(1787241600000)


def test_mysql_runs_after_feishu_calc_records(monkeypatch, tmp_path):
    order = []

    class Client:
        calc_table_id = "calc"

        def __init__(self, **_kwargs):
            pass

        def _to_feishu_date(self, _value):
            return 1

        def list_records(self, **_kwargs):
            return []

        def build_calc_record_fields(self, *_args, **_kwargs):
            return [{"类型": "电量", "分类": "市电", "项目": "总电量", "计算方式": "差值", "用电量": 12.5}]

        def batch_create_records(self, *_args, **_kwargs):
            order.append("feishu")

        def upload_attachment(self, _path):
            order.append("attachment")
            return "file-token"

        def upload_attachment_record(self, **_kwargs):
            return {}

    monkeypatch.setattr(
        feishu_upload_runtime,
        "insert_calc_fields_to_local_mysql",
        lambda **_kwargs: order.append("mysql") or 1,
    )
    source_file = tmp_path / "A.xlsx"
    source_file.write_bytes(b"xlsx")
    result = SimpleNamespace(
        source_file=str(source_file),
        building="A楼",
        month="2026-08",
        values={"PUE": 1.3},
        records=[object()],
    )
    config = {
        "feishu": {
            "enable_upload": True,
            "app_id": "id",
            "app_secret": "secret",
            "app_token": "app",
            "calc_table_id": "calc",
            "attachment_table_id": "attachment",
            "date_field_mode": "timestamp",
            "date_field_day": 1,
            "date_tz_offset_hours": 8,
            "timeout": 30,
            "report_type": "全景平台月报",
            "skip_zero_records": False,
            "local_mysql": _mysql_config(),
        }
    }

    feishu_upload_runtime.upload_results_to_feishu(
        [result],
        config,
        resolve_upload_date_from_runtime=lambda _config: "2026-08-20",
        client_factory=Client,
        emit_log=lambda _message: None,
    )

    assert order == ["feishu", "mysql", "attachment"]
