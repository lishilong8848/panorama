from __future__ import annotations

import copy
from types import SimpleNamespace

import pymysql

from app.config.config_adapter import adapt_runtime_config
from app.config.config_schema_v3 import DEFAULT_CONFIG_V3
from app.modules.report_pipeline.service.local_mysql_persistence import LocalMysqlCalculationWriter


class _Cursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def executemany(self, sql, rows):
        self.connection.calls.append((sql, list(rows)))


class _Connection:
    def __init__(self):
        self.calls = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def ping(self, reconnect=False):
        assert reconnect is True

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def _result(building):
    return SimpleNamespace(
        building=building,
        records=[
            SimpleNamespace(
                type_name="原类型",
                category_name="原分类",
                item_name="PUE",
                building=building,
                calc_method="公式",
                value=1.234,
            )
        ],
    )


def test_local_mysql_reuses_one_connection_and_commits_once_per_building(monkeypatch):
    connection = _Connection()
    connect_calls = []

    def connect(**kwargs):
        connect_calls.append(kwargs)
        return connection

    monkeypatch.setattr(pymysql, "connect", connect)
    writer = LocalMysqlCalculationWriter(
        {
            "enabled": True,
            "host": "127.0.0.1",
            "port": 3306,
            "user": "root",
            "password": "123456",
            "database": "feishu_copy",
            "table": "electricity_consumption_details",
        }
    )
    client = SimpleNamespace(
        _canonical_metric_name_fn=lambda value: value,
        _dimension_mapping={"PUE": ("用电量拆分", "分析指标", "PUE")},
    )

    assert writer.persist(_result("A楼"), client, date_text="2026-08-24", skip_zero_records=False) == 1
    assert writer.persist(_result("B楼"), client, date_text="2026-08-24", skip_zero_records=False) == 1
    writer.close()

    assert len(connect_calls) == 1
    assert connection.commits == 2
    assert len(connection.calls) == 2
    assert connection.calls[0][1][0][0:3] == ("用电量拆分", "分析指标", "PUE")
    assert connection.calls[0][1][0][-1] == "A楼"
    assert "`id`" not in connection.calls[0][0]
    assert connection.closed is True


def test_local_mysql_config_reaches_runtime_feishu():
    config = copy.deepcopy(DEFAULT_CONFIG_V3)
    config["features"]["monthly_report"]["upload"]["local_mysql"]["database"] = "feishu_copy"

    runtime = adapt_runtime_config(config)

    assert runtime["feishu"]["local_mysql"]["database"] == "feishu_copy"
