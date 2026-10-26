from copy import deepcopy

from handover_log_module.service.handover_cabinet_shift_record_bitable_export_service import HandoverCabinetShiftRecordBitableExportService
from handover_log_module.service.handover_daily_report_bitable_export_service import HandoverDailyReportBitableExportService


DATE = "2026-10-26"
MIDNIGHT = HandoverDailyReportBitableExportService._midnight_timestamp_ms(DATE)


class Client:
    def __init__(self):
        self.records = []
        self.created = 0
        self.updated = []
        self.queries = []
        self.omit_created_ids = False
        self.fail_create = False

    def list_fields(self, **_):
        return [{"field_name": name, "type": field_type} for name, field_type in (
            ("年度", 3), ("日期", 5), ("班次", 3), ("楼栋", 3), ("交接班日报", 15),
            ("值班人员", 1), ("接班人员", 1), ("机房总规划机柜数（个）", 2),
            ("实际上电机柜数（个）", 2), ("本班组上电机柜数（个）", 2), ("本班组下电机柜数（个）", 2),
        )]

    def list_records(self, **kwargs):
        self.queries.append(kwargs)
        limit = kwargs.get("max_records", 0)
        return deepcopy(self.records[:limit] if limit else self.records)

    def batch_create_records(self, *, fields_list, **_):
        if self.fail_create:
            raise RuntimeError("create timeout")
        created = []
        for fields in fields_list:
            self.created += 1
            record = {"record_id": f"rec_{self.created}", "fields": deepcopy(fields)}
            created.append(record)
            self.records.append(record)
        return [{"code": 0, "data": {"records": [] if self.omit_created_ids else deepcopy(created)}}]

    def update_record(self, *, record_id, fields, **_):
        record = next(item for item in self.records if item["record_id"] == record_id)
        record["fields"].update(deepcopy(fields))
        self.updated.append(record_id)
        return {"code": 0, "data": {"record": deepcopy(record)}}

    def batch_update_records(self, *, records, **kwargs):
        return [self.update_record(**item, **kwargs) for item in records]

    def get_record_by_id(self, *, record_id, **_):
        return deepcopy(next(item for item in self.records if item["record_id"] == record_id))


def test_daily_create_update_and_recreate_after_cloud_record_deleted():
    client = Client()
    service = HandoverDailyReportBitableExportService({})
    service._new_client = lambda _: client
    run = lambda url: service.export_record(duty_date=DATE, duty_shift="day", spreadsheet_url=url, emit_log=lambda _: None)
    assert run("https://example.test/first")["record_id"] == "rec_1"
    assert run("https://example.test/second")["record_id"] == "rec_1"
    assert client.created == 1
    assert client.records[0]["fields"]["交接班日报"]["link"] == "https://example.test/second"
    client.records.clear()
    assert run("https://example.test/third")["record_id"] == "rec_2"
    assert len(client.records) == 1
    assert all(q["max_records"] == 0 and "2026-10-27" in q["filter_formula"] for q in client.queries)


def test_daily_matches_whole_date_and_repairs_year_in_place_without_5000_limit():
    client = Client()
    client.records = [{"record_id": f"other_{n}", "fields": {"日期": MIDNIGHT - 86400000}} for n in range(5001)]
    client.records.append({"record_id": "existing", "fields": {"年度": "2026", "日期": MIDNIGHT + 16 * 3600000, "班次": "白班"}})
    service = HandoverDailyReportBitableExportService({})
    service._new_client = lambda _: client
    result = service.export_record(duty_date=DATE, duty_shift="day", spreadsheet_url="https://example.test/new", emit_log=lambda _: None)
    assert result["record_id"] == "existing"
    assert client.created == 0
    assert client.updated == ["existing"]
    assert client.records[-1]["fields"]["年度"] == "2026年度"


def cabinet_service(client):
    service = object.__new__(HandoverCabinetShiftRecordBitableExportService)
    service.handover_cfg = {}
    service._log_prefix = "test"
    service._new_client = lambda _: client
    service._build_row = lambda session, _: {
        "building": session["building"], "duty_date": DATE, "duty_shift": session.get("duty_shift", "day"),
        "revision": 1, "shift_text": "白班", "duty_staff": "新人员", "handover_staff": "下一班",
        "planned_cabinets": 100, "powered_cabinets": 80, "shift_power_on_cabinets": 1,
        "shift_power_off_cabinets": 0, "extra_fields": {},
    }
    return service


def test_cabinet_updates_same_building_date_shift_and_creates_only_missing():
    client = Client()
    client.records = [
        {"record_id": "a_day", "fields": {"楼栋": "A楼", "日期": MIDNIGHT + 12 * 3600000, "班次": "白班"}},
        {"record_id": "a_night", "fields": {"楼栋": "A楼", "日期": MIDNIGHT, "班次": "夜班"}},
        {"record_id": "c_day", "fields": {"楼栋": "C楼", "日期": MIDNIGHT, "班次": "白班"}},
    ]
    service = cabinet_service(client)
    rows = [{"building": "A楼"}, {"building": "B楼"}]
    for _ in range(2):
        assert service.export_sessions(sessions=rows, emit_log=lambda _: None)["status"] == "ok"
    assert client.created == 1
    assert set(client.updated) == {"a_day", "rec_1"}
    assert len(client.records) == 4
    assert client.queries[0]["max_records"] == 0
    assert "2026-10-27" in client.queries[0]["filter_formula"]


def test_cabinet_missing_create_ids_is_not_success_and_retry_does_not_duplicate():
    client = Client()
    client.omit_created_ids = True
    service = cabinet_service(client)
    first = service.export_sessions(sessions=[{"building": "A楼"}], emit_log=lambda _: None)
    assert first["status"] == "failed"
    assert "创建结果不完整" in first["details"]["A楼"]["error"]
    second = service.export_sessions(sessions=[{"building": "A楼"}], emit_log=lambda _: None)
    assert second["status"] == "ok"
    assert client.created == 1


def test_cabinet_remote_write_failure_is_preserved_for_retry():
    client = Client()
    client.fail_create = True
    result = cabinet_service(client).export_sessions(sessions=[{"building": "A楼"}], emit_log=lambda _: None)
    assert result["status"] == "failed"
    assert result["details"]["A楼"]["uploaded_revision"] == 0
    assert "timeout" in result["details"]["A楼"]["error"]
