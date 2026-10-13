from handover_log_module.service.power_alert_sync_service import PowerAlertSyncService, _PowerAlertTable


def test_daily_row_line_payload_uses_235_for_e_and_215_for_other_buildings(tmp_path):
    service = PowerAlertSyncService({"paths": {"runtime_state_root": str(tmp_path)}})
    rows = []
    for building, column, powers in (
        ("E楼", "A", [234.999] * 24),
        ("E楼", "B", [215, 234.999, 235, 236] + [0] * 20),
        ("E楼", "C", [235] + [0] * 23),
        ("A楼", "A", [215] + [0] * 23),
        ("D楼", "A", [215] + [0] * 23),
    ):
        fields = {
            "机楼": building, "包间": f"{building[0]}-202包间",
            "机列": f"{building[0]}-202-{column}列A路-DC001",
            "支路编号": f"{column}01-A1", "PDU编号": "1",
            **{f"功率-{hour}:00": value for hour, value in enumerate(powers)},
        }
        row = service._normalize_source_row(fields)
        assert row is not None
        rows.append(row)
    result = service._generate_all_targets(
        rows,
        target_tables=[_PowerAlertTable("row_line", "机列", "tblCcFzmKKz50tYT", "", 215)],
        report_date="2026/10/13", data_center_name="EA118", emit_log=lambda _text: None,
    )["row_line"]
    assert {(item["楼栋"], item["机列"]) for item in result} == {("E楼", "B列"), ("E楼", "C列"), ("A楼", "A列"), ("D楼", "A列")}
    e_record = next(item for item in result if item["楼栋"] == "E楼" and item["机列"] == "B列")
    assert e_record["功率"] == "236KW"
    assert e_record["时长"] == "2h"
    boundary = next(item for item in result if item["楼栋"] == "E楼" and item["机列"] == "C列")
    assert boundary["功率"] == "235KW" and boundary["时长"] == "1h"


def test_same_room_label_does_not_merge_e_and_other_buildings(tmp_path):
    service = PowerAlertSyncService({"paths": {"runtime_state_root": str(tmp_path)}})
    rows = [service._normalize_source_row({
        "机楼": building, "包间": "202包间", "机列": f"{building[0]}-202-A列A路-DC001",
        "支路编号": "A01-A1", "PDU编号": "1", "功率-0:00": value,
    }) for building, value in (("A楼", 215), ("E楼", 234.999))]
    result = service._generate_row_line_rows(
        rows, threshold=215, report_date="2026/10/13", data_center_name="EA118", emit_log=lambda _text: None,
    )
    assert len(result) == 1
    assert result[0]["楼栋"] == "A楼" and result[0]["功率"] == "215KW"
