from datetime import datetime
import json
import re

from app.config.inspection_data_config import INSPECTION_ROUND_FIELDS
from app.modules.inspection_data.service import handover_inspection_inputs as inputs
from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore


def when(text):
    return datetime.fromisoformat("2026-11-" + text).replace(tzinfo=inputs.TZ)


def mode_fixture():
    return [
        ["制冷模式"],
        [None, None, None, "采集点", "2026-11-15 09:20:00", "2026-11-15 09:25:00"],
        [],
        [None, None, "控制器", "1号冷机模式", 3, "1.0"],
        [None, None, "控制器", "2#冷机模式", 2, 4],
        [None, None, "控制器", "3号冷机模式", 2, "未知"],
        [None, None, "控制器", "4号冷机模式", 4, 3],
        [None, None, "控制器", "5号冷机模式设定", 4, 4],
        [None, None, "控制器", "6号冷机运行模式", 4, 2],
    ]


def test_modes_keep_all_three_marks_and_distinguish_stop_and_unknown():
    start, end = when("15 09:20"), when("15 09:30")
    observations = inputs.mode_observations(mode_fixture(), start=start, end=end)
    for building, marks in (
        ("D楼", ["√", "/", "/", "/", "/", "√", "/", "√", "/"]),
        ("E楼", ["/", "√", "/", "√", "/", "/", "/", "/", "√"]),
    ):
        rows = inputs.mode_rows(building, observations, inputs.DEFAULT_VALUE_MAP, set())
        expected = marks[:3] + ["/"] * 3 + marks[3:]
        assert [row["测点值"] for row in rows] == expected
        assert {row["目标单元格"][0] for row in rows} == set("KLNP")
        assert {row["采样时间"] for row in rows} == {int(when("15 09:25").timestamp() * 1000)}
        assert all(row["目标工作表"] == inputs.SHEET and row["目标巡检表"] == inputs.WORKBOOKS[building]
                   and row["专业"] == "暖通" and row["记录类型"] == "测点明细"
                   and row["测点来源"] == inputs.MODE_SOURCE
                   and re.fullmatch(r"local_[0-9a-f]{32}", row["测点ID"])
                   and {row[field] for field in INSPECTION_ROUND_FIELDS.values()} == {row["目标单元格"]}
                   for row in rows)
        assert rows == inputs.mode_rows(building, observations, inputs.DEFAULT_VALUE_MAP, set())
    observations[(2, when("15 09:25"))].add("2")
    rows = inputs.mode_rows("D楼", observations, inputs.DEFAULT_VALUE_MAP, {"K8"})
    assert {row["目标单元格"][0] for row in rows} == set("NP")


def pressure_payload(value):
    return {"rows": [{
        "row_id": "west:1", "zone": "west", "unit": 1, "position": 2,
        "inlet_pressure": "0.30", "outlet_pressure": value,
    }], "primary_rows": [{
        "row_id": "primary:east:6", "zone": "east", "unit": 6, "position": 1,
        "inlet_pressure": "0.31", "outlet_pressure": "0.41",
    }], "secondary_rows": [{
        "row_id": "secondary:east:5", "zone": "east", "unit": 5, "position": 1,
        "inlet_pressure": "0.32", "outlet_pressure": "0.42",
    }]}


def test_only_explicit_exact_same_round_pressure_after_confirm(tmp_path, monkeypatch):
    building = "D楼"
    config = {"_global_paths": {"runtime_state_root": str(tmp_path)}}
    store = ReviewBuildingDocumentStore(config=config, building=building)
    session = {"building": building, "duty_date": "2026-11-15", "duty_shift": "day",
               "session_id": "d-day", "revision": 4, "output_file": str(tmp_path / "review.xlsx"),
               "confirmed": True}
    document = {"cooling_pump_pressures": pressure_payload("0 MPa")}
    baseline = pressure_payload("0.33")
    inputs.record_pressure_edits(
        session, {}, document, baseline,
        touched={
            "west:1/outlet_pressure": {"at": when("15 09:39").isoformat(), "value": "0 MPa"},
            "primary:east:6/inlet_pressure": {"at": when("15 09:29").isoformat(), "value": "0.31"},
            "secondary:east:5/inlet_pressure": {"at": when("15 09:39").isoformat(), "value": "0.32"},
            "secondary:east:5/outlet_pressure": {"at": when("15 09:39").isoformat(), "value": "0.43"},
            "west:2/inlet_pressure": {"at": when("15 09:39").isoformat(), "value": "0.3"},
        },
        now=when("15 09:40"),
    )
    assert set(document[inputs.PRESSURE_EDITS]["cells"]) == {"B25", "O24"}
    store.upsert_imported_document(session=session, document=document, imported_from_excel=False)
    revision = store.get_document(session["session_id"])["revision"]
    from handover_log_module.service.review_session_service import ReviewSessionService
    monkeypatch.setattr(ReviewSessionService, "get_latest_session_for_context_fast", lambda *_a, **_kw: session)
    assert inputs._confirmed_pressure_rows(config, building, when("15 10:02")) == []
    assert not inputs.record_pressure_confirmation(config, session, revision + 1, now=when("15 10:01"))
    assert inputs.record_pressure_confirmation(config, session, revision, now=when("15 10:01"))
    from app.modules.inspection_data.service.point_settings import read_inspection_upload_retrigger

    marker = read_inspection_upload_retrigger({"paths": {"runtime_state_root": str(tmp_path)}})
    assert marker["force_id"] and "D楼" in marker["reason"]
    public = inputs.public_review_document(store.get_document(session["session_id"])["document"])
    assert public == {"cooling_pump_pressures": pressure_payload("0 MPa")}
    output = inputs._confirmed_pressure_rows(config, building, when("15 10:02"))
    assert {row["目标单元格"]: row["测点值"] for row in output} == {"B25": "0", "O24": "0.32"}
    assert inputs._confirmed_pressure_rows(config, "E楼", when("15 10:02")) == []
    assert inputs._confirmed_pressure_rows(config, building, when("15 12:01")) == []
    session["revision"] = 5
    assert inputs._confirmed_pressure_rows(config, building, when("15 10:02")) == []


def test_night_two_belongs_to_previous_roster_date_and_stops_at_shift_end():
    assert inputs.pressure_round(when("15 00:00")) == ("2026-11-14", "night", 2)
    assert inputs.pressure_round(when("15 09:00")) is None
    assert inputs.pressure_round(when("15 19:59")) is None
    assert inputs.pressure_round(when("15 20:00")) == ("2026-11-15", "night", 1)


def test_source_index_upload_keeps_collected_zero_and_falls_back_only_for_blank(tmp_path, monkeypatch):
    source = tmp_path / "mode.json"
    source.write_text(json.dumps({
        "schema_version": 1, "building": "D楼", "source_family": inputs.MODE_FAMILY,
        "query_start": "2026-11-15 09:19:00", "query_end": "2026-11-15 09:30:00",
        "sheets": [{"name": "模式", "rows": mode_fixture()}],
    }), encoding="utf-8")

    class Bridge:
        def source_index_batch(self, queries):
            assert [query["building"] for query in queries] == ["D楼", "E楼"]
            return [{"entries": [{"relative_path": source.name, "building": "D楼",
                                   "source_family": inputs.MODE_FAMILY}]}, {"entries": []}]

    sample = when("15 09:31")
    pressures = [
        inputs._point_row("D楼", cell, inputs.PRESSURE_SOURCE, "1号冷却泵", "出水压力(MPa)",
                          "0.42", "0.42", sample, "审核")
        for cell in ("B25", "C25")
    ]
    monkeypatch.setattr(inputs, "_confirmed_pressure_rows", lambda _, building, __: pressures if building == "D楼" else [])
    target = {"目标巡检表": inputs.WORKBOOKS["D楼"], "目标工作表": inputs.SHEET, "专业": "暖通"}
    collected = [{**target, "目标单元格": "B25", "测点值": "0.00"},
                 {**target, "目标单元格": "C25", "测点值": ""}]
    window = {"query_start": "2026-11-15 09:15:00", "query_end": "2026-11-15 09:30:00"}
    result = inputs.handover_inspection_rows(
        {"shared_bridge": {"external_root_dir": str(tmp_path)}, "paths": {"runtime_state_root": str(tmp_path)}},
        Bridge(), {"D楼": window, "E楼": window}, {"D楼": collected, "E楼": []},
        lambda msg: None, now=when("15 09:40"),
    )
    assert result["E楼"] == []
    assert {row["目标单元格"] for row in result["D楼"] if row["测点来源"] == inputs.PRESSURE_SOURCE} == {"C25"}
    assert len([row for row in result["D楼"] if row["测点来源"] == inputs.MODE_SOURCE]) == 12


def test_chiller_operation_uses_configured_point_ids_and_its_building_row_order():
    from app.modules.inspection_data.service.chiller_operating_points import operation_rows

    rules = {"unit_modes": {"1": "1_1", "2": "1_2", "3": "1_3", "4": "1_4"},
             "secondary_frequencies": {"1": "2_1", "2": "2_2", "3": "2_3", "4": "2_4"}}
    values = {"1_1": "3", "1_2": "4", "1_3": "2", "1_4": "1",
              "2_1": "10", "2_2": "10.001", "2_3": "NaN", "2_4": "0"}
    for building in ("D楼", "E楼"):
        observed = [{"楼栋": building, "测点ID": key, "测点原始值": value} for key, value in values.items()]
        observed.append({"楼栋": "其他楼", "测点ID": "1_1", "测点原始值": "4"})
        rows = operation_rows(building, "2026-11-15 09:45:00", observed, rules,
                              {"pumps": {("冷却泵", "1"): "25", ("冷却泵", "2"): "0", ("冷冻一次泵", "2"): "0"}})
        marks = {row["目标单元格"]: row["测点值"] for row in rows}
        assert len(marks) == len(rows)
        assert [marks[f"K{line}"] for line in (7, 8, 9)] == (
            ["/", "/", "√"] if building == "D楼" else ["√", "/", "/"])
        assert marks["M8" if building == "D楼" else "M9"] == "√"
        assert marks["N7" if building == "D楼" else "N8"] == "√"
        assert "B4" not in marks and "B24" not in marks and "K10" not in marks
        assert [marks[cell] for cell in ("C4", "C6", "C17", "C21", "C26", "L4", "L5", "L7", "L15")] == [
            "△", "/", "/", "△", "△", "△", "√", "/", "/"]
        assert marks["K24"] == "/" and marks["K26"] == "△"
        assert "L24" not in marks and marks["L26"] == "√"
        assert marks["M24"] == "" and marks["M26"] == ""
        assert marks["N24"] == "/" and marks["N26"] == "△"
        assert marks["F4"] == "" and marks["O4"] == "" and marks["O5"] == "√"
        assert all(row[field] == row["目标单元格"] for row in rows for field in INSPECTION_ROUND_FIELDS.values())


def test_chiller_operating_rules_are_real_distinct_collection_points():
    import pytest

    from app.modules.inspection_data.service.chiller_operating_points import HVAC_FAMILY, normalize_operating_rules

    points = {"1_1", "1_2"}
    expected = {"unit_modes": {"6": "1_1"}, "secondary_frequencies": {"6": "1_2"}}
    assert normalize_operating_rules("D楼", HVAC_FAMILY, expected, points) == expected
    assert normalize_operating_rules("E楼", HVAC_FAMILY, {"unit_modes": {}}, points) == {}
    with pytest.raises(ValueError, match="本楼启用"):
        normalize_operating_rules("E楼", HVAC_FAMILY, expected, {"1_1"})
    with pytest.raises(ValueError, match="每个测点"):
        normalize_operating_rules("E楼", HVAC_FAMILY,
                                  {"unit_modes": {"1": "1_1"}, "secondary_frequencies": {"2": "1_1"}}, points)
    with pytest.raises(ValueError, match="暖通"):
        normalize_operating_rules("D楼", "electrical_inspection_family", expected, points)


def test_existing_capacity_reports_control_correct_pump_without_ids(tmp_path):
    from unittest.mock import Mock

    from app.modules.inspection_data.service.chiller_operating_points import CAPACITY_FAMILY, operation_rows, report_operating_controls

    sample = "2026-11-15 18:00:00"
    doc = {"value_time": sample, "query_start": sample, "query_end": sample}
    report_rows = {
        "D楼": [
            ["D楼150冷站", "一层D-150-BAS-102网络控制器", "2#冷冻泵频率反馈", "26.25"],
            ["", "", "2#冷却泵频率反馈", "0.00"],
            ["", "一层D-150-BAS-105网络控制器", "2#二次泵频率反馈(主)", "10.01"],
            ["", "一层D-124-BAS-105网络控制器", "2#二次泵频率反馈(主)", "0"],  # wrong controller
        ],
        "E楼": [
            ["E楼124DDC", "E-124-DDC-101", "E-124-DDC-101_一次冷冻泵频率反馈", "27.00"],
            ["", "", "E-124-DDC-101_冷却泵频率反馈", "10.00"],
            ["", "E-150-DDC-105", "E-150-DDC-105_冷冻水二次泵频率反馈SCHWP-1", "30.01"],
            ["", "", "E-150-DDC-105_冷冻水二次泵频率反馈SCHWP-4", "99"],  # wrong zone
        ],
    }
    index = {}
    for building, raw_rows in report_rows.items():
        filename = building + ".json"
        rows = [[], ["", "", "", "", sample], [], *[["", *row] for row in raw_rows]]
        (tmp_path / filename).write_text(json.dumps({
            "schema_version": 1, "source_family": CAPACITY_FAMILY, "building": building,
            "query_start": sample, "query_end": sample, "sheets": [{"name": "sheet1", "rows": rows}],
        }, ensure_ascii=False), encoding="utf-8")
        index[building] = {"building": building, "source_family": CAPACITY_FAMILY, "relative_path": filename}

    bridge = Mock()
    bridge.source_index_batch.side_effect = lambda queries: [
        {"entries": [index[q["building"]]] if q["source_family"] == CAPACITY_FAMILY else []} for q in queries]
    controls = report_operating_controls(
        bridge, {"shared_bridge": {"root_dir": str(tmp_path)}}, dict.fromkeys(report_rows, doc), lambda _: None)
    assert controls["D楼"]["pumps"] == {
        ("冷冻一次泵", "2"): "26.25", ("冷却泵", "2"): "0.00", ("冷冻二次泵", "2"): "10.01"}
    assert controls["E楼"]["pumps"] == {
        ("冷冻一次泵", "4"): "27.00", ("冷却泵", "4"): "10.00", ("冷冻二次泵", "1"): "30.01"}
    marks = {row["目标单元格"]: row["测点值"]
             for row in operation_rows("E楼", sample, [], {}, controls["E楼"])}
    assert marks["E21"] == "√" and "E17" not in marks
    assert marks["E26"] == "△" and marks["E24"] == "/"
    assert marks["K26"] == "√" and marks["N26"] == ""
