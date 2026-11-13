"""H楼审核页“业务分类”后端回归：模板解析 schema、单元格集合、规范化、交付映射、KV 存取。

附件（xlsx）交付时各分类（含培训管理）按记录数在主表**逐行渲染**（新增一条即新增一行，
超出模板行则插行、少于则删行），页脚与打印区自动顺延，不再写“分类续表”第二张表；
未保存的分类保留模板默认（``/`` 占位、注意事项常备文本）。“三、注意事项交接”执行人
为空时自动填当班人员第一个。云表（在线表格）兼容路径仍按模板固定行写扁平单元格。
"""
from __future__ import annotations

import pytest

pytest.importorskip("openpyxl")

from app.core.app_state import AppStateRepository
from handover_log_module.service.station_h_review_selection_service import (
    StationHReviewSelectionService, StationHSectionsConflictError,
)
from handover_log_module.service.station_h_section_schema import (
    build_station_h_section_cell_values,
    normalize_station_h_sections,
    station_h_section_cells,
    station_h_section_schema,
)

EXPECTED_SECTIONS = [
    "新事件处理", "演练管理", "维护管理", "园区施工管理",
    "客户服务", "其他重要工作记录", "三、注意事项交接",
]


def test_schema_excludes_training_and_matches_template():
    schema = station_h_section_schema()
    names = [item["name"] for item in schema]
    assert names == EXPECTED_SECTIONS
    assert "培训管理" not in names  # 培训由 training_rows 单独处理，避免与其在 B31..G31 冲突
    by_name = {item["name"]: item for item in schema}
    event = by_name["新事件处理"]
    assert event["rows"] == [19]
    assert [column["key"] for column in event["columns"]] == ["B", "C", "D", "F", "H"]
    attention = by_name["三、注意事项交接"]
    assert attention["rows"] == [40, 41, 42]
    assert [column["key"] for column in attention["columns"]] == ["B", "H"]


def test_section_cells_cover_data_rows_and_exclude_training():
    cells = station_h_section_cells()
    for expected in ("B19", "C19", "D19", "F19", "H19", "B28", "E28", "B34", "F37", "B40", "B42", "H42"):
        assert expected in cells, expected
    # 培训行不得混入分类单元格集合
    assert "B31" not in cells and "G31" not in cells


def test_normalize_retains_added_rows_and_drops_unknown_columns():
    normalized = normalize_station_h_sections({
        "新事件处理": [{"cells": {"B": "一级", "H": 42, "ZZZ": "x"}}, {"cells": {"B": "越界行"}}],
        "不存在的分类": [{"cells": {"B": "x"}}],
    })
    # 总是按 schema 产出全部 7 个分类；schema 外分类忽略
    assert set(normalized) == set(EXPECTED_SECTIONS)
    event = normalized["新事件处理"]
    assert len(event) == 2
    assert event[0]["cells"] == {"B": "一级", "C": "", "D": "", "F": "", "H": "42"}  # 非法列丢弃、缺列补空、值转字符串
    assert event[1]["cells"]["B"] == "越界行"
    assert normalized["三、注意事项交接"] == [
        {"cells": {"B": "", "H": ""}}, {"cells": {"B": "", "H": ""}}, {"cells": {"B": "", "H": ""}},
    ]


def test_build_cell_values_only_non_empty_at_absolute_positions():
    cells = build_station_h_section_cell_values({
        "新事件处理": [{"cells": {"B": "一级", "C": "10:00", "D": "", "F": "处理中", "H": "张三"}}],
        "三、注意事项交接": [{"cells": {"B": "", "H": "李四"}}, {"cells": {"B": "", "H": ""}}, {"cells": {"B": "", "H": ""}}],
        "园区施工管理": [{"cells": {"B": "施工方A"}}],
    })
    assert cells == {
        "B19": "一级", "C19": "10:00", "F19": "处理中", "H19": "张三",
        "H40": "李四",
        "B28": "施工方A",
    }
    # 空值不写：保留模板默认（/ 占位、注意事项常备文本）
    assert "D19" not in cells and "B40" not in cells and "B41" not in cells and "H41" not in cells


def test_sections_kv_roundtrip_and_people_only_save_preserves(tmp_path):
    repository = AppStateRepository(app_dir=tmp_path)
    repository.ensure_ready()
    service = StationHReviewSelectionService({}, app_state_repository=repository)
    sections = {"新事件处理": [{"cells": {"B": "一级", "C": "10:00", "D": "描述", "F": "进展", "H": "张三"}}]}
    saved = service.save_selection(
        duty_date="2026-11-10", duty_shift="day",
        current_people="甲", next_people="乙", long_day_people=[], sections=sections,
    )
    assert saved["sections"]["新事件处理"][0]["cells"]["B"] == "一级"
    # 重新打开读到内容
    reopened = StationHReviewSelectionService({}, app_state_repository=repository).get_selection("2026-11-10", "day")
    assert reopened["sections"]["新事件处理"][0]["cells"] == {
        "B": "一级", "C": "10:00", "D": "描述", "F": "进展", "H": "张三",
    }
    # 仅改人员的保存（sections=None）不得清掉已存分类
    people_only = service.save_selection(
        duty_date="2026-11-10", duty_shift="day", current_people="甲", next_people="丙", long_day_people=[],
    )
    assert people_only["sections"]["新事件处理"][0]["cells"]["B"] == "一级"


def test_append_training_only_preserves_sections(tmp_path):
    repository = AppStateRepository(app_dir=tmp_path)
    repository.ensure_ready()
    service = StationHReviewSelectionService({}, app_state_repository=repository)
    sections = {"客户服务": [{"cells": {"B": "客户X", "F": "已完成", "H": "王五"}}]}
    service.save_selection(
        duty_date="2026-11-10", duty_shift="night",
        current_people="甲", next_people="乙", long_day_people=[], sections=sections,
    )
    # 交付期自动追加培训（append_training_only）不得丢失分类
    result = service.save_selection(
        duty_date="2026-11-10", duty_shift="night",
        current_people="甲", next_people="乙", long_day_people=[],
        training_rows=[{"row_id": "training:1", "cells": {"B": "培训"}}], append_training_only=True,
    )
    assert result["sections"]["客户服务"][0]["cells"]["B"] == "客户X"
    assert result["training_rows"][0]["cells"]["B"] == "培训"


def test_sections_revision_rejects_stale_editor_and_preserves_latest(tmp_path):
    repository = AppStateRepository(app_dir=tmp_path)
    repository.ensure_ready()
    service = StationHReviewSelectionService({}, app_state_repository=repository)
    args = dict(duty_date="2026-11-10", duty_shift="day", current_people="甲", next_people="乙", long_day_people=[])
    first = service.save_selection(**args, sections={"新事件处理": [{"cells": {"D": "已保存"}}]}, base_sections_revision=0)
    assert first["sections_revision"] == 1
    with pytest.raises(StationHSectionsConflictError):
        service.save_selection(**args, sections={"演练管理": [{"cells": {"C": "旧页面编辑"}}]}, base_sections_revision=0)
    saved = service.save_selection(**args)
    assert saved["sections_revision"] == 1
    assert saved["sections"]["新事件处理"][0]["cells"]["D"] == "已保存"
    cleared = service.save_selection(**args, sections={}, base_sections_revision=1)
    assert cleared["sections_revision"] == 2
    assert cleared["sections"]["新事件处理"][0]["cells"]["D"] == ""


def test_previous_shift_attention_inherits_but_deleted_rows_stay_deleted(tmp_path):
    repository = AppStateRepository(app_dir=tmp_path)
    repository.ensure_ready()
    service = StationHReviewSelectionService({}, app_state_repository=repository)
    people = dict(current_people="甲", next_people="乙", long_day_people=[])
    attention = "三、注意事项交接"
    rows = [{"cells": {"B": "上一班注意事项", "H": "甲"}}]
    service.save_selection(duty_date="2026-11-09", duty_shift="night", sections={attention: rows}, **people)
    context = dict(duty_date="2026-11-10", duty_shift="day")
    assert service.resolve_selection(**context)["sections"][attention] == rows
    service.save_selection(**context, sections={attention: []}, base_sections_revision=0, **people)
    cleared = service.resolve_selection(**context)
    assert cleared["sections"][attention] == []
    assert build_station_h_section_cell_values(
        cleared["sections"], clear_attention_defaults=cleared["attention_handover_saved"],
    ) == {"B40": "", "H40": "", "B41": "", "H41": "", "B42": "", "H42": ""}
    assert service.resolve_selection(duty_date="2026-11-10", duty_shift="night")["sections"][attention] == []


def test_deleted_training_row_is_not_reinserted_on_save(tmp_path):
    repository = AppStateRepository(app_dir=tmp_path)
    repository.ensure_ready()
    service = StationHReviewSelectionService({}, app_state_repository=repository)
    args = dict(duty_date="2026-11-10", duty_shift="day", current_people="甲", next_people="乙", long_day_people=[])
    row = {"row_id": "training:1", "cells": {"B": "培训"}}
    service.save_selection(**args, training_rows=[row])
    saved = service.save_selection(**args, training_rows=[], deleted_training_row_ids=["training:1"])
    assert saved["training_rows"] == []
    reopened = service.get_selection(args["duty_date"], args["duty_shift"])
    assert reopened["deleted_training_row_ids"] == ["training:1"]
    assert service.save_selection(**args, training_rows=[row], append_training_only=True)["training_rows"] == []


def test_saving_unedited_training_preview_does_not_overwrite_newer_saved_edit(tmp_path):
    repository = AppStateRepository(app_dir=tmp_path)
    repository.ensure_ready()
    service = StationHReviewSelectionService({}, app_state_repository=repository)
    args = dict(duty_date="2026-11-10", duty_shift="day", current_people="甲", next_people="乙", long_day_people=[])
    service.save_selection(**args, training_rows=[{"row_id": "training:1", "cells": {"B": "已编辑"}}])
    saved = service.save_selection(**args, training_rows=[
        {"row_id": "training:1", "cells": {"B": "旧的预览"}},
        {"row_id": "training:2", "cells": {"B": "新培训"}},
    ], unedited_training_row_ids=["training:1", "training:2"])
    assert [row["cells"]["B"] for row in saved["training_rows"]] == ["已编辑", "新培训"]


def test_section_cells_land_in_generated_workbook(tmp_path):
    """端到端：分类记录经 build_station_h_workbook 在主表**逐行**写入；每条记录各占一行，未填分类保留模板默认。"""
    import openpyxl
    from handover_log_module.service.handover_attachment_workbook_builder import (
        build_station_h_workbook, default_station_h_template_path,
    )
    from handover_log_module.service.station_h_section_schema import build_station_h_section_rows

    template = default_station_h_template_path()
    if not template.is_file():
        pytest.skip("H楼模板不存在")
    sections = {
        "新事件处理": [
            {"cells": {"B": "一级", "C": "10:00", "D": "=1+1", "F": "已恢复", "H": "张三"}},
            {"cells": {"B": "二级", "D": "=" + "需跟进长描述" * 36, "H": "王五"}},
        ],
        "园区施工管理": [{"cells": {"B": "施工方A", "F": "进行中", "H": "李四"}}],
    }
    section_rows = build_station_h_section_rows(sections, [])
    out = tmp_path / "H_handover.xlsx"
    build_station_h_workbook(
        template, out, {"B2": "2026年11月10日", "F2": "白班"},
        section_rows=section_rows, attention_executor="赵六",
    )
    workbook = openpyxl.load_workbook(str(out), data_only=False)
    try:
        ws = workbook["H楼"] if "H楼" in workbook.sheetnames else workbook.worksheets[0]
        # 新事件处理 2 条记录 → 主表 2 行（19、20），每条各占一行，不再挤一行/不再进续表
        assert ws["A19"].value == 1
        assert ws["B19"].value == "一级"
        assert ws["C19"].value == "10:00"
        assert ws["D19"].value == "=1+1"      # D:E 合并，写锚点 D19
        assert ws["D19"].data_type == "s"
        assert ws["F19"].value == "已恢复"    # F:G 合并，写锚点 F19
        assert ws["H19"].value == "张三"
        assert ws["A20"].value == 2           # 第 2 条记录 = 主表新增一行
        assert ws["B20"].value == "二级"
        assert ws["H20"].value == "王五"
        assert ws["D20"].alignment.wrap_text
        # 园区施工管理因上方新事件插了 1 行，整体顺延到行 29
        assert ws["B29"].value == "施工方A"
        assert ws["H29"].value == "李四"
        # 未填写的注意事项分类保留模板常备文本，执行人自动填当班第一个（赵六），整体顺延 +1 到行 41-43
        assert ws["B41"].value not in (None, "")
        assert ws["H41"].value == "赵六"
        assert ws["H42"].value == "赵六"
        assert ws["H43"].value == "赵六"
        # 彻底去掉「分类续表」第二张工作表
        assert "分类续表" not in workbook.sheetnames
        # 打印区域随插行动态覆盖到页脚（模板 50 行 + 净增 1 行）
        assert str(ws.print_area).endswith("$H$51")
    finally:
        workbook.close()


def test_training_records_render_one_row_each(tmp_path):
    """培训管理多条记录在主表逐行渲染（行31起），不再用换行挤进单个单元格。"""
    import openpyxl
    from handover_log_module.service.handover_attachment_workbook_builder import (
        build_station_h_workbook, default_station_h_template_path,
    )
    from handover_log_module.service.station_h_section_schema import build_station_h_section_rows

    template = default_station_h_template_path()
    if not template.is_file():
        pytest.skip("H楼模板不存在")
    training_rows = [
        {"row_id": "training:1", "cells": {"B": "消防培训", "D": "14:00", "E": "实操", "F": "王五", "G": "全员"}},
        {"row_id": "training:2", "cells": {"B": "安全培训", "D": "15:00", "E": "理论", "F": "赵六", "G": "A组"}},
    ]
    section_rows = build_station_h_section_rows({}, training_rows)
    out = tmp_path / "H_training.xlsx"
    build_station_h_workbook(template, out, {"B2": "2026年11月10日"}, section_rows=section_rows)
    workbook = openpyxl.load_workbook(str(out), data_only=False)
    try:
        ws = workbook["H楼"] if "H楼" in workbook.sheetnames else workbook.worksheets[0]
        # 培训管理模板数据行=31；2 条记录 → 行 31、32（插 1 行）
        assert ws["A31"].value == 1
        assert ws["B31"].value == "消防培训"
        assert ws["G31"].value == "全员"
        assert ws["A32"].value == 2
        assert ws["B32"].value == "安全培训"
        assert ws["F32"].value == "赵六"
        # 单元格内不含换行拼接（不再挤一行）
        assert "\n" not in str(ws["B31"].value or "")
        assert "分类续表" not in workbook.sheetnames
    finally:
        workbook.close()


def test_build_station_h_handover_log_workbook_renders_sections(tmp_path, monkeypatch):
    """打印链路：build_station_h_handover_log_workbook 用当前保存态生成逐行渲染、带打印预设的工作簿。"""
    import openpyxl
    from types import SimpleNamespace
    from handover_log_module.service import review_followup_trigger_service as followup_module
    from handover_log_module.service.handover_attachment_workbook_builder import default_station_h_template_path

    if not default_station_h_template_path().is_file():
        pytest.skip("H楼模板不存在")

    service = object.__new__(followup_module.ReviewFollowupTriggerService)
    service._review_service = SimpleNamespace(build_batch_key=lambda d, s: f"{d}|{s}")
    monkeypatch.setattr(service, "_list_batch_sessions_resilient", lambda batch_key, emit_log: [], raising=False)
    cell_result = {
        "ok": True,
        "cells": {"B2": "2026年11月12日", "F2": "白班", "C3": "值班甲", "H45": "接班甲", "H46": "接班甲",
                  "H47": "接班甲", "H48": "接班甲"},
        "section_rows": {
            "新事件处理": [
                {"B": "一级", "C": "10:00", "D": "跳闸", "F": "已处理", "H": "值班甲"},
                {"B": "二级", "C": "11:00", "D": "告警", "F": "处理中", "H": "值班乙"},
            ],
            "培训管理": [{"B": "消防培训", "D": "14:00", "E": "实操", "F": "王五", "G": "全员"}],
        },
        "attention_executor": "值班甲",
    }
    monkeypatch.setattr(service, "_build_station_h_cell_values", lambda **_kw: cell_result, raising=False)
    service._cloud_sheet_sync_service = SimpleNamespace()  # 非附件交付服务 → 用默认模板路径

    out = tmp_path / "H_print.xlsx"
    result = service.build_station_h_handover_log_workbook(
        duty_date="2026-11-12", duty_shift="day", output_path=out, emit_log=lambda _m: None,
    )
    assert result == out and out.is_file()
    workbook = openpyxl.load_workbook(str(out), data_only=False)
    try:
        ws = workbook["H楼"]
        # 头部固定区不受插行影响
        assert ws["B2"].value == "2026年11月12日"
        assert ws["C3"].value == "值班甲"
        # 新事件 2 条 → 行 19、20
        assert ws["D19"].value == "跳闸"
        assert ws["D20"].value == "告警"
        # 培训 1 条，因新事件插 1 行顺延到行 32
        assert ws["B32"].value == "消防培训"
        # 注意事项未保存：保留模板默认文本，执行人自动填当班第一个（值班甲），顺延 +1 到行 41
        assert ws["B41"].value not in (None, "")
        assert ws["H41"].value == "值班甲"
        # 页脚「清点确认人（接班）」顺延 +1 到行 46，仍为接班甲
        assert ws["H46"].value == "接班甲"
        # 打印预设烘焙进工作簿，打印区随插行覆盖到页脚
        assert ws.page_setup.orientation == "portrait"
        assert ws.sheet_properties.pageSetUpPr.fitToPage is True
        assert ws.page_setup.fitToWidth == 1 and ws.page_setup.fitToHeight == 1
        assert str(ws.print_area).endswith("$H$51")
        assert "分类续表" not in workbook.sheetnames
    finally:
        workbook.close()
