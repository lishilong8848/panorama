from __future__ import annotations

from pathlib import Path

import openpyxl

from handover_log_module.service.handover_summary_message_service import HandoverSummaryMessageService


def _write_handover_file(path: Path) -> None:
    workbook = openpyxl.Workbook()
    ws = workbook.active
    ws.title = "交接班日志"
    ws["A1"] = "EA118机房A栋数据中心交接班日志"
    ws["C3"] = "张三 李四"
    ws["G3"] = "王五 赵六"
    ws["B6"] = "1.45"
    ws["D6"] = "2432"
    ws["F6"] = "1674.72"
    ws["B13"] = "1272000"
    ws["D13"] = "330000"
    workbook.save(path)
    workbook.close()


def test_handover_summary_message_uses_capacity_cooling_summary(tmp_path: Path, monkeypatch) -> None:
    output_file = tmp_path / "handover.xlsx"
    _write_handover_file(output_file)
    service = HandoverSummaryMessageService({"template": {"sheet_name": "交接班日志"}})
    monkeypatch.setattr(service, "_lookup_contact_phones", lambda names, *, emit_log: {})

    message = service.build_for_session(
        {
            "building": "A楼",
            "duty_date": "2026-04-29",
            "duty_shift": "day",
            "output_file": str(output_file),
            "capacity_cooling_summary": {
                "lines": {
                    "west": "冷冻站A区3套制冷单元2用1备，2#制冷单元板换模式运行正常；",
                    "east": "冷冻站B区3套制冷单元2用1备，4#制冷单元板换模式运行正常；",
                }
            },
        },
        emit_log=lambda _msg: None,
    )

    assert "2、冷冻站A区3套制冷单元2用1备，2#制冷单元板换模式运行正常；" in message
    assert "3、冷冻站B区3套制冷单元2用1备，4#制冷单元板换模式运行正常；" in message
    assert "制冷单元按当前运行方式运行" not in message
