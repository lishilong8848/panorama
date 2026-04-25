from __future__ import annotations

from copy import copy
from pathlib import Path

import pytest
from openpyxl import Workbook
from PIL import Image

from handover_log_module.service import capacity_report_image_delivery_service as module


def _write_capacity_workbook(path: Path) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "本班组"
    sheet.print_area = "A1:D4"
    sheet.merge_cells("A1:D1")
    sheet["A1"] = "A楼交接班容量报表"
    font = copy(sheet["A1"].font)
    font.bold = True
    font.sz = 14
    sheet["A1"].font = font
    sheet["A2"] = "项目"
    sheet["B2"] = "数值"
    sheet["A3"] = "机柜"
    sheet["B3"] = 12
    sheet["C3"] = "备注"
    sheet["D3"] = "正常"
    workbook.save(path)
    workbook.close()


def test_capacity_report_renderer_outputs_valid_png(tmp_path: Path) -> None:
    source = tmp_path / "capacity.xlsx"
    _write_capacity_workbook(source)
    renderer = module.CapacityReportImageRenderer(
        {"_global_paths": {"runtime_state_root": str(tmp_path / "runtime")}, "capacity_report": {"template": {"sheet_name": "本班组"}}}
    )

    image_path = renderer.render_to_image(
        source_file=str(source),
        building="A楼",
        duty_date="2026-04-24",
        duty_shift="day",
        session_id="A楼|2026-04-24|day",
    )

    assert image_path.exists()
    with Image.open(image_path) as image:
        assert image.format == "PNG"
        assert image.width > 100
        assert image.height > 60


def test_capacity_image_delivery_uploads_once_and_sends_to_all_recipients(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "capacity.xlsx"
    _write_capacity_workbook(source)
    sent = []
    uploaded = []
    updated_states = []

    class _FakeClient:
        def upload_image(self, image_path: str):
            uploaded.append(image_path)
            return {"image_key": "img-key"}

        def send_image_message(self, *, receive_id: str, receive_id_type: str, image_key: str):
            sent.append({"receive_id": receive_id, "receive_id_type": receive_id_type, "image_key": image_key})
            return {"message_id": f"msg-{len(sent)}"}

    class _FakeLinkService:
        def __init__(self, *_args, **_kwargs):
            pass

        def _recipients_for_building(self, _building):
            return [{"open_id": "ou_1", "note": "甲"}, {"open_id": "ou_2", "note": "乙"}]

        def _build_feishu_client(self):
            return _FakeClient()

        @staticmethod
        def _resolve_effective_receive_id_type(_recipient_id, _configured_receive_id_type="open_id"):
            return "open_id"

    class _FakeReviewSessionService:
        def __init__(self, *_args, **_kwargs):
            pass

        def update_capacity_image_delivery(self, *, session_id: str, capacity_image_delivery):
            updated_states.append({"session_id": session_id, "delivery": dict(capacity_image_delivery)})
            return {"session_id": session_id, "capacity_image_delivery": dict(capacity_image_delivery)}

    monkeypatch.setattr(module, "ReviewLinkDeliveryService", _FakeLinkService)
    monkeypatch.setattr(module, "ReviewSessionService", _FakeReviewSessionService)
    service = module.CapacityReportImageDeliveryService(
        {"_global_paths": {"runtime_state_root": str(tmp_path / "runtime")}, "capacity_report": {"template": {"sheet_name": "本班组"}}}
    )

    result = service.send_for_session(
        {
            "session_id": "session-a",
            "building": "A楼",
            "duty_date": "2026-04-24",
            "duty_shift": "day",
            "capacity_output_file": str(source),
            "capacity_sync": {"status": "ready"},
        },
        building="A楼",
        emit_log=lambda _line: None,
    )

    assert result["status"] == "success"
    assert len(uploaded) == 1
    assert Path(uploaded[0]).exists()
    assert sent == [
        {"receive_id": "ou_1", "receive_id_type": "open_id", "image_key": "img-key"},
        {"receive_id": "ou_2", "receive_id_type": "open_id", "image_key": "img-key"},
    ]
    assert updated_states[0]["delivery"]["status"] == "sending"
    assert updated_states[-1]["delivery"]["status"] == "success"


def test_capacity_image_delivery_rejects_duplicate_running_session(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "capacity.xlsx"
    _write_capacity_workbook(source)

    class _FakeLinkService:
        def __init__(self, *_args, **_kwargs):
            pass

        def _recipients_for_building(self, _building):
            return [{"open_id": "ou_1"}]

    class _FakeReviewSessionService:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_session_by_id(self, _session_id: str):
            return {
                "session_id": "session-a",
                "building": "A楼",
                "capacity_output_file": str(source),
                "capacity_sync": {"status": "ready"},
                "capacity_image_delivery": {"status": "sending"},
            }

    monkeypatch.setattr(module, "ReviewLinkDeliveryService", _FakeLinkService)
    monkeypatch.setattr(module, "ReviewSessionService", _FakeReviewSessionService)
    service = module.CapacityReportImageDeliveryService({"_global_paths": {"runtime_state_root": str(tmp_path / "runtime")}})

    with pytest.raises(ValueError, match="正在发送中"):
        service.begin_delivery(
            {
                "session_id": "session-a",
                "building": "A楼",
                "capacity_output_file": str(source),
                "capacity_sync": {"status": "ready"},
            },
            building="A楼",
        )


def test_capacity_image_delivery_marks_failed_when_upload_fails(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "capacity.xlsx"
    _write_capacity_workbook(source)
    updated_states = []

    class _FakeClient:
        def upload_image(self, _image_path: str):
            raise RuntimeError("upload failed")

    class _FakeLinkService:
        def __init__(self, *_args, **_kwargs):
            pass

        def _recipients_for_building(self, _building):
            return [{"open_id": "ou_1"}]

        def _build_feishu_client(self):
            return _FakeClient()

    class _FakeReviewSessionService:
        def __init__(self, *_args, **_kwargs):
            pass

        def update_capacity_image_delivery(self, *, session_id: str, capacity_image_delivery):
            updated_states.append({"session_id": session_id, "delivery": dict(capacity_image_delivery)})
            return {"session_id": session_id, "capacity_image_delivery": dict(capacity_image_delivery)}

    monkeypatch.setattr(module, "ReviewLinkDeliveryService", _FakeLinkService)
    monkeypatch.setattr(module, "ReviewSessionService", _FakeReviewSessionService)
    service = module.CapacityReportImageDeliveryService(
        {"_global_paths": {"runtime_state_root": str(tmp_path / "runtime")}, "capacity_report": {"template": {"sheet_name": "本班组"}}}
    )

    with pytest.raises(RuntimeError, match="upload failed"):
        service.send_for_session(
            {
                "session_id": "session-a",
                "building": "A楼",
                "duty_date": "2026-04-24",
                "duty_shift": "day",
                "capacity_output_file": str(source),
                "capacity_sync": {"status": "ready"},
            },
            building="A楼",
            emit_log=lambda _line: None,
        )

    assert updated_states[0]["delivery"]["status"] == "sending"
    assert updated_states[-1]["delivery"]["status"] == "failed"
    assert "upload failed" in updated_states[-1]["delivery"]["error"]
