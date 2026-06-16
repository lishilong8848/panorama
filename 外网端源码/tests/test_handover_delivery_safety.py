from __future__ import annotations

import pytest

pytest.importorskip("openpyxl")
pytest.importorskip("requests")

from PIL import Image

from handover_log_module.service.capacity_report_image_delivery_service import CapacityReportImageDeliveryService
from handover_log_module.service.handover_summary_message_service import HandoverSummaryMessageService


class _ReviewService:
    def __init__(self) -> None:
        self.capacity_updates: list[dict] = []
        self.review_updates: list[dict] = []

    def update_capacity_image_delivery(self, *, session_id: str, capacity_image_delivery: dict) -> dict:
        self.capacity_updates.append({"session_id": session_id, "payload": dict(capacity_image_delivery)})
        return {"capacity_image_delivery": dict(capacity_image_delivery)}

    def update_review_link_delivery(self, *, session_id: str, review_link_delivery: dict) -> dict:
        self.review_updates.append({"session_id": session_id, "payload": dict(review_link_delivery)})
        raise AssertionError("capacity image delivery must not update review_link_delivery")


class _LinkClient:
    def __init__(self) -> None:
        self.text_messages: list[dict] = []
        self.image_messages: list[dict] = []

    def upload_image(self, image_path: str) -> dict:
        return {"image_key": f"image::{image_path}"}

    def send_text_message(self, *, receive_id: str, receive_id_type: str, text: str) -> None:
        self.text_messages.append({"receive_id": receive_id, "receive_id_type": receive_id_type, "text": text})

    def send_image_message(self, *, receive_id: str, receive_id_type: str, image_key: str) -> None:
        self.image_messages.append({"receive_id": receive_id, "receive_id_type": receive_id_type, "image_key": image_key})


class _LinkService:
    def __init__(self, client: _LinkClient | None = None) -> None:
        self.client = client or _LinkClient()

    def _recipient_snapshot_for_building(self, _building: str) -> dict:
        return {
            "recipients": [{"open_id": "ou_test", "note": "测试"}],
            "raw_count": 1,
            "enabled_count": 1,
            "disabled_count": 0,
            "invalid_count": 0,
            "open_ids": ["ou_test"],
        }

    def _resolve_effective_receive_id_type(self, _recipient_id: str, configured_receive_id_type: str = "open_id") -> str:
        return configured_receive_id_type

    def _build_feishu_client(self) -> _LinkClient:
        return self.client


class _CapacityService:
    def build_capacity_overlay_signature(self, **_kwargs) -> dict:
        return {"signature": "signature", "input_signature": "input", "valid": True}


class _SummaryService:
    def __init__(self, text: str) -> None:
        self.text = text

    def build_for_session(self, _session: dict, *, emit_log=print) -> str:
        return self.text


def _session(tmp_path, *, output_file: str = "handover.xlsx") -> dict:
    capacity = tmp_path / "capacity.xlsx"
    capacity.write_bytes(b"not-a-real-workbook-for-this-unit-test")
    output = tmp_path / output_file
    output.write_bytes(b"not-a-real-workbook-for-this-unit-test")
    return {
        "session_id": "A楼|2026-06-16|day",
        "building": "A楼",
        "duty_date": "2026-06-16",
        "duty_shift": "day",
        "capacity_output_file": str(capacity),
        "output_file": str(output),
        "review_link_delivery": {"status": "success", "source": "review_link", "successful_recipients": ["ou_old"]},
        "capacity_image_delivery": {},
        "capacity_sync": {"status": "ready", "input_signature": "input"},
    }


def test_summary_message_skips_blank_people(monkeypatch, tmp_path):
    service = HandoverSummaryMessageService({})
    output_path = tmp_path / "handover.xlsx"
    output_path.write_bytes(b"fake")
    monkeypatch.setattr(
        service,
        "_read_output_context",
        lambda _path: {
            "current_people": "",
            "next_people": "",
            "title": "E楼世纪互联 白班",
        },
    )

    logs: list[str] = []
    text = service.build_for_session(
        {
            "building": "E楼",
            "duty_date": "2026-06-16",
            "duty_shift": "day",
            "output_file": str(output_path),
            "session_id": "E楼|2026-06-16|day",
        },
        emit_log=logs.append,
    )

    assert text == ""
    assert any("人员信息不完整" in item for item in logs)


def test_capacity_image_send_rejects_handover_text_containing_review_link(tmp_path):
    review_service = _ReviewService()
    link_client = _LinkClient()
    service = CapacityReportImageDeliveryService(
        {},
        review_service=review_service,
        link_service=_LinkService(link_client),
        capacity_service=_CapacityService(),
        summary_service=_SummaryService("交接班内容\n审核链接：http://127.0.0.1/handover/review/a"),
    )

    session = _session(tmp_path)
    result = service.send_for_session(session, ensure_capacity_ready=lambda: session, emit_log=lambda _text: None)

    assert result["ok"] is False
    assert "审核页链接" in result["error"]
    assert review_service.review_updates == []
    assert link_client.text_messages == []
    assert link_client.image_messages == []
    assert result["review_link_delivery"]["source"] == "review_link"


def test_capacity_image_send_success_does_not_mutate_review_link_delivery(monkeypatch, tmp_path):
    review_service = _ReviewService()
    link_client = _LinkClient()
    service = CapacityReportImageDeliveryService(
        {},
        review_service=review_service,
        link_service=_LinkService(link_client),
        capacity_service=_CapacityService(),
        summary_service=_SummaryService("【A楼世纪互联 白班】\n【交班人员】张三\n【接班人员】李四"),
    )

    def _render_image(*, source_path, output_path, emit_log):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        image = Image.new("RGB", (600, 400), "white")
        for x in range(50, 350):
            for y in range(50, 180):
                image.putpixel((x, y), (0, 0, 0))
        image.save(output_path)
        return output_path

    monkeypatch.setattr(service, "_render_capacity_report_image", _render_image)

    session = _session(tmp_path)
    result = service.send_for_session(session, ensure_capacity_ready=lambda: session, emit_log=lambda _text: None)

    assert result["ok"] is True, result
    assert review_service.review_updates == []
    assert result["review_link_delivery"]["source"] == "review_link"
    assert link_client.text_messages and link_client.image_messages
    assert result["capacity_image_delivery"]["text_successful_recipients"] == ["ou_test"]
    assert result["capacity_image_delivery"]["image_successful_recipients"] == ["ou_test"]


def test_rendered_capacity_image_rejects_blank_png(tmp_path):
    blank = tmp_path / "blank.png"
    Image.new("RGB", (600, 400), "white").save(blank)

    with pytest.raises(ValueError, match="空白图片"):
        CapacityReportImageDeliveryService._validate_rendered_image_content(blank)
