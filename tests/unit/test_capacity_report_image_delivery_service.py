from __future__ import annotations

from pathlib import Path

from PIL import Image

from handover_log_module.service.capacity_report_image_delivery_service import CapacityReportImageDeliveryService


def _write_png(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", (80, 40), "white")
    image.save(path, format="PNG")


class _FakeReviewService:
    def __init__(self) -> None:
        self.capacity_updates = []
        self.review_updates = []

    def update_capacity_image_delivery(self, *, session_id: str, capacity_image_delivery):
        self.capacity_updates.append({"session_id": session_id, "delivery": dict(capacity_image_delivery)})
        return {"capacity_image_delivery": dict(capacity_image_delivery)}

    def update_review_link_delivery(self, *, session_id: str, review_link_delivery):
        self.review_updates.append({"session_id": session_id, "delivery": dict(review_link_delivery)})
        return {"review_link_delivery": dict(review_link_delivery)}


class _FakeClient:
    def __init__(self, *, fail_text: str = "", fail_image: str = "") -> None:
        self.uploaded = []
        self.text_messages = []
        self.image_messages = []
        self.fail_text = fail_text
        self.fail_image = fail_image

    def upload_image(self, image_path: str):
        self.uploaded.append(image_path)
        return {"image_key": "img-key"}

    def send_text_message(self, *, receive_id: str, receive_id_type: str, text: str):
        if receive_id == self.fail_text:
            raise RuntimeError("text failed")
        self.text_messages.append({"receive_id": receive_id, "receive_id_type": receive_id_type, "text": text})
        return {"message_id": f"text-{len(self.text_messages)}"}

    def send_image_message(self, *, receive_id: str, receive_id_type: str, image_key: str):
        if receive_id == self.fail_image:
            raise RuntimeError("image failed")
        self.image_messages.append({"receive_id": receive_id, "receive_id_type": receive_id_type, "image_key": image_key})
        return {"message_id": f"image-{len(self.image_messages)}"}


class _FakeLinkService:
    def __init__(self, client: _FakeClient) -> None:
        self.client = client

    def _recipient_snapshot_for_building(self, _building):
        return {
            "recipients": [{"open_id": "ou_1", "note": "甲"}, {"open_id": "ou_2", "note": "乙"}],
            "raw_count": 3,
            "enabled_count": 2,
            "disabled_count": 1,
            "invalid_count": 0,
            "open_ids": ["ou_1", "ou_2"],
        }

    def _build_feishu_client(self):
        return self.client

    @staticmethod
    def _resolve_effective_receive_id_type(_recipient_id, _configured_receive_id_type="open_id"):
        return "open_id"


class _FakeSummaryService:
    @staticmethod
    def build_for_session(_session, *, emit_log):
        return "交接班日志全文"


class _FakeCapacityService:
    def __init__(self, signature: str = "sig-current") -> None:
        self.signature = signature
        self.signature_calls = 0

    def build_capacity_overlay_signature(self, **_kwargs):
        self.signature_calls += 1
        return {
            "signature": self.signature,
            "input_signature": "input-current",
            "valid": True,
            "error": "",
        }

    def sync_overlay_for_existing_report_from_cells(self, **_kwargs):
        return {"status": "ready", "input_signature": "input-current", "error": ""}


def _base_session(source: Path, *, image_path: str = "", image_signature: str = ""):
    return {
        "session_id": "A楼|2026-04-29|day",
        "building": "A楼",
        "duty_date": "2026-04-29",
        "duty_shift": "day",
        "output_file": str(source),
        "capacity_output_file": str(source),
        "capacity_sync": {"status": "ready", "input_signature": "input-current"},
        "capacity_image_delivery": {
            "status": "",
            "image_path": image_path,
            "image_signature": image_signature,
        },
    }


def _service(tmp_path: Path, client: _FakeClient, review: _FakeReviewService, capacity: _FakeCapacityService):
    return CapacityReportImageDeliveryService(
        {"_global_paths": {"runtime_state_root": str(tmp_path / "runtime")}, "capacity_report": {"template": {"sheet_name": "本班组"}}},
        review_service=review,
        link_service=_FakeLinkService(client),
        capacity_service=capacity,
        summary_service=_FakeSummaryService(),
    )


def test_capacity_image_delivery_uploads_once_and_sends_text_then_image(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "capacity.xlsx"
    source.write_bytes(b"xlsx")
    client = _FakeClient()
    review = _FakeReviewService()
    capacity = _FakeCapacityService()
    service = _service(tmp_path, client, review, capacity)
    rendered = []

    def _fake_render(*, source_path: Path, output_path: Path, emit_log):
        rendered.append(source_path)
        _write_png(output_path)
        return output_path

    monkeypatch.setattr(service, "_render_excel_copy_picture", _fake_render)
    logs = []

    result = service.send_for_session(
        _base_session(source),
        handover_cells={"H6": "1", "F8": "西区1 东区2", "B6": "1", "D6": "2", "F6": "3", "B13": "4", "D13": "5"},
        ensure_capacity_ready=lambda: _base_session(source),
        emit_log=logs.append,
    )

    assert result["status"] == "success"
    assert len(client.uploaded) == 1
    assert [item["receive_id"] for item in client.text_messages] == ["ou_1", "ou_2"]
    assert [item["receive_id"] for item in client.image_messages] == ["ou_1", "ou_2"]
    assert rendered == [source]
    assert review.capacity_updates[-1]["delivery"]["status"] == "success"
    assert review.review_updates[-1]["delivery"]["status"] == "success"


def test_capacity_image_delivery_reuses_valid_cached_png(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "capacity.xlsx"
    source.write_bytes(b"xlsx")
    cached = tmp_path / "cached.png"
    _write_png(cached)
    stat = cached.stat()
    client = _FakeClient()
    review = _FakeReviewService()
    capacity = _FakeCapacityService()
    service = _service(tmp_path, client, review, capacity)
    monkeypatch.setattr(
        service,
        "_render_excel_copy_picture",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("should not render")),
    )
    session = _base_session(source, image_path=str(cached), image_signature="sig-current")
    session["capacity_image_delivery"]["image_file_size"] = stat.st_size
    session["capacity_image_delivery"]["image_file_mtime_ns"] = stat.st_mtime_ns

    result = service.send_for_session(session, handover_cells={}, emit_log=lambda _line: None)

    assert result["status"] == "success"
    assert client.uploaded == [str(cached)]


def test_capacity_image_delivery_text_failure_returns_text_step(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "capacity.xlsx"
    source.write_bytes(b"xlsx")
    client = _FakeClient(fail_text="ou_1")
    review = _FakeReviewService()
    service = _service(tmp_path, client, review, _FakeCapacityService())

    def _fake_render(*, source_path: Path, output_path: Path, emit_log):
        _write_png(output_path)
        return output_path

    monkeypatch.setattr(service, "_render_excel_copy_picture", _fake_render)

    result = service.send_for_session(
        _base_session(source),
        handover_cells={},
        ensure_capacity_ready=lambda: _base_session(source),
        emit_log=lambda _line: None,
    )

    assert result["status"] == "failed"
    assert result["failed_recipients"][0]["open_id"] == "ou_1"
    assert result["failed_recipients"][0]["step"] == "text"
    assert [item["receive_id"] for item in client.image_messages] == ["ou_2"]


def test_capacity_image_delivery_screenshot_failure_does_not_upload_or_send(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "capacity.xlsx"
    source.write_bytes(b"xlsx")
    client = _FakeClient()
    review = _FakeReviewService()
    service = _service(tmp_path, client, review, _FakeCapacityService())
    monkeypatch.setattr(
        service,
        "_render_excel_copy_picture",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("excel failed")),
    )

    result = service.send_for_session(
        _base_session(source),
        handover_cells={},
        ensure_capacity_ready=lambda: _base_session(source),
        emit_log=lambda _line: None,
    )

    assert result["status"] == "failed"
    assert "Excel截图失败" in result["error"]
    assert client.uploaded == []
    assert client.text_messages == []
    assert client.image_messages == []
