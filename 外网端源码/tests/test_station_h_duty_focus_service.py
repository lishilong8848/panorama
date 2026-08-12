import base64
import hashlib
import json
from io import BytesIO
from pathlib import Path

import pytest
import requests
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from openpyxl import load_workbook
from PIL import Image

from handover_log_module.service.station_h_duty_focus_service import (
    StationHDutyFocusService,
)
from handover_log_module.service.station_h_signature_service import (
    STATION_H_SIGNATURE_TABLES,
    StationHSignatureError,
    StationHSignatureService,
)


def _signature_png(color):
    stream = BytesIO()
    Image.new("RGBA", (180, 56), color).save(stream, format="PNG")
    return stream.getvalue()


class _ReviewService:
    def __init__(self):
        self.single_calls = 0
        self.many_calls = 0
        self.sessions = {
            "2026-08-12|day": [
                {
                    "building": "A楼",
                    "capacity_running_units": {
                        "west": [{"unit": 2, "mode_text": "制冷"}],
                        "east": [{"unit": 6, "mode_text": "板换"}],
                    },
                }
            ],
            "2026-08-11|night": [
                {
                    "building": "A楼",
                    "capacity_running_units": {
                        "west": [{"unit": 1, "mode_text": "预冷"}],
                        "east": [{"unit": 4, "mode_text": "制冷"}],
                    },
                }
            ],
        }

    def list_batch_sessions(self, batch_key):
        self.single_calls += 1
        return self.sessions.get(batch_key, [])

    def list_batch_sessions_many(self, batch_keys):
        self.many_calls += 1
        return {batch_key: self.sessions.get(batch_key, []) for batch_key in batch_keys}

    @staticmethod
    def get_outdoor_temperature_state(*, batch_key):
        assert batch_key == "2026-08-12|day"
        return {
            "shared_blocks": {
                "outdoor_temperature": {"cells": {"B7": "27.7", "D7": "25.8"}}
            }
        }


class _SignatureService:
    def __init__(self):
        first_table, second_table = STATION_H_SIGNATURE_TABLES[0][0], STATION_H_SIGNATURE_TABLES[1][0]
        self.people = [
            {
                "selection_id": f"{first_table}:rec_handover",
                "table_id": first_table,
                "record_id": "rec_handover",
                "source_label": "人员签名",
                "name": "张三",
                "signature_revision": "revision-handover-v1",
                "available": True,
            },
            {
                "selection_id": f"{second_table}:rec_takeover",
                "table_id": second_table,
                "record_id": "rec_takeover",
                "source_label": "公司外人员签名",
                "name": "李四",
                "signature_revision": "revision-takeover-v1",
                "available": True,
            },
        ]

    def cached_directory(self):
        return {"people": self.people, "refreshed_at": "2026-08-12 10:00:00", "error": ""}

    @staticmethod
    def match_person(people, name):
        return StationHSignatureService.match_person(people, name)

    @staticmethod
    def resolve_signature_png(*, table_id, record_id):
        color = (10, 80, 200, 255) if record_id == "rec_handover" else (20, 140, 80, 255)
        return {"table_id": table_id, "record_id": record_id, "png": _signature_png(color)}


def _service(tmp_path):
    project_template = Path(__file__).resolve().parents[1] / "值班关注点模板.xlsx"
    assert project_template.exists()
    return StationHDutyFocusService(
        {"_global_paths": {"runtime_state_root": str(tmp_path)}},
        review_service=_ReviewService(),
        signature_service=_SignatureService(),
        template_path=project_template,
    )


def test_auto_focus_uses_same_zone_changes_and_shared_temperature(tmp_path):
    service = _service(tmp_path)

    focus = service.build_status(
        duty_date="2026-08-12",
        duty_shift="day",
        selection={"current_people": ["张三"], "next_people": ["李四"]},
    )

    row = focus["rows"][0]
    assert row["modes"] == {
        "1": "△",
        "2": "制冷",
        "3": "△",
        "4": "△",
        "5": "△",
        "6": "板换",
    }
    assert row["change_note"] == "1#→2#、4#→6#"
    assert focus["checks"]["11"] == "√"
    assert focus["checks"]["19"] == "27.7℃/25.8℃"
    assert focus["signatures"]["handover"]["signature_revision"] == "revision-handover-v1"
    assert focus["signatures"]["takeover"]["signature_revision"] == "revision-takeover-v1"
    assert focus["print_ready"] is True
    assert service.review_service.many_calls == 1
    assert service.review_service.single_calls == 0


def test_change_note_never_pairs_across_chiller_sides():
    previous = {str(unit): ("制冷" if unit == 1 else "△") for unit in range(1, 7)}
    current = {str(unit): ("制冷" if unit == 4 else "△") for unit in range(1, 7)}

    note = StationHDutyFocusService._change_note(previous, current)

    assert note == "1#→△、△→4#"
    assert "1#→4#" not in note


def test_build_workbook_preserves_template_and_embeds_two_signatures(tmp_path):
    service = _service(tmp_path)
    focus = service.build_status(
        duty_date="2026-08-12",
        duty_shift="day",
        selection={"current_people": ["张三"], "next_people": ["李四"]},
    )

    output = service.build_workbook(
        duty_date="2026-08-12",
        duty_shift="day",
        focus=focus,
        require_signatures=True,
    )

    workbook = load_workbook(output, read_only=False, data_only=False)
    worksheet = workbook.worksheets[0]
    assert worksheet["B3"].value == "2026-08-12"
    assert "白班" in worksheet["C3"].value
    assert worksheet["B5"].value == "△"
    assert worksheet["C5"].value == "制冷"
    assert worksheet["G5"].value == "板换"
    assert worksheet["H5"].value == "1#→2#、4#→6#"
    assert worksheet["H11"].value == "√"
    assert worksheet["H19"].value == "27.7℃/25.8℃"
    assert worksheet["B3"].font.name == "宋体"
    assert worksheet["C5"].font.name == "宋体"
    assert worksheet["H19"].font.name == "宋体"
    assert worksheet["H19"].alignment.horizontal == "center"
    assert len(worksheet._images) == 2
    assert worksheet.print_area == "'Sheet1'!$A$1:$I$41"
    workbook.close()


def test_build_print_document_removes_intermediate_workbook(tmp_path, monkeypatch):
    service = _service(tmp_path)
    focus = service.build_status(
        duty_date="2026-08-12",
        duty_shift="day",
        selection={"current_people": ["张三"], "next_people": ["李四"]},
    )
    observed = {}

    def _fake_build_pdf(*, workbook_path):
        observed["workbook_path"] = Path(workbook_path)
        assert observed["workbook_path"].exists()
        pdf_path = observed["workbook_path"].parent / "print" / "result.pdf"
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        pdf_path.write_bytes(b"%PDF-1.4\n%%EOF")
        return pdf_path

    monkeypatch.setattr(service, "build_print_pdf", _fake_build_pdf)

    pdf_path = service.build_print_document(
        duty_date="2026-08-12",
        duty_shift="day",
        focus=focus,
    )

    assert pdf_path.exists()
    assert not observed["workbook_path"].exists()


def test_build_image_document_caches_current_image_and_marks_changed_focus_stale(tmp_path, monkeypatch):
    service = _service(tmp_path)
    focus = service.build_status(
        duty_date="2026-08-12",
        duty_shift="day",
        selection={"current_people": ["张三"], "next_people": ["李四"]},
    )
    observed = {"render_count": 0}

    def _fake_build_workbook(**_kwargs):
        workbook_path = service.output_root() / "image-source.xlsx"
        workbook_path.write_bytes(b"xlsx")
        observed["workbook_path"] = workbook_path
        return workbook_path

    def _fake_build_pdf(*, workbook_path):
        assert Path(workbook_path).exists()
        pdf_path = service.output_root() / "image-source.pdf"
        pdf_path.write_bytes(b"%PDF-1.4\n%%EOF")
        observed["pdf_path"] = pdf_path
        return pdf_path

    def _fake_render(_pdf_path):
        observed["render_count"] += 1
        return _signature_png((230, 240, 250, 255))

    monkeypatch.setattr(service, "build_workbook", _fake_build_workbook)
    monkeypatch.setattr(service, "build_print_pdf", _fake_build_pdf)
    monkeypatch.setattr(service, "_render_pdf_to_png_bytes", _fake_render)

    first = service.build_image_document(
        duty_date="2026-08-12",
        duty_shift="day",
        focus=focus,
        force=False,
    )
    second = service.build_image_document(
        duty_date="2026-08-12",
        duty_shift="day",
        focus=focus,
        force=False,
    )

    assert first["generated"] is True
    assert second["generated"] is False
    assert observed["render_count"] == 1
    assert Path(first["path"]).exists()
    assert not observed["workbook_path"].exists()
    assert not observed["pdf_path"].exists()

    changed = json.loads(json.dumps(focus, ensure_ascii=False))
    changed["checks"]["11"] = "待确认"
    status = service.image_status(
        duty_date="2026-08-12",
        duty_shift="day",
        focus=changed,
    )
    assert status["status"] == "stale"
    assert status["available"] is True
    assert status["current"] is False

    changed_signature = json.loads(json.dumps(focus, ensure_ascii=False))
    changed_signature["signatures"]["handover"]["signature_revision"] = "revision-handover-v2"
    signature_status = service.image_status(
        duty_date="2026-08-12",
        duty_shift="day",
        focus=changed_signature,
    )
    assert signature_status["status"] == "stale"
    assert signature_status["current"] is False


def test_signature_revision_changes_when_attachment_is_replaced():
    table_id = STATION_H_SIGNATURE_TABLES[0][0]
    base_record = {
        "record_id": "rec_handover",
        "last_modified_time": "1786500000000",
        "fields": {
            "姓名": "张三",
            "手写签名": [{"name": "张三.sigenc", "file_token": "token-v1", "size": 1024}],
            "密钥": json.dumps({
                "version": 2,
                "portable_dek": "dek",
                "file_nonce": "nonce",
                "encrypted_sha256": "encrypted-v1",
                "signature_sha256": "plain-v1",
            }),
        },
    }
    first = StationHSignatureService._person_from_record(
        base_record,
        table_id=table_id,
        source_label="人员签名",
    )
    replaced = json.loads(json.dumps(base_record, ensure_ascii=False))
    replaced["last_modified_time"] = "1786500001000"
    replaced["fields"]["手写签名"][0]["file_token"] = "token-v2"
    replaced["fields"]["密钥"] = json.dumps({
        "version": 2,
        "portable_dek": "dek",
        "file_nonce": "nonce",
        "encrypted_sha256": "encrypted-v2",
        "signature_sha256": "plain-v2",
    })
    second = StationHSignatureService._person_from_record(
        replaced,
        table_id=table_id,
        source_label="人员签名",
    )

    assert first["signature_revision"]
    assert second["signature_revision"]
    assert first["signature_revision"] != second["signature_revision"]


def test_portable_v2_signature_decryption_round_trip():
    plain = _signature_png((30, 60, 120, 255))
    key = bytes(range(32))
    nonce = bytes(range(12))
    aad = {"employee_no": "10001", "record_type": "signature"}
    canonical_aad = json.dumps(aad, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    encrypted = AESGCM(key).encrypt(nonce, plain, canonical_aad)
    payload = b"CLIPFLOW_SIGENC_V2\n" + encrypted
    metadata = {
        "version": 2,
        "portable_dek": base64.urlsafe_b64encode(key).decode("ascii").rstrip("="),
        "file_nonce": base64.urlsafe_b64encode(nonce).decode("ascii").rstrip("="),
        "aad": aad,
        "signature_sha256": hashlib.sha256(plain).hexdigest(),
        "encrypted_sha256": hashlib.sha256(payload).hexdigest(),
    }

    assert StationHSignatureService._decrypt_portable_v2(payload, metadata) == plain


def test_complete_saved_focus_does_not_reload_review_sessions(tmp_path):
    class _UnavailableReviewService:
        @staticmethod
        def list_batch_sessions(_batch_key):
            raise AssertionError("complete saved focus must not reload review sessions")

        @staticmethod
        def list_batch_sessions_many(_batch_keys):
            raise AssertionError("complete saved focus must not reload review sessions")

    service = _service(tmp_path)
    service.review_service = _UnavailableReviewService()
    first_table, second_table = STATION_H_SIGNATURE_TABLES[0][0], STATION_H_SIGNATURE_TABLES[1][0]
    saved_focus = {
        "date_text": "2099-01-01",
        "shift": "night",
        "rows": [
            {
                "building": building,
                "modes": {str(unit): "△" for unit in range(1, 7)},
                "change_note": "",
            }
            for building in ("A楼", "B楼", "C楼", "D楼", "E楼")
        ],
        "checks": {str(row): "√" for row in range(11, 40)},
        "signatures": {
            "handover": {
                "selection_id": f"{first_table}:rec_handover",
                "table_id": first_table,
                "record_id": "rec_handover",
                "name": "张三",
            },
            "takeover": {
                "selection_id": f"{second_table}:rec_takeover",
                "table_id": second_table,
                "record_id": "rec_takeover",
                "name": "李四",
            },
        },
    }

    focus = service.build_status(
        duty_date="2026-08-12",
        duty_shift="day",
        selection={
            "current_people": ["张三"],
            "next_people": ["李四"],
            "duty_focus": saved_focus,
        },
    )

    assert focus["rows"][0]["modes"]["1"] == "△"
    assert focus["auto_source"]["previous_batch"] == "2026-08-11|night"


def test_submitted_focus_is_bounded_and_forces_current_context():
    raw = {
        "date_text": "2099-01-01",
        "shift": "night",
        "rows": [
            {
                "building": "A楼",
                "modes": {"1": "INVALID"},
                "change_note": "x" * 500,
            }
        ],
        "checks": {"11": "y" * 1000},
        "signatures": {},
    }

    normalized = StationHDutyFocusService.normalize_submitted_focus(
        raw,
        duty_date="2026-08-12",
        duty_shift="day",
        selection={"current_people": ["张三"], "next_people": ["李四"]},
    )

    assert normalized["date_text"] == "2026-08-12"
    assert normalized["shift"] == "day"
    assert normalized["rows"][0]["modes"]["1"] == "△"
    assert len(normalized["rows"][0]["change_note"]) == 200
    assert len(normalized["checks"]["11"]) == 500
    assert len(normalized["rows"]) == 5
    assert len(normalized["checks"]) == 29


def test_signature_download_retries_interrupted_stream(monkeypatch):
    class _Client:
        timeout = 30

        @staticmethod
        def refresh_token(force=False):
            return "token-refreshed" if force else "token"

        @staticmethod
        def invalidate_token():
            return None

    class _Response:
        status_code = 200
        headers = {}

        def __init__(self, *, fail=False):
            self.fail = fail

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def iter_content(self, chunk_size):
            assert chunk_size == 64 * 1024
            if self.fail:
                raise requests.ConnectionError("stream interrupted")
            yield b"signature-bytes"

    responses = iter([_Response(fail=True), _Response()])
    calls = []

    def _get(*_args, **kwargs):
        calls.append(kwargs)
        return next(responses)

    monkeypatch.setattr(requests, "get", _get)

    payload = StationHSignatureService._download_encrypted_attachment(
        _Client(),
        {"download_url": "https://open.feishu.cn/signature.sigenc"},
    )

    assert payload == b"signature-bytes"
    assert len(calls) == 2
    assert all(call.get("stream") is True for call in calls)


def test_signature_download_rejects_oversized_attachment(monkeypatch):
    class _Client:
        timeout = 30

        @staticmethod
        def refresh_token(force=False):
            return "token"

    class _Response:
        status_code = 200
        headers = {"Content-Length": str(20 * 1024 * 1024 + 1)}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(requests, "get", lambda *_args, **_kwargs: _Response())

    with pytest.raises(StationHSignatureError, match="超过20MB限制"):
        StationHSignatureService._download_encrypted_attachment(
            _Client(),
            {"download_url": "https://open.feishu.cn/signature.sigenc"},
        )
