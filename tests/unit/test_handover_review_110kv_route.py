from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.modules.handover_review.api import routes
from handover_log_module.service.review_session_service import ReviewSessionService


def _fake_request():
    logs: list[str] = []
    container = SimpleNamespace(
        add_system_log=logs.append,
        config={},
        job_service=SimpleNamespace(),
    )
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container))), logs


def _block(*, batch_key: str = "2026-04-25|day", revision: int = 3, power_kw: str = "100") -> dict:
    block = ReviewSessionService.normalize_substation_110kv_payload(
        {
            "batch_key": batch_key,
            "revision": revision,
            "updated_at": "2026-04-25 17:00:00",
            "updated_by_building": "A楼",
            "rows": [
                {
                    "row_id": "incoming_akai",
                    "label": "阿开",
                    "line_voltage": "110",
                    "current": "10",
                    "power_kw": power_kw,
                    "power_factor": "0.98",
                    "load_rate": "50%",
                },
            ],
        },
        batch_key=batch_key,
    )
    return block


class _FakeReviewService:
    def __init__(self, current: dict, *, save_error: str = "") -> None:
        self.current = current
        self.save_error = save_error
        self.saved_calls: list[dict] = []

    def get_substation_110kv(self, batch_key: str) -> dict:
        assert batch_key == "2026-04-25|day"
        return self.current

    def save_substation_110kv(self, **kwargs) -> dict:
        self.saved_calls.append(kwargs)
        if self.save_error:
            raise ValueError(self.save_error)
        saved = ReviewSessionService.normalize_substation_110kv_payload(
            {"batch_key": kwargs["batch_key"], "revision": int(self.current.get("revision", 0)) + 1, "rows": kwargs["rows"]},
            batch_key=kwargs["batch_key"],
        )
        self.current = saved
        return saved

    def get_substation_110kv_lock(self, *, batch_key: str, client_id: str = "") -> dict:
        return {
            "current_revision": int(self.current.get("revision", 0)),
            "client_holds_lock": True,
            "is_editing_elsewhere": False,
            "active_editor": None,
            "lease_expires_at": "",
        }

    def get_session_by_id(self, session_id: str) -> dict:
        return {
            "session_id": session_id,
            "building": "A楼",
            "batch_key": "2026-04-25|day",
            "revision": 9,
        }


def _patch_route_services(monkeypatch, service: _FakeReviewService, sync_calls: list[dict]) -> None:
    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (service, object(), object(), None))
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda _service, _code: "A楼")
    monkeypatch.setattr(
        routes,
        "_load_target_session_or_404",
        lambda _service, **_kwargs: {
            "session_id": "A楼|2026-04-25|day",
            "building": "A楼",
            "batch_key": "2026-04-25|day",
        },
    )
    monkeypatch.setattr(routes, "_attach_excel_sync_from_store", lambda _container, session: session)

    def _sync(**kwargs):
        sync_calls.append(kwargs)
        return {"updated": 5, "failed": 0, "errors": []}

    monkeypatch.setattr(routes, "_sync_substation_110kv_to_batch_capacity_reports", _sync)


def test_shared_110kv_save_no_change_skips_save_and_capacity_sync(monkeypatch) -> None:
    current = _block(revision=7, power_kw="100")
    service = _FakeReviewService(current)
    sync_calls: list[dict] = []
    _patch_route_services(monkeypatch, service, sync_calls)
    request, logs = _fake_request()

    response = routes.handover_review_shared_110kv_save(
        "a",
        request,
        {
            "session_id": "A楼|2026-04-25|day",
            "client_id": "old-page",
            "base_revision": 1,
            "rows": current["rows"],
        },
    )

    assert response["ok"] is True
    assert response["no_change"] is True
    assert response["shared_blocks"]["substation_110kv"]["revision"] == 7
    assert response["capacity_sync_result"]["updated"] == 0
    assert service.saved_calls == []
    assert sync_calls == []
    assert any("内容无变化" in line for line in logs)


def test_shared_110kv_save_changed_payload_saves_and_queues_capacity_sync(monkeypatch) -> None:
    current = _block(revision=7, power_kw="100")
    changed = _block(revision=7, power_kw="120")
    service = _FakeReviewService(current)
    sync_calls: list[dict] = []
    _patch_route_services(monkeypatch, service, sync_calls)
    request, _logs = _fake_request()

    response = routes.handover_review_shared_110kv_save(
        "a",
        request,
        {
            "session_id": "A楼|2026-04-25|day",
            "client_id": "client-a",
            "base_revision": 7,
            "rows": changed["rows"],
        },
    )

    assert response["ok"] is True
    assert "no_change" not in response
    assert response["shared_blocks"]["substation_110kv"]["revision"] == 8
    assert len(service.saved_calls) == 1
    assert service.saved_calls[0]["base_revision"] == 7
    assert len(sync_calls) == 1
    assert sync_calls[0]["shared_110kv"]["rows"][0]["power_kw"] == "120"


def test_substation_110kv_batch_sync_queues_light_overlay_scope(monkeypatch) -> None:
    shared_110kv = _block(revision=8, power_kw="120")
    document = {
        "fixed_blocks": [
            {
                "fields": [
                    {"cell": "H6", "value": "10"},
                    {"cell": "F8", "value": "西区30/东区40"},
                    {"cell": "B6", "value": "1.2"},
                    {"cell": "D6", "value": "2000"},
                    {"cell": "F6", "value": "1200"},
                    {"cell": "D8", "value": "8"},
                    {"cell": "B13", "value": "100"},
                    {"cell": "D13", "value": "80"},
                ]
            }
        ],
        "cooling_pump_pressures": {"rows": []},
    }
    queued_calls: list[dict] = []

    class _FakeReviewService:
        def list_batch_sessions(self, batch_key: str) -> list[dict]:
            assert batch_key == "2026-04-25|day"
            return [
                {
                    "session_id": "A楼|2026-04-25|day",
                    "building": "A楼",
                    "capacity_output_file": "capacity-a.xlsx",
                }
            ]

        def update_capacity_sync(self, **kwargs) -> dict:
            assert kwargs["capacity_status"] == "pending"
            return {
                "session_id": kwargs["session_id"],
                "building": "A楼",
                "capacity_output_file": "capacity-a.xlsx",
            }

    class _FakeDocumentState:
        def load_document(self, session: dict) -> tuple[dict, dict]:
            return document, session

    class _FakeQueueService:
        def enqueue_capacity_overlay_sync(self, **kwargs) -> dict:
            queued_calls.append(kwargs)
            return {"job_id": "job-1"}

    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *args, **kwargs: _FakeDocumentState())
    monkeypatch.setattr(routes, "_build_xlsx_write_queue_service", lambda *args, **kwargs: _FakeQueueService())

    result = routes._sync_substation_110kv_to_batch_capacity_reports(
        container=object(),
        review_service=_FakeReviewService(),
        parser=object(),
        writer=object(),
        shared_110kv=shared_110kv,
        emit_log=lambda _msg: None,
    )

    assert result["updated"] == 1
    assert queued_calls[0]["overlay_scope"] == "substation_110kv"
    assert queued_calls[0]["shared_110kv"]["rows"][0]["power_kw"] == "120"


@pytest.mark.parametrize(
    ("save_error", "detail"),
    [
        ("shared_block_revision_conflict", "110KV变电站内容已被其他楼栋更新，请刷新后重试"),
        ("shared_block_lock_required", "110KV变电站正在其他楼栋或终端编辑，请稍后重试"),
    ],
)
def test_shared_110kv_save_changed_payload_keeps_existing_conflict_semantics(
    monkeypatch,
    save_error: str,
    detail: str,
) -> None:
    current = _block(revision=7, power_kw="100")
    changed = _block(revision=7, power_kw="120")
    service = _FakeReviewService(current, save_error=save_error)
    sync_calls: list[dict] = []
    _patch_route_services(monkeypatch, service, sync_calls)
    request, _logs = _fake_request()

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_shared_110kv_save(
            "a",
            request,
            {
                "session_id": "A楼|2026-04-25|day",
                "client_id": "client-a",
                "base_revision": 1,
                "rows": changed["rows"],
            },
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == detail
    assert len(service.saved_calls) == 1
    assert sync_calls == []
