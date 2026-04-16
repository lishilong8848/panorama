from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.modules.handover_review.api import routes
from handover_log_module.service.review_session_service import ReviewSessionStoreUnavailableError


class _DummyJobService:
    @contextmanager
    def resource_guard(self, **_kwargs):
        yield


def _fake_request():
    container = SimpleNamespace(
        add_system_log=lambda *_args, **_kwargs: None,
        config={},
        config_path="config.json",
        reload_config=lambda _cfg: None,
        job_service=_DummyJobService(),
    )
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_handover_review_data_returns_503_when_store_temporarily_unavailable(monkeypatch) -> None:
    class _Parser:
        config = {}

        def parse(self, _output_file):
            return {"fixed_blocks": [], "sections": [], "footer_blocks": []}

    class _Service:
        def get_building_by_code(self, _building_code):
            return "A楼"

        def get_session_by_id(self, _session_id):
            raise ReviewSessionStoreUnavailableError("审核状态存储暂时不可用，请稍后重试")

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), _Parser(), None, None))

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_data(
            "a",
            _fake_request(),
            session_id="A楼|2026-04-10|day",
        )

    assert exc_info.value.status_code == 503
    assert "审核状态存储暂时不可用" in str(exc_info.value.detail)


def test_handover_review_confirm_returns_503_when_store_temporarily_unavailable(monkeypatch) -> None:
    class _Service:
        def get_building_by_code(self, _building_code):
            return "A楼"

        def get_session_by_id(self, _session_id):
            return {
                "session_id": "A楼|2026-04-10|day",
                "building": "A楼",
                "batch_key": "2026-04-10|day",
                "output_file": __file__,
            }

        def get_latest_session_id(self, _building):
            return "A楼|2026-04-10|day"

        def mark_confirmed(self, **_kwargs):
            raise ReviewSessionStoreUnavailableError("审核状态存储暂时不可用，请稍后重试")

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), None, None, None))
    monkeypatch.setattr(routes, "_ensure_session_lock_held_or_409", lambda *_args, **_kwargs: None)

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_confirm(
            "a",
            _fake_request(),
            {"session_id": "A楼|2026-04-10|day", "base_revision": 1, "client_id": "review-a001"},
        )

    assert exc_info.value.status_code == 503
    assert "审核状态存储暂时不可用" in str(exc_info.value.detail)


def test_handover_review_lock_claim_returns_503_when_store_temporarily_unavailable(monkeypatch) -> None:
    class _Service:
        def get_building_by_code(self, _building_code):
            return "A楼"

        def get_session_by_id(self, _session_id):
            return {
                "session_id": "A楼|2026-04-10|day",
                "building": "A楼",
                "batch_key": "2026-04-10|day",
                "output_file": __file__,
            }

        def claim_session_lock(self, **_kwargs):
            raise ReviewSessionStoreUnavailableError("审核状态存储暂时不可用，请稍后重试")

    monkeypatch.setattr(routes, "_build_review_services", lambda _container: (_Service(), None, None, None))

    with pytest.raises(HTTPException) as exc_info:
        routes.handover_review_lock_claim(
            "a",
            _fake_request(),
            {
                "session_id": "A楼|2026-04-10|day",
                "client_id": "terminal-a001",
                "holder_label": "终端-A001",
            },
        )

    assert exc_info.value.status_code == 503
    assert "审核状态存储暂时不可用" in str(exc_info.value.detail)
