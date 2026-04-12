from __future__ import annotations

import sqlite3

import pytest

from handover_log_module.service.review_session_service import (
    ReviewSessionService,
    ReviewSessionStoreUnavailableError,
)


def test_get_session_by_id_wraps_recoverable_store_error(monkeypatch) -> None:
    service = ReviewSessionService({})

    def _raise_locked():
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(service._review_state_store, "load_state", _raise_locked)

    with pytest.raises(ReviewSessionStoreUnavailableError, match="审核状态存储暂时不可用"):
        service.get_session_by_id("A楼|2026-04-10|day")


def test_claim_session_lock_wraps_permission_error(monkeypatch) -> None:
    service = ReviewSessionService({})
    session = {
        "session_id": "A楼|2026-04-10|day",
        "building": "A楼",
        "revision": 3,
    }

    monkeypatch.setattr(service, "get_session_by_id", lambda _session_id: dict(session))

    def _raise_permission_error(**_kwargs):
        raise PermissionError("WinError 5")

    monkeypatch.setattr(service._review_state_store, "claim_lock", _raise_permission_error)

    with pytest.raises(ReviewSessionStoreUnavailableError, match="审核状态存储暂时不可用"):
        service.claim_session_lock(
            building="A楼",
            session_id="A楼|2026-04-10|day",
            client_id="terminal-a001",
            holder_label="终端-A001",
        )
