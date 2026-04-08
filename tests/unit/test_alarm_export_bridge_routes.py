from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.modules.report_pipeline.api import routes


class _Container:
    pass


class _App:
    def __init__(self) -> None:
        self.state = type("State", (), {"container": _Container()})()


class _Request:
    def __init__(self) -> None:
        self.app = _App()


def test_alarm_export_route_is_retired() -> None:
    request = _Request()
    with pytest.raises(HTTPException) as exc_info:
        routes.job_alarm_export_run(request)
    assert exc_info.value.status_code == 410
    assert "已退役" in str(exc_info.value.detail)
