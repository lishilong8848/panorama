from __future__ import annotations

import asyncio
import io
from pathlib import Path
from types import SimpleNamespace

from starlette.datastructures import UploadFile

from app.modules.handover_review.api import routes


class _FakeJob:
    def __init__(self, job_id: str = "job-1") -> None:
        self.job_id = job_id

    def to_dict(self) -> dict:
        return {"job_id": self.job_id, "status": "queued"}


class _FakeJobService:
    def __init__(self) -> None:
        self.last_run_func = None
        self.last_name = ""

    def start_job(self, name, run_func, **kwargs):  # noqa: ANN001
        self.last_name = name
        self.last_run_func = run_func
        return _FakeJob()

    def run_last(self):
        assert self.last_run_func is not None
        return self.last_run_func(lambda message: None)


def _fake_request(logs=None):
    log_lines = logs if isinstance(logs, list) else []
    job_service = _FakeJobService()
    container = SimpleNamespace(
        add_system_log=lambda line, *_args, **_kwargs: log_lines.append(str(line)),
        config=object(),
        config_path="config.json",
        reload_config=lambda _cfg: None,
        runtime_config={},
        job_service=job_service,
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))
    request._job_service = job_service
    request._logs = log_lines
    return request


class _ReviewService:
    def get_cloud_batch(self, batch_key):
        return {"spreadsheet_url": "https://example.com/wiki", "batch_key": batch_key}


class _ReviewServiceWithoutSpreadsheet:
    def get_cloud_batch(self, batch_key):
        return {"spreadsheet_url": "", "batch_key": batch_key}


class _StateService:
    def __init__(self) -> None:
        self._export_state = {
            "status": "success",
            "record_id": "rec_1",
            "record_url": "",
            "spreadsheet_url": "https://example.com/wiki",
            "summary_screenshot_path": "summary.png",
            "external_screenshot_path": "external.png",
            "updated_at": "2026-03-24 10:00:00",
            "error": "",
            "error_code": "",
            "error_detail": "",
        }

    def build_batch_key(self, duty_date, duty_shift):
        return f"{duty_date}|{duty_shift}"

    def get_export_state(self, *, duty_date, duty_shift):
        return dict(self._export_state)

    def mark_pending_asset_rewrite(self, *, duty_date, duty_shift):
        self._export_state.update(
            {
                "status": "pending_asset_rewrite",
                "updated_at": "2026-03-24 10:05:00",
                "error": "",
                "error_code": "",
                "error_detail": "",
            }
        )
        return dict(self._export_state)

    def get_screenshot_auth_state(self):
        return {"status": "ready", "profile_dir": "profile", "last_checked_at": "2026-03-24 10:00:00", "error": ""}

    def get_context(self, **kwargs):
        return {
            "ok": True,
            "batch_key": "2026-03-24|day",
            "duty_date": kwargs["duty_date"],
            "duty_shift": kwargs["duty_shift"],
            "daily_report_record_export": {
                **self._export_state,
                "spreadsheet_url": kwargs["spreadsheet_url"],
            },
            "screenshot_auth": kwargs["screenshot_auth"],
            "capture_assets": kwargs["capture_assets"],
        }

    def update_export_state(self, **kwargs):
        payload = dict(kwargs["daily_report_record_export"])
        payload.setdefault("updated_at", "2026-03-24 10:06:00")
        self._export_state = payload
        return dict(self._export_state)


class _AssetService:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.png_bytes = (
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
            b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\rIDATx\x9cc````\x00\x00\x00\x05\x00\x01"
            b"\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
        )
        self.summary_auto = tmp_path / "summary_sheet_auto.png"
        self.summary_auto.write_bytes(self.png_bytes)
        self.summary_manual = tmp_path / "summary_sheet_manual.png"
        self.external_auto = tmp_path / "external_page_auto.png"
        self.external_auto.write_bytes(self.png_bytes)
        self.external_manual = tmp_path / "external_page_manual.png"

    def get_capture_assets_context(self, *, duty_date, duty_shift):  # noqa: ARG002
        return {
            "summary_sheet_image": {
                "exists": True,
                "source": "auto",
                "stored_path": str(self.summary_auto),
                "captured_at": "2026-03-24 10:00:00",
                "preview_url": "/summary/thumb",
                "thumbnail_url": "/summary/thumb",
                "full_image_url": "/summary/full",
                "auto": {
                    "exists": True,
                    "stored_path": str(self.summary_auto),
                    "captured_at": "2026-03-24 10:00:00",
                    "preview_url": "/summary/auto/thumb",
                    "thumbnail_url": "/summary/auto/thumb",
                    "full_image_url": "/summary/auto/full",
                },
                "manual": {
                    "exists": self.summary_manual.exists(),
                    "stored_path": str(self.summary_manual) if self.summary_manual.exists() else "",
                    "captured_at": "2026-03-24 10:05:00" if self.summary_manual.exists() else "",
                    "preview_url": "/summary/manual/thumb" if self.summary_manual.exists() else "",
                    "thumbnail_url": "/summary/manual/thumb" if self.summary_manual.exists() else "",
                    "full_image_url": "/summary/manual/full" if self.summary_manual.exists() else "",
                },
            },
            "external_page_image": {
                "exists": True,
                "source": "auto",
                "stored_path": str(self.external_auto),
                "captured_at": "2026-03-24 10:00:01",
                "preview_url": "/external/thumb",
                "thumbnail_url": "/external/thumb",
                "full_image_url": "/external/full",
                "auto": {
                    "exists": True,
                    "stored_path": str(self.external_auto),
                    "captured_at": "2026-03-24 10:00:01",
                    "preview_url": "/external/auto/thumb",
                    "thumbnail_url": "/external/auto/thumb",
                    "full_image_url": "/external/auto/full",
                },
                "manual": {
                    "exists": self.external_manual.exists(),
                    "stored_path": str(self.external_manual) if self.external_manual.exists() else "",
                    "captured_at": "2026-03-24 10:05:00" if self.external_manual.exists() else "",
                    "preview_url": "/external/manual/thumb" if self.external_manual.exists() else "",
                    "thumbnail_url": "/external/manual/thumb" if self.external_manual.exists() else "",
                    "full_image_url": "/external/manual/full" if self.external_manual.exists() else "",
                },
            },
        }

    def get_asset_file_path(self, *, duty_date, duty_shift, target, variant, view="full"):  # noqa: ARG002
        if target == "summary_sheet":
            if view == "thumb":
                thumb = self.tmp_path / "summary_sheet_thumb.jpg"
                thumb.write_bytes(b"jpg")
                return thumb
            return self.summary_auto
        if view == "thumb":
            thumb = self.tmp_path / "external_page_thumb.jpg"
            thumb.write_bytes(b"jpg")
            return thumb
        return self.external_auto

    def save_manual_image(self, *, duty_date, duty_shift, target, content, mime_type="", original_name=""):  # noqa: ARG002
        path = self.summary_manual if target == "summary_sheet" else self.external_manual
        path.write_bytes(content)
        return path

    def delete_manual_image(self, *, duty_date, duty_shift, target):
        path = self.summary_manual if target == "summary_sheet" else self.external_manual
        if not path.exists():
            return False
        path.unlink()
        return True


class _ScreenshotService:
    def check_auth_status(self, emit_log, ensure_browser_running=False):  # noqa: ARG002
        return {"status": "ready", "profile_dir": "profile", "last_checked_at": "2026-03-24 10:00:00", "error": ""}

    def capture_summary_sheet(self, **kwargs):
        assert "spreadsheet_url" not in kwargs
        return {"status": "ok", "error": "", "path": "summary_auto.png"}

    def capture_external_page(self, **kwargs):
        return {"status": "ok", "error": "", "path": "external_auto.png"}


class _StructuredFailScreenshotService(_ScreenshotService):
    def capture_summary_sheet(self, **kwargs):  # noqa: ARG002
        return {
            "status": "failed",
            "stage": "find_existing_page",
            "error": "target_page_not_open",
            "error_detail": "target_page_not_open",
            "error_message": "目标网页当前没有在系统浏览器中打开，请先打开对应页面后再重试。",
            "path": "",
        }


class _PageMismatchScreenshotService(_ScreenshotService):
    def capture_summary_sheet(self, **kwargs):  # noqa: ARG002
        return {
            "status": "failed",
            "stage": "find_existing_page",
            "error": "target_page_mismatch",
            "error_detail": "target_page_id=pgeZUMIpMDuIIfLA, resolved_page_id=pgecZCUXaEtvP9Yl",
            "error_message": "当前打开页面与目标页面不一致，请重新打开对应飞书页面后重试。",
            "resolved_url": "https://vnet.feishu.cn/app/LTjUbmZsTaTFIVsuQSLcUi4Onf4?pageId=pgecZCUXaEtvP9Yl",
            "resolved_page_id": "pgecZCUXaEtvP9Yl",
            "matched_mode": "reused",
            "path": "",
        }


class _ExplodingScreenshotService(_ScreenshotService):
    def capture_summary_sheet(self, **kwargs):  # noqa: ARG002
        raise RuntimeError("boom")


class _FollowupService:
    def __init__(self, _cfg) -> None:
        pass

    def rewrite_daily_report_record(self, *, duty_date, duty_shift, emit_log):  # noqa: ARG002
        return {
            "status": "success",
            "record_id": "rec_new",
            "record_url": "",
            "spreadsheet_url": "https://example.com/wiki",
            "summary_screenshot_path": "summary_sheet_manual.png",
            "external_screenshot_path": "external_page_auto.png",
            "updated_at": "2026-03-24 10:10:00",
            "error": "",
        }


class _FailingFollowupService:
    def __init__(self, _cfg) -> None:
        pass

    def rewrite_daily_report_record(self, *, duty_date, duty_shift, emit_log):  # noqa: ARG002
        exc = RuntimeError("飞书接口调用失败: {'code': 1254068, 'msg': 'URLFieldConvFail', 'error': {'message': 'invalid'}}")
        exc.error_code = "daily_report_url_field_invalid"
        exc.error_detail = str(exc)
        raise exc


def test_handover_daily_report_capture_asset_file_route(tmp_path, monkeypatch):
    asset_service = _AssetService(tmp_path)
    monkeypatch.setattr(
        routes,
        "_build_daily_report_services",
        lambda _container: (_ReviewService(), _StateService(), asset_service, _ScreenshotService()),
    )
    response = routes.handover_daily_report_capture_asset_file(
        _fake_request(),
        duty_date="2026-03-24",
        duty_shift="day",
        target="summary_sheet",
        variant="effective",
    )
    assert Path(response.path) == asset_service.summary_auto


def test_handover_daily_report_capture_asset_thumb_route(tmp_path, monkeypatch):
    asset_service = _AssetService(tmp_path)
    monkeypatch.setattr(
        routes,
        "_build_daily_report_services",
        lambda _container: (_ReviewService(), _StateService(), asset_service, _ScreenshotService()),
    )
    response = routes.handover_daily_report_capture_asset_file(
        _fake_request(),
        duty_date="2026-03-24",
        duty_shift="day",
        target="external_page",
        variant="effective",
        view="thumb",
    )
    assert Path(response.path).suffix.lower() == ".jpg"
    assert response.media_type == "image/jpeg"


def test_handover_daily_report_recapture_route_returns_context(tmp_path, monkeypatch):
    asset_service = _AssetService(tmp_path)
    state_service = _StateService()
    monkeypatch.setattr(
        routes,
        "_build_daily_report_services",
        lambda _container: (_ReviewServiceWithoutSpreadsheet(), state_service, asset_service, _ScreenshotService()),
    )
    request = _fake_request()
    accepted = routes.handover_daily_report_recapture_asset(
        request,
        {"duty_date": "2026-03-24", "duty_shift": "day", "target": "summary_sheet"},
    )
    assert accepted["ok"] is True
    assert accepted["accepted"] is True
    payload = request._job_service.run_last()
    assert payload["ok"] is True
    assert payload["target"] == "summary_sheet"
    assert payload["result"]["status"] == "ok"
    assert payload["daily_report_record_export"]["status"] == "pending_asset_rewrite"


def test_handover_daily_report_upload_and_restore_routes(tmp_path, monkeypatch):
    asset_service = _AssetService(tmp_path)
    state_service = _StateService()
    monkeypatch.setattr(
        routes,
        "_build_daily_report_services",
        lambda _container: (_ReviewService(), state_service, asset_service, _ScreenshotService()),
    )
    upload = UploadFile(filename="clip.png", file=io.BytesIO(b"manual-image"), headers={"content-type": "image/png"})

    upload_payload = asyncio.run(
        routes.handover_daily_report_upload_asset(
            _fake_request(),
            duty_date="2026-03-24",
            duty_shift="day",
            target="external_page",
            file=upload,
        )
    )
    assert upload_payload["ok"] is True
    assert upload_payload["target"] == "external_page"

    restore_payload = routes.handover_daily_report_delete_manual_asset(
        _fake_request(),
        duty_date="2026-03-24",
        duty_shift="day",
        target="external_page",
    )
    assert restore_payload["ok"] is True
    assert restore_payload["removed"] is True


def test_handover_daily_report_rewrite_record_route(tmp_path, monkeypatch):
    asset_service = _AssetService(tmp_path)
    state_service = _StateService()
    monkeypatch.setattr(
        routes,
        "_build_daily_report_services",
        lambda _container: (_ReviewService(), state_service, asset_service, _ScreenshotService()),
    )
    monkeypatch.setattr(routes, "ReviewFollowupTriggerService", _FollowupService)

    request = _fake_request()
    accepted = routes.handover_daily_report_rewrite_record(
        request,
        {"duty_date": "2026-03-24", "duty_shift": "day"},
    )
    assert accepted["ok"] is True
    assert accepted["accepted"] is True
    payload = request._job_service.run_last()
    assert payload["ok"] is True
    assert payload["error"] == ""
    assert payload["error_code"] == ""


def test_handover_daily_report_recapture_route_returns_structured_failure(tmp_path, monkeypatch):
    asset_service = _AssetService(tmp_path)
    state_service = _StateService()
    monkeypatch.setattr(
        routes,
        "_build_daily_report_services",
        lambda _container: (_ReviewService(), state_service, asset_service, _StructuredFailScreenshotService()),
    )

    request = _fake_request()
    routes.handover_daily_report_recapture_asset(
        request,
        {"duty_date": "2026-03-24", "duty_shift": "day", "target": "summary_sheet"},
    )
    payload = request._job_service.run_last()

    assert payload["ok"] is False
    assert payload["result"]["status"] == "failed"
    assert payload["result"]["stage"] == "find_existing_page"
    assert payload["result"]["error"] == "target_page_not_open"
    assert payload["result"]["error_message"] == "目标网页当前没有在系统浏览器中打开，请先打开对应页面后再重试。"


def test_handover_daily_report_recapture_route_converts_unexpected_exception(tmp_path, monkeypatch):
    logs = []
    asset_service = _AssetService(tmp_path)
    state_service = _StateService()
    monkeypatch.setattr(
        routes,
        "_build_daily_report_services",
        lambda _container: (_ReviewService(), state_service, asset_service, _ExplodingScreenshotService()),
    )

    request = _fake_request(logs)
    routes.handover_daily_report_recapture_asset(
        request,
        {"duty_date": "2026-03-24", "duty_shift": "day", "target": "summary_sheet"},
    )
    payload = request._job_service.run_last()

    assert payload["ok"] is False
    assert payload["result"]["status"] == "failed"
    assert payload["result"]["stage"] == "unknown"
    assert payload["result"]["error_message"] == "操作失败，请查看系统错误日志。"


def test_handover_daily_report_rewrite_record_route_returns_structured_failure(tmp_path, monkeypatch):
    asset_service = _AssetService(tmp_path)
    state_service = _StateService()
    monkeypatch.setattr(
        routes,
        "_build_daily_report_services",
        lambda _container: (_ReviewService(), state_service, asset_service, _ScreenshotService()),
    )
    monkeypatch.setattr(routes, "ReviewFollowupTriggerService", _FailingFollowupService)

    request = _fake_request()
    routes.handover_daily_report_rewrite_record(
        request,
        {"duty_date": "2026-03-24", "duty_shift": "day"},
    )
    payload = request._job_service.run_last()

    assert payload["ok"] is False
    assert payload["error_code"] == "daily_report_url_field_invalid"
    assert "URLFieldConvFail" in payload["error_detail"]


def test_handover_daily_report_recapture_route_returns_page_resolution_meta(tmp_path, monkeypatch):
    asset_service = _AssetService(tmp_path)
    state_service = _StateService()
    monkeypatch.setattr(
        routes,
        "_build_daily_report_services",
        lambda _container: (_ReviewService(), state_service, asset_service, _PageMismatchScreenshotService()),
    )

    request = _fake_request()
    routes.handover_daily_report_recapture_asset(
        request,
        {"duty_date": "2026-03-24", "duty_shift": "day", "target": "summary_sheet"},
    )
    payload = request._job_service.run_last()

    assert payload["ok"] is False
    assert payload["result"]["error"] == "target_page_mismatch"
    assert payload["result"]["resolved_page_id"] == "pgecZCUXaEtvP9Yl"
    assert payload["result"]["matched_mode"] == "reused"
