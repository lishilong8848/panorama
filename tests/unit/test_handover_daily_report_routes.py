from __future__ import annotations

from types import SimpleNamespace

from app.modules.handover_review.api import routes


class _FakeJob:
    def __init__(self, *, job_id: str, name: str, resource_keys: list[str], priority: str, feature: str, result):
        self.job_id = job_id
        self.name = name
        self.resource_keys = list(resource_keys)
        self.priority = priority
        self.feature = feature
        self.result = result

    def to_dict(self):
        return {
            "job_id": self.job_id,
            "name": self.name,
            "feature": self.feature,
            "submitted_by": self.priority,
            "status": "queued",
            "priority": self.priority,
            "resource_keys": list(self.resource_keys),
            "wait_reason": "",
            "summary": "",
            "result": self.result,
        }


class _FakeJobService:
    def __init__(self):
        self.calls = []

    def start_job(self, name, run_func, *, resource_keys=None, priority="manual", feature="", submitted_by=""):  # noqa: ANN001
        result = run_func(lambda *_args, **_kwargs: None)
        job = _FakeJob(
            job_id=f"job-{len(self.calls) + 1}",
            name=name,
            resource_keys=list(resource_keys or []),
            priority=submitted_by or priority,
            feature=feature or name,
            result=result,
        )
        self.calls.append(
            {
                "name": name,
                "resource_keys": list(resource_keys or []),
                "priority": priority,
                "feature": feature,
                "submitted_by": submitted_by,
                "result": result,
                "job": job,
            }
        )
        return job


def _fake_request():
    container = SimpleNamespace(
        add_system_log=lambda *_args, **_kwargs: None,
        config=object(),
        config_path="config.json",
        reload_config=lambda _cfg: None,
        job_service=_FakeJobService(),
    )
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def test_handover_daily_report_context_route(monkeypatch):
    class _ReviewService:
        def get_cloud_batch(self, batch_key):
            assert batch_key == "2026-03-24|night"
            return {"spreadsheet_url": "https://example.com/wiki"}

    class _StateService:
        def build_batch_key(self, duty_date, duty_shift):
            return f"{duty_date}|{duty_shift}"

        def get_export_state(self, *, duty_date, duty_shift):
            assert duty_date == "2026-03-24"
            assert duty_shift == "night"
            return {"status": "idle", "spreadsheet_url": ""}

        def get_context(self, **kwargs):
            return {
                "ok": True,
                "batch_key": "2026-03-24|night",
                "duty_date": kwargs["duty_date"],
                "duty_shift": kwargs["duty_shift"],
                "daily_report_record_export": {"status": "idle", "spreadsheet_url": kwargs["spreadsheet_url"]},
                "screenshot_auth": kwargs["screenshot_auth"],
                "capture_assets": kwargs["capture_assets"],
            }

    class _AssetService:
        def get_capture_assets_context(self, *, duty_date, duty_shift):
            assert duty_date == "2026-03-24"
            assert duty_shift == "night"
            return {"summary_sheet_image": {"exists": False}, "external_page_image": {"exists": False}}

    class _ScreenshotService:
        def check_auth_status(self, emit_log, ensure_browser_running=False):  # noqa: ARG002
            assert ensure_browser_running is False
            return {"status": "ready", "profile_dir": "profile", "last_checked_at": "2026-03-24 10:00:00", "error": ""}

    monkeypatch.setattr(
        routes,
        "_build_daily_report_services",
        lambda _container: (_ReviewService(), _StateService(), _AssetService(), _ScreenshotService()),
    )

    payload = routes.handover_daily_report_context(_fake_request(), duty_date="2026-03-24", duty_shift="night")

    assert payload["ok"] is True
    assert payload["batch_key"] == "2026-03-24|night"
    assert payload["daily_report_record_export"]["spreadsheet_url"] == "https://example.com/wiki"
    assert payload["screenshot_auth"]["status"] == "ready"


def test_handover_daily_report_open_auth_route_submits_job(monkeypatch):
    request = _fake_request()
    job_service = request.app.state.container.job_service

    class _ScreenshotService:
        def open_login_browser(self, emit_log):
            emit_log("opened")
            return {
                "ok": True,
                "status": "opened",
                "message": "browser opened",
                "profile_dir": "D:/runtime/playwright",
            }

    monkeypatch.setattr(
        routes,
        "_build_daily_report_services",
        lambda _container: (object(), object(), object(), _ScreenshotService()),
    )

    payload = routes.handover_daily_report_open_screenshot_auth(
        request,
        {"duty_date": "2026-03-24", "duty_shift": "night"},
    )

    assert payload["ok"] is True
    assert payload["accepted"] is True
    assert payload["job"]["feature"] == "daily_report_auth_open"
    assert payload["job"]["result"]["status"] == "opened"
    assert "browser:controlled" in job_service.calls[0]["resource_keys"]
    assert "handover_batch:2026-03-24|night" in job_service.calls[0]["resource_keys"]


def test_handover_daily_report_screenshot_test_route_submits_job(monkeypatch):
    request = _fake_request()
    job_service = request.app.state.container.job_service

    class _ReviewService:
        def get_cloud_batch(self, batch_key):
            assert batch_key == "2026-03-24|night"
            return {"spreadsheet_url": ""}

    class _AssetService:
        def get_capture_assets_context(self, *, duty_date, duty_shift):
            assert duty_date == "2026-03-24"
            assert duty_shift == "night"
            return {
                "summary_sheet_image": {"exists": True, "stored_path": "summary.png", "captured_at": "2026-03-24 10:00:00"},
                "external_page_image": {"exists": True, "stored_path": "external.png", "captured_at": "2026-03-24 10:00:01"},
            }

    class _ScreenshotService:
        def capture_summary_sheet(self, **kwargs):
            assert kwargs["duty_date"] == "2026-03-24"
            assert kwargs["duty_shift"] == "night"
            return {"status": "ok", "error": "", "path": "summary.png"}

        def capture_external_page(self, **kwargs):
            assert kwargs["duty_date"] == "2026-03-24"
            assert kwargs["duty_shift"] == "night"
            return {"status": "ok", "error": "", "path": "external.png"}

    monkeypatch.setattr(
        routes,
        "_build_daily_report_services",
        lambda _container: (_ReviewService(), object(), _AssetService(), _ScreenshotService()),
    )

    payload = routes.handover_daily_report_screenshot_test(
        request,
        {"duty_date": "2026-03-24", "duty_shift": "night"},
    )

    assert payload["ok"] is True
    assert payload["accepted"] is True
    assert payload["job"]["feature"] == "daily_report_screenshot_test"
    assert payload["job"]["result"]["status"] == "ok"
    assert "browser:controlled" in job_service.calls[0]["resource_keys"]
    assert "handover_batch:2026-03-24|night" in job_service.calls[0]["resource_keys"]

