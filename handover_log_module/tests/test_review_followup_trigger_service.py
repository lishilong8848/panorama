from __future__ import annotations

from copy import deepcopy

from handover_log_module.service.review_followup_trigger_service import ReviewFollowupTriggerService


def make_session(
    building: str,
    *,
    revision: int = 1,
    confirmed: bool = True,
    cloud_status: str = "pending_upload",
    synced_revision: int = 0,
) -> dict:
    session_id = f"{building}|2026-03-22|night"
    return {
        "session_id": session_id,
        "building": building,
        "batch_key": "2026-03-22|night",
        "duty_date": "2026-03-22",
        "duty_shift": "night",
        "output_file": f"D:\\handover\\{building}_交接班日志.xlsx",
        "data_file": f"D:\\source\\{building}_源数据.xlsx",
        "revision": revision,
        "confirmed": confirmed,
        "day_metric_export": {
            "status": "skipped",
            "reason": "already_uploaded",
            "uploaded_count": 0,
            "error": "",
            "uploaded_revision": revision,
            "metric_values_by_id": {},
            "metric_origin_context": {"by_metric_id": {}, "by_target_cell": {}},
        },
        "source_data_attachment_export": {
            "status": "skipped",
            "reason": "already_uploaded",
            "uploaded_count": 0,
            "error": "",
            "uploaded_revision": revision,
        },
        "cloud_sheet_sync": {
            "attempted": False,
            "success": False,
            "status": cloud_status,
            "spreadsheet_token": "sheet_token_1" if cloud_status != "prepare_failed" else "",
            "spreadsheet_url": "https://vnet.feishu.cn/wiki/wiki_token_1",
            "spreadsheet_title": "日报云文档",
            "sheet_title": building,
            "synced_revision": synced_revision,
            "last_attempt_revision": synced_revision,
            "prepared_at": "2026-03-22 02:00:00",
            "updated_at": "2026-03-22 02:00:00",
            "error": "" if cloud_status != "failed" else "upload failed",
            "synced_row_count": 59,
            "synced_column_count": 9,
            "synced_merges": [],
        },
    }


class FakeReviewService:
    def __init__(self, sessions: list[dict], *, ready: bool = True, batch_meta: dict | None = None) -> None:
        self.sessions = {item["building"]: deepcopy(item) for item in sessions}
        self.ready = ready
        self.batch_meta = deepcopy(batch_meta)
        self.day_updates: list[dict] = []
        self.attachment_updates: list[dict] = []
        self.cloud_updates: list[dict] = []
        self.register_calls: list[dict] = []

    def get_batch_status(self, batch_key: str) -> dict:
        return {
            "batch_key": batch_key,
            "confirmed_count": len(self.sessions) if self.ready else max(0, len(self.sessions) - 1),
            "required_count": len(self.sessions),
            "all_confirmed": self.ready,
            "ready_for_followup_upload": self.ready,
            "buildings": [
                {
                    "building": item["building"],
                    "has_session": True,
                    "confirmed": item["confirmed"],
                    "session_id": item["session_id"],
                    "revision": item["revision"],
                    "updated_at": item["cloud_sheet_sync"].get("updated_at", ""),
                    "cloud_sheet_sync": deepcopy(item["cloud_sheet_sync"]),
                }
                for item in self.list_batch_sessions(batch_key)
            ],
        }

    def list_batch_sessions(self, batch_key: str) -> list[dict]:
        return [deepcopy(self.sessions[key]) for key in sorted(self.sessions)]

    def get_latest_session(self, building: str) -> dict | None:
        session = self.sessions.get(building)
        return deepcopy(session) if session else None

    def get_session_by_id(self, session_id: str) -> dict | None:
        target = str(session_id or "").strip()
        for session in self.sessions.values():
            if str(session.get("session_id", "")).strip() == target:
                return deepcopy(session)
        return None

    def get_cloud_batch(self, batch_key: str) -> dict | None:
        if self.batch_meta and self.batch_meta.get("batch_key") == batch_key:
            return deepcopy(self.batch_meta)
        return None

    def is_first_full_cloud_sync_completed(self, batch_key: str) -> bool:
        if not self.batch_meta or self.batch_meta.get("batch_key") != batch_key:
            return False
        return bool(self.batch_meta.get("first_full_cloud_sync_completed", False))

    def mark_first_full_cloud_sync_completed(self, *, batch_key: str) -> dict | None:
        if not self.batch_meta or self.batch_meta.get("batch_key") != batch_key:
            return None
        self.batch_meta["first_full_cloud_sync_completed"] = True
        self.batch_meta["first_full_cloud_sync_at"] = "2026-03-24 10:00:00"
        return deepcopy(self.batch_meta)

    def register_cloud_batch(self, *, batch_key: str, duty_date: str, duty_shift: str, cloud_batch: dict) -> dict:
        normalized = {
            "batch_key": batch_key,
            "duty_date": duty_date,
            "duty_shift": duty_shift,
            **deepcopy(cloud_batch),
        }
        self.batch_meta = normalized
        self.register_calls.append(normalized)
        return deepcopy(normalized)

    def attach_cloud_batch_to_session(self, *, session_id: str, batch_key: str, building: str) -> dict:
        assert self.batch_meta is not None
        for session in self.sessions.values():
            if session["session_id"] != session_id:
                continue
            session["cloud_sheet_sync"].update(
                {
                    "status": "prepare_failed" if self.batch_meta.get("status") == "prepare_failed" else "pending_upload",
                    "spreadsheet_token": self.batch_meta.get("spreadsheet_token", ""),
                    "spreadsheet_url": self.batch_meta.get("spreadsheet_url", ""),
                    "spreadsheet_title": self.batch_meta.get("spreadsheet_title", ""),
                    "prepared_at": self.batch_meta.get("prepared_at", ""),
                    "sheet_title": building,
                    "error": self.batch_meta.get("error", "") if self.batch_meta.get("status") == "prepare_failed" else "",
                }
            )
            return deepcopy(session)
        raise AssertionError(f"unknown session_id: {session_id}")

    def update_cloud_sheet_sync(self, *, session_id: str, cloud_sheet_sync: dict) -> dict:
        for session in self.sessions.values():
            if session["session_id"] == session_id:
                session["cloud_sheet_sync"] = deepcopy(cloud_sheet_sync)
                self.cloud_updates.append({"session_id": session_id, "cloud_sheet_sync": deepcopy(cloud_sheet_sync)})
                return deepcopy(session)
        raise AssertionError(f"unknown session_id: {session_id}")

    def update_day_metric_export(self, *, session_id: str, day_metric_export: dict) -> dict:
        self.day_updates.append({"session_id": session_id, "state": deepcopy(day_metric_export)})
        return {}

    def update_source_data_attachment_export(self, *, session_id: str, source_data_attachment_export: dict) -> dict:
        self.attachment_updates.append({"session_id": session_id, "state": deepcopy(source_data_attachment_export)})
        return {}


class FakeCloudSyncService:
    def __init__(self) -> None:
        self.prepare_calls: list[dict] = []
        self.sync_calls: list[dict] = []
        self.validate_calls: list[dict] = []
        self.next_validate_result = {"valid": True, "error": ""}
        self.next_prepare_result = {
            "attempted": True,
            "success": True,
            "status": "prepared",
            "spreadsheet_token": "sheet_token_1",
            "spreadsheet_url": "https://vnet.feishu.cn/wiki/wiki_token_1",
            "spreadsheet_title": "日报云文档",
            "prepared_at": "2026-03-22 02:00:00",
            "updated_at": "2026-03-22 02:00:00",
            "error": "",
        }
        self.next_sync_result = {
            "status": "ok",
            "spreadsheet_token": "sheet_token_1",
            "spreadsheet_url": "https://vnet.feishu.cn/wiki/wiki_token_1",
            "spreadsheet_title": "日报云文档",
            "uploaded_buildings": [],
            "skipped_buildings": [],
            "failed_buildings": [],
            "details": {},
        }

    def prepare_batch_spreadsheet(self, **kwargs):
        self.prepare_calls.append(deepcopy(kwargs))
        return deepcopy(self.next_prepare_result)

    def validate_batch_spreadsheet(self, *, batch_meta: dict, emit_log=print):
        self.validate_calls.append({"batch_meta": deepcopy(batch_meta)})
        return deepcopy(self.next_validate_result)

    def sync_confirmed_buildings(self, *, batch_meta: dict, building_items: list[dict], emit_log=print):
        self.sync_calls.append({"batch_meta": deepcopy(batch_meta), "building_items": deepcopy(building_items)})
        result = deepcopy(self.next_sync_result)
        if not result["uploaded_buildings"] and not result["failed_buildings"] and not result["skipped_buildings"]:
            result["uploaded_buildings"] = [item["building"] for item in building_items]
            result["details"] = {
                item["building"]: {
                    "status": "success",
                    "sheet_title": item["building"],
                    "synced_revision": item["revision"],
                    "rows": 59,
                    "cols": 9,
                    "merged": 70,
                    "synced_row_count": 59,
                    "synced_column_count": 9,
                    "synced_merges": [],
                    "error": "",
                }
                for item in building_items
            }
        return result


class FakeDayMetricExportService:
    def __init__(self) -> None:
        self.calls = []

    def rewrite_from_output_file(self, **kwargs):
        self.calls.append(deepcopy(kwargs))
        return {
            "status": "skipped",
            "reason": "already_uploaded",
            "uploaded_count": 0,
            "created_records": 0,
            "deleted_records": 0,
            "error": "",
        }

    def run_from_output_file(self, **kwargs):
        return self.rewrite_from_output_file(**kwargs)


class FakeSourceAttachmentExportService:
    def __init__(self) -> None:
        self.calls = []

    def build_deferred_state(self, *, duty_shift: str) -> dict:  # noqa: ARG002
        return {"status": "skipped", "reason": "disabled"}

    def list_existing_records(self, emit_log=print):  # noqa: ARG002
        return []

    def run_from_source_file(self, **kwargs):
        self.calls.append(deepcopy(kwargs))
        return {"status": "skipped", "reason": "already_uploaded", "uploaded_count": 0, "error": ""}


class FakeReviewDocumentStateService:
    def __init__(self) -> None:
        self.force_sync_calls: list[dict] = []

    def force_sync_session_dict(self, session: dict, *, reason: str = "") -> dict:
        self.force_sync_calls.append({"session_id": session.get("session_id"), "reason": reason})
        return {
            "status": "synced",
            "synced_revision": int(session.get("revision", 0) or 0),
            "pending_revision": 0,
            "error": "",
            "updated_at": "2026-03-22 02:00:00",
        }


def build_trigger(review_service: FakeReviewService, cloud_service: FakeCloudSyncService) -> ReviewFollowupTriggerService:
    trigger = ReviewFollowupTriggerService({"network": {"enable_auto_switch_wifi": False}})
    trigger._review_service = review_service  # type: ignore[attr-defined]
    trigger._cloud_sheet_sync_service = cloud_service  # type: ignore[attr-defined]
    trigger._review_document_state_service = FakeReviewDocumentStateService()  # type: ignore[attr-defined]
    trigger._day_metric_export_service = FakeDayMetricExportService()  # type: ignore[attr-defined]
    trigger._source_data_attachment_export_service = FakeSourceAttachmentExportService()  # type: ignore[attr-defined]
    trigger._daily_report_state_service = FakeDailyReportStateService()  # type: ignore[attr-defined]
    trigger._daily_report_asset_service = FakeDailyReportAssetService()  # type: ignore[attr-defined]
    trigger._daily_report_screenshot_service = FakeDailyReportScreenshotService()  # type: ignore[attr-defined]
    trigger._daily_report_bitable_export_service = FakeDailyReportBitableExportService()  # type: ignore[attr-defined]
    return trigger


def test_build_cloud_items_only_returns_buildings_with_outdated_cloud_sync() -> None:
    review_service = FakeReviewService(
        [
            make_session("A楼", revision=3, cloud_status="success", synced_revision=3),
            make_session("B楼", revision=2, cloud_status="success", synced_revision=1),
            make_session("C楼", revision=1, cloud_status="disabled", synced_revision=0),
        ]
    )
    trigger = build_trigger(review_service, FakeCloudSyncService())

    upload_items, skipped_buildings, failed_buildings = trigger._build_cloud_items(
        review_service.list_batch_sessions("2026-03-22|night")
    )

    assert [item["building"] for item in upload_items] == ["B楼"]
    assert failed_buildings == []
    assert skipped_buildings == [
        {"building": "A楼", "reason": "already_uploaded"},
        {"building": "C楼", "reason": "disabled"},
    ]


class FakeDailyReportStateService:
    def __init__(self) -> None:
        self.updated = []

    def get_export_state(self, **kwargs):  # noqa: ANN003
        if self.updated:
            return deepcopy(self.updated[-1].get("daily_report_record_export", {}))
        return {
            "status": "success",
            "record_id": "rec_daily",
            "record_url": "",
            "spreadsheet_url": "https://vnet.feishu.cn/wiki/wiki_token_1",
            "summary_screenshot_path": "summary_sheet.png",
            "external_screenshot_path": "external_page.png",
            "summary_screenshot_source_used": "auto",
            "external_screenshot_source_used": "auto",
            "updated_at": "2026-03-24 10:00:00",
            "error": "",
            "error_code": "",
            "error_detail": "",
        }

    def update_export_state(self, **kwargs):
        self.updated.append(deepcopy(kwargs))
        payload = deepcopy(kwargs.get("daily_report_record_export", {}))
        payload.setdefault("updated_at", "2026-03-24 10:00:00")
        return payload


class FakeDailyReportAssetService:
    def prune_stale_assets(self):
        return 0

    def get_capture_assets_context(self, *, duty_date, duty_shift):  # noqa: ARG002
        return {
            "summary_sheet_image": {
                "exists": True,
                "source": "auto",
                "stored_path": "summary_sheet.png",
                "captured_at": "2026-03-24 10:00:00",
                "preview_url": "",
                "auto": {
                    "exists": True,
                    "stored_path": "summary_sheet.png",
                    "captured_at": "2026-03-24 10:00:00",
                    "preview_url": "",
                },
                "manual": {"exists": False, "stored_path": "", "captured_at": "", "preview_url": ""},
            },
            "external_page_image": {
                "exists": True,
                "source": "auto",
                "stored_path": "external_page.png",
                "captured_at": "2026-03-24 10:00:01",
                "preview_url": "",
                "auto": {
                    "exists": True,
                    "stored_path": "external_page.png",
                    "captured_at": "2026-03-24 10:00:01",
                    "preview_url": "",
                },
                "manual": {"exists": False, "stored_path": "", "captured_at": "", "preview_url": ""},
            },
        }


class FakeManualPreferredDailyReportAssetService(FakeDailyReportAssetService):
    def get_capture_assets_context(self, *, duty_date, duty_shift):  # noqa: ARG002
        payload = super().get_capture_assets_context(duty_date=duty_date, duty_shift=duty_shift)
        payload["summary_sheet_image"].update(
            {
                "source": "manual",
                "stored_path": "summary_sheet_manual.png",
                "captured_at": "2026-03-24 10:10:00",
            }
        )
        payload["summary_sheet_image"]["manual"] = {
            "exists": True,
            "stored_path": "summary_sheet_manual.png",
            "captured_at": "2026-03-24 10:10:00",
            "preview_url": "",
        }
        payload["external_page_image"].update(
            {
                "source": "manual",
                "stored_path": "external_page_manual.png",
                "captured_at": "2026-03-24 10:10:01",
            }
        )
        payload["external_page_image"]["manual"] = {
            "exists": True,
            "stored_path": "external_page_manual.png",
            "captured_at": "2026-03-24 10:10:01",
            "preview_url": "",
        }
        return payload


class FakeDailyReportScreenshotService:
    def __init__(self) -> None:
        self.auth_checks = 0
        self.summary_calls = []
        self.external_calls = []

    def check_auth_status(self, emit_log=print):  # noqa: ARG002
        self.auth_checks += 1
        return {"status": "ready", "error": "", "profile_dir": "profile", "last_checked_at": "2026-03-24 10:00:00"}

    def capture_summary_sheet(self, **kwargs):
        self.summary_calls.append(deepcopy(kwargs))
        return {"status": "ok", "path": "summary_sheet.png", "error": ""}

    def capture_external_page(self, **kwargs):
        self.external_calls.append(deepcopy(kwargs))
        return {"status": "ok", "path": "external_page.png", "error": ""}


class FakeDailyReportBitableExportService:
    def __init__(self) -> None:
        self.calls = []

    def export_record(self, **kwargs):
        self.calls.append(deepcopy(kwargs))
        return {"status": "success", "record_id": "rec_daily", "record_url": "", "error": ""}


def prepared_batch_meta() -> dict:
    return {
        "batch_key": "2026-03-22|night",
        "duty_date": "2026-03-22",
        "duty_shift": "night",
        "status": "prepared",
        "spreadsheet_token": "sheet_token_1",
        "spreadsheet_url": "https://vnet.feishu.cn/wiki/wiki_token_1",
        "spreadsheet_title": "日报云文档",
        "prepared_at": "2026-03-22 02:00:00",
        "updated_at": "2026-03-22 02:00:00",
        "error": "",
        "first_full_cloud_sync_completed": False,
        "first_full_cloud_sync_at": "",
    }


def test_trigger_batch_runs_daily_report_export_after_cloud_success() -> None:
    sessions = [make_session("A")]
    review_service = FakeReviewService(sessions, ready=True, batch_meta=prepared_batch_meta())
    cloud_service = FakeCloudSyncService()
    trigger = build_trigger(review_service, cloud_service)
    trigger._daily_report_state_service = FakeDailyReportStateService()  # type: ignore[attr-defined]
    trigger._daily_report_asset_service = FakeDailyReportAssetService()  # type: ignore[attr-defined]
    trigger._daily_report_screenshot_service = FakeDailyReportScreenshotService()  # type: ignore[attr-defined]
    trigger._daily_report_bitable_export_service = FakeDailyReportBitableExportService()  # type: ignore[attr-defined]

    result = trigger.trigger_batch("2026-03-22|night", emit_log=lambda *_args, **_kwargs: None)

    assert result["status"] == "ok"
    assert result["cloud_sheet_sync"]["status"] == "ok"
    assert result["daily_report_record_export"]["status"] == "success"
    assert result["daily_report_record_export"]["summary_screenshot_source_used"] == "auto"
    assert result["daily_report_record_export"]["external_screenshot_source_used"] == "auto"
    assert trigger._daily_report_bitable_export_service.calls[0]["spreadsheet_url"] == "https://vnet.feishu.cn/wiki/wiki_token_1"  # type: ignore[attr-defined]
    assert review_service.batch_meta["first_full_cloud_sync_completed"] is True


def test_followup_attachment_export_falls_back_to_cached_source_file() -> None:
    sessions = [make_session("C")]
    sessions[0]["data_file"] = ""
    sessions[0]["source_file_cache"] = {
        "managed": True,
        "stored_path": r"D:\managed\C楼_源数据.xlsx",
        "original_name": "C楼_源数据.xlsx",
        "stored_at": "2026-03-22 02:00:00",
        "cleanup_status": "active",
        "cleanup_at": "",
    }
    sessions[0]["source_data_attachment_export"] = {
        "status": "failed",
        "reason": "missing_source_file",
        "uploaded_count": 0,
        "error": "missing",
        "uploaded_at": "",
        "uploaded_revision": 0,
    }
    review_service = FakeReviewService(sessions, ready=True, batch_meta=prepared_batch_meta())
    cloud_service = FakeCloudSyncService()
    trigger = build_trigger(review_service, cloud_service)

    result = trigger._run_session_followup_exports(  # type: ignore[attr-defined]
        batch_key="2026-03-22|night",
        sessions=review_service.list_batch_sessions("2026-03-22|night"),
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert result["failed_buildings"] == []
    assert trigger._source_data_attachment_export_service.calls[0]["data_file"] == r"D:\managed\C楼_源数据.xlsx"  # type: ignore[attr-defined]


def test_trigger_batch_does_not_run_day_metric_rewrite_from_review_followup() -> None:
    sessions = [make_session("A")]
    sessions[0]["day_metric_export"]["status"] = "failed"
    sessions[0]["day_metric_export"]["uploaded_revision"] = 0
    sessions[0]["day_metric_export"]["metric_origin_context"] = {
        "by_metric_id": {"cold_temp_max": {"b_norm": "E-301", "c_norm": "C3-2"}},
        "by_target_cell": {"D6": {"metric_key": "city_power", "b_norm": "A-401", "c_norm": ""}},
    }
    review_service = FakeReviewService(sessions, ready=True, batch_meta=prepared_batch_meta())
    cloud_service = FakeCloudSyncService()
    trigger = build_trigger(review_service, cloud_service)

    result = trigger.trigger_batch("2026-03-22|night", emit_log=lambda *_args, **_kwargs: None)

    assert result["status"] == "ok"
    assert len(trigger._day_metric_export_service.calls) == 0  # type: ignore[attr-defined]


def test_trigger_batch_daily_report_export_prefers_manual_assets() -> None:
    sessions = [make_session("A")]
    review_service = FakeReviewService(sessions, ready=True, batch_meta=prepared_batch_meta())
    cloud_service = FakeCloudSyncService()
    trigger = build_trigger(review_service, cloud_service)
    trigger._daily_report_state_service = FakeDailyReportStateService()  # type: ignore[attr-defined]
    trigger._daily_report_asset_service = FakeManualPreferredDailyReportAssetService()  # type: ignore[attr-defined]
    trigger._daily_report_screenshot_service = FakeDailyReportScreenshotService()  # type: ignore[attr-defined]
    trigger._daily_report_bitable_export_service = FakeDailyReportBitableExportService()  # type: ignore[attr-defined]

    result = trigger.trigger_batch("2026-03-22|night", emit_log=lambda *_args, **_kwargs: None)

    assert result["daily_report_record_export"]["status"] == "success"
    assert result["daily_report_record_export"]["summary_screenshot_source_used"] == "manual"
    assert result["daily_report_record_export"]["external_screenshot_source_used"] == "manual"
    assert trigger._daily_report_bitable_export_service.calls[0]["summary_screenshot_path"] == "summary_sheet_manual.png"  # type: ignore[attr-defined]
    assert trigger._daily_report_bitable_export_service.calls[0]["external_screenshot_path"] == "external_page_manual.png"  # type: ignore[attr-defined]


def test_trigger_batch_skips_daily_report_when_cloud_not_ok() -> None:
    sessions = [make_session("A")]
    review_service = FakeReviewService(sessions, ready=True, batch_meta=prepared_batch_meta())
    cloud_service = FakeCloudSyncService()
    cloud_service.next_sync_result = {
        "status": "partial_failed",
        "spreadsheet_token": "sheet_token_1",
        "spreadsheet_url": "https://vnet.feishu.cn/wiki/wiki_token_1",
        "spreadsheet_title": "日报",
        "uploaded_buildings": [],
        "skipped_buildings": [],
        "failed_buildings": [{"building": "A", "error": "limit"}],
        "details": {},
    }
    trigger = build_trigger(review_service, cloud_service)
    trigger._daily_report_state_service = FakeDailyReportStateService()  # type: ignore[attr-defined]
    trigger._daily_report_asset_service = FakeDailyReportAssetService()  # type: ignore[attr-defined]
    trigger._daily_report_screenshot_service = FakeDailyReportScreenshotService()  # type: ignore[attr-defined]
    trigger._daily_report_bitable_export_service = FakeDailyReportBitableExportService()  # type: ignore[attr-defined]
    logs: list[str] = []

    result = trigger.trigger_batch("2026-03-22|night", emit_log=logs.append)

    assert result["daily_report_record_export"]["status"] == "skipped_due_to_cloud_sync_not_ok"
    assert result["daily_report_record_export"]["error"] == "云表同步状态不是成功: 失败"
    assert trigger._daily_report_bitable_export_service.calls == []  # type: ignore[attr-defined]
    assert any("云表同步状态=失败" in message for message in logs)


def test_trigger_batch_blocks_before_all_confirmed() -> None:
    sessions = [make_session("A")]
    review_service = FakeReviewService(sessions, ready=False)
    cloud_service = FakeCloudSyncService()
    trigger = build_trigger(review_service, cloud_service)

    result = trigger.trigger_batch("2026-03-22|night", emit_log=lambda *_args: None)

    assert result["status"] == "blocked"
    assert result["cloud_sheet_sync"]["status"] == "blocked"
    assert cloud_service.sync_calls == []


def test_trigger_batch_uploads_all_confirmed_buildings_once() -> None:
    sessions = [
        make_session("A", revision=2, synced_revision=0),
        make_session("B", revision=3, synced_revision=0),
    ]
    review_service = FakeReviewService(sessions, ready=True, batch_meta=prepared_batch_meta())
    cloud_service = FakeCloudSyncService()
    trigger = build_trigger(review_service, cloud_service)

    result = trigger.trigger_batch("2026-03-22|night", emit_log=lambda *_args: None)

    assert result["status"] == "ok"
    assert result["cloud_sheet_sync"]["status"] == "ok"
    assert cloud_service.sync_calls[0]["building_items"] == [
        {"building": "A", "output_file": "D:\\handover\\A_交接班日志.xlsx", "revision": 2},
        {"building": "B", "output_file": "D:\\handover\\B_交接班日志.xlsx", "revision": 3},
    ]
    assert review_service.get_latest_session("A")["cloud_sheet_sync"]["status"] == "success"
    assert review_service.get_latest_session("A")["cloud_sheet_sync"]["synced_revision"] == 2
    assert review_service.get_latest_session("B")["cloud_sheet_sync"]["synced_revision"] == 3
    assert review_service.batch_meta["first_full_cloud_sync_completed"] is True


def test_trigger_after_single_confirm_uses_single_building_cloud_sync_after_first_full_upload() -> None:
    sessions = [
        make_session("A", revision=3, synced_revision=2),
        make_session("B", revision=2, cloud_status="success", synced_revision=2),
    ]
    sessions[0]["source_data_attachment_export"]["uploaded_revision"] = 2
    sessions[0]["source_data_attachment_export"]["frozen_after_first_full_cloud_sync"] = True
    batch_meta = prepared_batch_meta()
    batch_meta["first_full_cloud_sync_completed"] = True
    batch_meta["first_full_cloud_sync_at"] = "2026-03-24 09:00:00"
    review_service = FakeReviewService(sessions, ready=True, batch_meta=batch_meta)
    cloud_service = FakeCloudSyncService()
    trigger = build_trigger(review_service, cloud_service)
    trigger._daily_report_state_service = FakeDailyReportStateService()  # type: ignore[attr-defined]
    trigger._daily_report_screenshot_service = FakeDailyReportScreenshotService()  # type: ignore[attr-defined]
    trigger._daily_report_bitable_export_service = FakeDailyReportBitableExportService()  # type: ignore[attr-defined]

    result = trigger.trigger_after_single_confirm(
        batch_key="2026-03-22|night",
        building="A",
        session_id="A|2026-03-22|night",
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert result["status"] == "ok"
    assert result["cloud_sheet_sync"]["uploaded_buildings"] == ["A"]
    assert len(cloud_service.sync_calls) == 1
    assert cloud_service.sync_calls[0]["building_items"] == [
        {"building": "A", "output_file": "D:\\handover\\A_交接班日志.xlsx", "revision": 3},
    ]
    assert trigger._source_data_attachment_export_service.calls == []  # type: ignore[attr-defined]
    assert trigger._daily_report_screenshot_service.summary_calls == []  # type: ignore[attr-defined]
    assert trigger._daily_report_bitable_export_service.calls == []  # type: ignore[attr-defined]
    assert result["daily_report_record_export"]["status"] == "success"
    assert result["followup_progress"]["attachment_pending_count"] == 0


def test_trigger_after_single_confirm_waits_until_all_buildings_confirmed_before_first_full_upload() -> None:
    sessions = [make_session("A")]
    review_service = FakeReviewService(sessions, ready=False, batch_meta=prepared_batch_meta())
    cloud_service = FakeCloudSyncService()
    trigger = build_trigger(review_service, cloud_service)
    trigger._daily_report_state_service = FakeDailyReportStateService()  # type: ignore[attr-defined]
    trigger._daily_report_screenshot_service = FakeDailyReportScreenshotService()  # type: ignore[attr-defined]
    trigger._daily_report_bitable_export_service = FakeDailyReportBitableExportService()  # type: ignore[attr-defined]

    result = trigger.trigger_after_single_confirm(
        batch_key="2026-03-22|night",
        building="A",
        session_id="A|2026-03-22|night",
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert result["status"] == "await_all_confirmed"
    assert trigger._source_data_attachment_export_service.calls == []  # type: ignore[attr-defined]
    assert len(trigger._daily_report_screenshot_service.summary_calls) == 0  # type: ignore[attr-defined]
    assert len(trigger._daily_report_bitable_export_service.calls) == 0  # type: ignore[attr-defined]


def test_trigger_after_single_confirm_runs_full_trigger_when_all_buildings_just_confirmed() -> None:
    sessions = [make_session("A")]
    review_service = FakeReviewService(sessions, ready=True, batch_meta=prepared_batch_meta())
    cloud_service = FakeCloudSyncService()
    trigger = build_trigger(review_service, cloud_service)
    trigger._daily_report_state_service = FakeDailyReportStateService()  # type: ignore[attr-defined]
    trigger._daily_report_screenshot_service = FakeDailyReportScreenshotService()  # type: ignore[attr-defined]
    trigger._daily_report_bitable_export_service = FakeDailyReportBitableExportService()  # type: ignore[attr-defined]

    result = trigger.trigger_after_single_confirm(
        batch_key="2026-03-22|night",
        building="A",
        session_id="A|2026-03-22|night",
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert result["status"] == "ok"
    assert len(trigger._daily_report_screenshot_service.summary_calls) == 1  # type: ignore[attr-defined]
    assert len(trigger._daily_report_bitable_export_service.calls) == 1  # type: ignore[attr-defined]


def test_retry_failed_cloud_sheet_in_batch_only_retries_failed_buildings() -> None:
    sessions = [
        make_session("A", revision=2, cloud_status="success", synced_revision=2),
        make_session("B", revision=4, cloud_status="failed", synced_revision=0),
        make_session("C", revision=5, cloud_status="prepare_failed", synced_revision=0),
        make_session("D", revision=1, cloud_status="pending_upload", synced_revision=0),
        make_session("E", revision=1, cloud_status="disabled", synced_revision=0),
    ]
    review_service = FakeReviewService(sessions, ready=True, batch_meta=prepared_batch_meta())
    cloud_service = FakeCloudSyncService()
    cloud_service.next_sync_result = {
        "status": "partial_failed",
        "spreadsheet_token": "sheet_token_1",
        "spreadsheet_url": "https://vnet.feishu.cn/wiki/wiki_token_1",
        "spreadsheet_title": "日报云文档",
        "uploaded_buildings": ["B"],
        "skipped_buildings": [],
        "failed_buildings": [{"building": "C", "error": "upload failed"}],
        "details": {
            "B": {
                "status": "success",
                "sheet_title": "B",
                "synced_revision": 4,
                "rows": 59,
                "cols": 9,
                "merged": 70,
                "synced_row_count": 59,
                "synced_column_count": 9,
                "synced_merges": [],
                "error": "",
            },
            "C": {
                "status": "failed",
                "sheet_title": "C",
                "synced_revision": 0,
                "rows": 0,
                "cols": 0,
                "merged": 0,
                "synced_row_count": 0,
                "synced_column_count": 0,
                "synced_merges": [],
                "error": "upload failed",
            },
        },
    }
    trigger = build_trigger(review_service, cloud_service)

    result = trigger.retry_failed_cloud_sheet_in_batch("2026-03-22|night", emit_log=lambda *_args: None)

    assert result["status"] == "partial_failed"
    assert cloud_service.sync_calls[0]["building_items"] == [
        {"building": "B", "output_file": "D:\\handover\\B_交接班日志.xlsx", "revision": 4},
        {"building": "C", "output_file": "D:\\handover\\C_交接班日志.xlsx", "revision": 5},
    ]
    assert review_service.get_latest_session("A")["cloud_sheet_sync"]["status"] == "success"
    assert review_service.get_latest_session("B")["cloud_sheet_sync"]["status"] == "success"
    assert review_service.get_latest_session("C")["cloud_sheet_sync"]["status"] == "failed"


def test_retry_failed_cloud_sheet_in_batch_does_not_run_other_followups() -> None:
    sessions = [make_session("A")]
    review_service = FakeReviewService(sessions, ready=True, batch_meta=prepared_batch_meta())
    cloud_service = FakeCloudSyncService()
    trigger = build_trigger(review_service, cloud_service)

    result = trigger.retry_failed_cloud_sheet_in_batch("2026-03-22|night", emit_log=lambda *_args: None)

    assert result["status"] == "skipped"
    assert review_service.day_updates == []
    assert review_service.attachment_updates == []


def test_trigger_batch_prepares_cloud_batch_when_missing() -> None:
    sessions = [make_session("A")]
    review_service = FakeReviewService(sessions, ready=True, batch_meta=None)
    cloud_service = FakeCloudSyncService()
    trigger = build_trigger(review_service, cloud_service)

    result = trigger.trigger_batch("2026-03-22|night", emit_log=lambda *_args: None)

    assert result["status"] == "ok"
    assert len(cloud_service.prepare_calls) == 1
    assert review_service.batch_meta["status"] == "prepared"


def test_trigger_batch_recreates_cloud_batch_when_cached_token_is_stale() -> None:
    sessions = [make_session("A")]
    stale_meta = prepared_batch_meta()
    stale_meta["spreadsheet_token"] = "stale_token"
    stale_meta["spreadsheet_url"] = "https://vnet.feishu.cn/wiki/stale_token"
    review_service = FakeReviewService(sessions, ready=True, batch_meta=stale_meta)
    cloud_service = FakeCloudSyncService()
    cloud_service.next_validate_result = {"valid": False, "error": "spreadsheet_deleted"}
    cloud_service.next_prepare_result["spreadsheet_token"] = "sheet_token_2"
    cloud_service.next_prepare_result["spreadsheet_url"] = "https://vnet.feishu.cn/wiki/wiki_token_2"
    cloud_service.next_sync_result["spreadsheet_token"] = "sheet_token_2"
    cloud_service.next_sync_result["spreadsheet_url"] = "https://vnet.feishu.cn/wiki/wiki_token_2"
    trigger = build_trigger(review_service, cloud_service)

    result = trigger.trigger_batch("2026-03-22|night", emit_log=lambda *_args: None)

    assert result["status"] == "ok"
    assert len(cloud_service.validate_calls) == 1
    assert len(cloud_service.prepare_calls) == 1
    assert review_service.batch_meta["spreadsheet_token"] == "sheet_token_2"
    assert cloud_service.sync_calls[0]["batch_meta"]["spreadsheet_token"] == "sheet_token_2"
    assert review_service.get_latest_session("A")["cloud_sheet_sync"]["spreadsheet_token"] == "sheet_token_2"


def test_trigger_batch_does_not_switch_network_before_followups() -> None:
    sessions = [make_session("A")]
    review_service = FakeReviewService(sessions, ready=True, batch_meta=prepared_batch_meta())
    cloud_service = FakeCloudSyncService()
    trigger = build_trigger(review_service, cloud_service)
    switch_calls: list[bool] = []
    logs: list[str] = []

    def _fake_ensure_external_network(_emit_log):
        switch_calls.append(True)
        return True

    trigger._ensure_external_network = _fake_ensure_external_network  # type: ignore[method-assign]

    result = trigger.trigger_batch("2026-03-22|night", emit_log=logs.append)

    assert result["status"] == "ok"
    assert switch_calls == []
    assert len(cloud_service.sync_calls) == 1
    assert any("已跳过自动切回外网" in message for message in logs)


def test_retry_failed_cloud_sheet_in_batch_keeps_network_switch_behavior() -> None:
    sessions = [make_session("A")]
    review_service = FakeReviewService(sessions, ready=True, batch_meta=prepared_batch_meta())
    cloud_service = FakeCloudSyncService()
    trigger = build_trigger(review_service, cloud_service)
    switch_calls: list[bool] = []

    def _fake_ensure_external_network(_emit_log):
        switch_calls.append(True)
        return True

    trigger._ensure_external_network = _fake_ensure_external_network  # type: ignore[method-assign]

    result = trigger.retry_failed_cloud_sheet_in_batch("2026-03-22|night", emit_log=lambda *_args: None)

    assert result["status"] == "skipped"
    assert switch_calls == []


def test_force_update_cloud_sheet_for_session_skips_gate_and_network_switch() -> None:
    sessions = [make_session("A")]
    review_service = FakeReviewService(sessions, ready=False, batch_meta=prepared_batch_meta())
    cloud_service = FakeCloudSyncService()
    trigger = build_trigger(review_service, cloud_service)
    switch_calls: list[bool] = []
    logs: list[str] = []

    def _fake_ensure_external_network(_emit_log):
        switch_calls.append(True)
        return True

    trigger._ensure_external_network = _fake_ensure_external_network  # type: ignore[method-assign]

    result = trigger.force_update_cloud_sheet_for_session("A|2026-03-22|night", emit_log=logs.append)

    assert result["status"] == "ok"
    assert switch_calls == []
    assert len(cloud_service.sync_calls) == 1
    assert cloud_service.sync_calls[0]["building_items"] == [
        {"building": "A", "output_file": sessions[0]["output_file"], "revision": 1},
    ]
    assert review_service.day_updates == []
    assert review_service.attachment_updates == []
    assert any("历史云表更新" in message for message in logs)
    assert any("状态=成功" in message for message in logs)

