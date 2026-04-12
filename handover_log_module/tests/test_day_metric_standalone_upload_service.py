from __future__ import annotations

import json
from pathlib import Path

from handover_log_module.service import day_metric_standalone_upload_service as module


class _FakeNotifyService:
    calls: list[dict] = []

    def __init__(self, cfg):  # noqa: ANN001
        self.cfg = cfg

    def send_failure(self, *, stage, detail, building="", emit_log=None, category="upload"):  # noqa: ANN001
        self.__class__.calls.append(
            {
                "stage": stage,
                "detail": detail,
                "building": building,
                "category": category,
            }
        )


class _FakeDownloadService:
    prepare_internal_calls = 0
    ensure_internal_calls = 0
    ensure_external_calls = 0
    run_calls: list[dict] = []
    workflow_log: list[str] = []
    failed_buildings_by_date: dict[str, set[str]] = {}

    def __init__(self, cfg):  # noqa: ANN001
        self.cfg = cfg

    def prepare_internal_for_batch_download(self, emit_log):  # noqa: ANN001
        self.__class__.prepare_internal_calls += 1
        return True

    def ensure_internal_ready(self, emit_log):  # noqa: ANN001
        self.__class__.ensure_internal_calls += 1
        return True

    def ensure_external_ready(self, emit_log):  # noqa: ANN001
        self.__class__.ensure_external_calls += 1
        return True

    def run(
        self,
        *,
        buildings,
        start_time=None,
        end_time=None,
        duty_date,
        duty_shift,
        switch_network=True,
        reuse_cached=True,
        emit_log,  # noqa: ANN001
    ):
        buildings_list = [str(item or "").strip() for item in (buildings or []) if str(item or "").strip()]
        self.__class__.run_calls.append(
            {
                "buildings": buildings_list,
                "start_time": start_time,
                "end_time": end_time,
                "duty_date": duty_date,
                "duty_shift": duty_shift,
                "switch_network": switch_network,
                "reuse_cached": reuse_cached,
            }
        )
        self.__class__.workflow_log.append(f"download:{duty_date}")
        failed_buildings = self.__class__.failed_buildings_by_date.get(str(duty_date), set())
        success_files = []
        failed = []
        for building in buildings_list:
            if building in failed_buildings:
                failed.append({"building": building, "error": "download_failed"})
                continue
            runtime_root = Path.cwd() / ".tmp_day_metric_sources"
            runtime_root.mkdir(parents=True, exist_ok=True)
            source_path = runtime_root / f"{building}_{duty_date}_{duty_shift}.xlsx"
            source_path.write_bytes(b"demo")
            success_files.append(
                {
                    "building": building,
                    "file_path": str(source_path),
                }
            )
        return {"success_files": success_files, "failed": failed}


class _FakeSourceCalcService:
    calls: list[dict] = []

    def __init__(self, cfg):  # noqa: ANN001
        self.cfg = cfg

    def calculate(
        self,
        *,
        building,
        data_file,
        duty_date,
    ):
        self.__class__.calls.append(
            {
                "building": building,
                "data_file": data_file,
                "duty_date": duty_date,
            }
        )
        return {
            "resolved_metrics": {"city_power": 11, "it_power": 22},
            "metric_origin_context": {
                "by_metric_id": {
                    "city_power": {
                        "metric_key": "city_power",
                        "row_index": 12,
                        "b_norm": "A-401",
                        "c_norm": "",
                        "b_text": "A-401",
                        "c_text": "R01",
                        "d_name": "总负荷",
                    },
                    "it_power": {
                        "metric_key": "it_power",
                        "row_index": 13,
                        "b_norm": "A-402",
                        "c_norm": "",
                        "b_text": "A-402",
                        "c_text": "R02",
                        "d_name": "IT总负荷",
                    },
                }
            },
        }


class _FakeSourceDataAttachmentExportService:
    calls: list[dict] = []
    workflow_log: list[str] = []
    remaining_failures_by_key: dict[tuple[str, str], int] = {}

    def __init__(self, cfg):  # noqa: ANN001
        self.cfg = cfg

    def run_from_source_file(self, *, building, duty_date, duty_shift, data_file, emit_log, existing_records=None):  # noqa: ANN001
        key = (str(duty_date).strip(), str(building).strip())
        self.__class__.calls.append(
            {
                "building": building,
                "duty_date": duty_date,
                "duty_shift": duty_shift,
                "data_file": data_file,
                "existing_records": existing_records,
            }
        )
        self.__class__.workflow_log.append(f"attachment:{duty_date}:{building}")
        remaining = int(self.__class__.remaining_failures_by_key.get(key, 0) or 0)
        if remaining > 0:
            self.__class__.remaining_failures_by_key[key] = remaining - 1
            return {"status": "failed", "error": "attachment_failed", "uploaded_count": 0}
        return {"status": "ok", "error": "", "uploaded_count": 1}


class _FakeDayMetricExportService:
    run_calls: list[dict] = []
    workflow_log: list[str] = []
    remaining_failures_by_key: dict[tuple[str, str], int] = {}

    def __init__(self, cfg):  # noqa: ANN001
        self.cfg = cfg

    def run(self, **kwargs):
        self.__class__.run_calls.append(dict(kwargs))
        duty_date = str(kwargs.get("duty_date", "")).strip()
        building = str(kwargs.get("building", "")).strip()
        self.__class__.workflow_log.append(f"upload:{duty_date}:{building}")
        key = (duty_date, building)
        remaining = int(self.__class__.remaining_failures_by_key.get(key, 0) or 0)
        if remaining > 0:
            self.__class__.remaining_failures_by_key[key] = remaining - 1
            return {
                "status": "failed",
                "error": "upload_failed",
                "created_records": 0,
                "deleted_records": 0,
            }
        return {
            "status": "ok",
            "error": "",
            "created_records": 12,
            "deleted_records": 12,
        }

    def rewrite_from_output_file(self, **kwargs):  # noqa: ANN001
        raise AssertionError("standalone upload should not call rewrite_from_output_file")


def _runtime_cfg(tmp_path: Path, *, auto_switch: bool = True):
    return {
        "paths": {"runtime_state_root": str(tmp_path / ".runtime")},
        "network": {"enable_auto_switch_wifi": auto_switch},
        "common": {
            "notify": {
                "enable_webhook": True,
                "feishu_webhook_url": "https://example.invalid/webhook",
                "on_download_failure": True,
                "on_wifi_failure": True,
                "on_upload_failure": True,
            }
        },
        "input": {"buildings": ["A", "B"]},
        "handover_log": {
            "download": {},
            "template": {"output_dir": str(tmp_path / "handover_output")},
            "day_metric_export": {
                "enabled": True,
                "only_day_shift": True,
                "source": {"app_token": "app", "table_id": "tbl"},
            },
        },
        "day_metric_upload": {
            "enabled": True,
            "manual_button_enabled": True,
            "source": {"reuse_handover_download": True, "reuse_handover_rule_engine": True},
            "behavior": {
                "only_day_shift": True,
                "failure_policy": "continue",
                "rewrite_existing": True,
                "basic_retry_attempts": 1,
                "basic_retry_backoff_sec": 0,
                "network_retry_attempts": 5,
                "network_retry_backoff_sec": 0,
                "alert_after_attempts": 5,
                "local_import_enabled": True,
                "local_import_scope": "single_date_single_building",
            },
        },
    }


def _patch_services(monkeypatch) -> None:  # noqa: ANN001
    _FakeNotifyService.calls = []
    _FakeDownloadService.prepare_internal_calls = 0
    _FakeDownloadService.ensure_internal_calls = 0
    _FakeDownloadService.ensure_external_calls = 0
    _FakeDownloadService.run_calls = []
    _FakeDownloadService.workflow_log = []
    _FakeDownloadService.failed_buildings_by_date = {}
    _FakeSourceCalcService.calls = []
    _FakeSourceDataAttachmentExportService.calls = []
    _FakeSourceDataAttachmentExportService.workflow_log = []
    _FakeSourceDataAttachmentExportService.remaining_failures_by_key = {}
    _FakeDayMetricExportService.run_calls = []
    _FakeDayMetricExportService.workflow_log = []
    _FakeDayMetricExportService.remaining_failures_by_key = {}
    monkeypatch.setattr(module, "WebhookNotifyService", _FakeNotifyService)
    monkeypatch.setattr(module, "HandoverDownloadService", _FakeDownloadService)
    monkeypatch.setattr(module, "DayMetricSourceCalcService", _FakeSourceCalcService)
    monkeypatch.setattr(module, "SourceDataAttachmentBitableExportService", _FakeSourceDataAttachmentExportService)
    monkeypatch.setattr(module, "DayMetricBitableExportService", _FakeDayMetricExportService)


def test_run_from_download_switches_network_once_and_downloads_before_processing(monkeypatch, tmp_path: Path) -> None:
    _patch_services(monkeypatch)
    service = module.DayMetricStandaloneUploadService(_runtime_cfg(tmp_path, auto_switch=True))

    result = service.run_from_download(
        selected_dates=["2026-03-20", "2026-03-21"],
        building_scope="single",
        building="A",
        emit_log=lambda *_args: None,
    )

    assert result["status"] == "ok"
    assert result["success_units"] == 2
    # 当前版本已取消切网流程；仅保留网络侧就绪检查。
    assert _FakeDownloadService.prepare_internal_calls == 0
    assert _FakeDownloadService.ensure_external_calls == 4
    assert len(_FakeDownloadService.run_calls) == 2
    assert all(call["switch_network"] is False for call in _FakeDownloadService.run_calls)
    assert all(call["reuse_cached"] is True for call in _FakeDownloadService.run_calls)
    assert _FakeDownloadService.run_calls[0]["start_time"] == "2026-03-20 12:00:00"
    assert _FakeDownloadService.run_calls[0]["end_time"] == "2026-03-20 12:20:00"
    assert len(_FakeSourceDataAttachmentExportService.calls) == 2
    assert len(_FakeDayMetricExportService.run_calls) == 2
    assert len(_FakeSourceCalcService.calls) == 2
    assert result["results"][0]["buildings"][0]["output_file"] == ""


def test_run_from_download_retries_download_failure_five_times_and_persists(monkeypatch, tmp_path: Path) -> None:
    _patch_services(monkeypatch)
    _FakeDownloadService.failed_buildings_by_date = {"2026-03-20": {"A"}}
    service = module.DayMetricStandaloneUploadService(_runtime_cfg(tmp_path, auto_switch=False))

    result = service.run_from_download(
        selected_dates=["2026-03-20"],
        building_scope="single",
        building="A",
        emit_log=lambda *_args: None,
    )

    row = result["results"][0]["buildings"][0]
    assert result["status"] == "failed"
    assert row["stage"] == "download"
    assert row["attempts"] == 5
    assert row["network_side"] == "internal"
    assert _FakeDownloadService.ensure_internal_calls == 5
    assert len(_FakeDownloadService.run_calls) == 5
    assert _FakeSourceDataAttachmentExportService.calls == []
    assert _FakeNotifyService.calls[-1]["category"] == "download"
    state_path = service._failed_units_state_path()
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["units"][0]["duty_date"] == "2026-03-20"
    assert payload["units"][0]["building"] == "A"
    assert payload["units"][0]["stage"] == "download"


def test_run_from_download_retries_attachment_failure_five_times_and_alerts(monkeypatch, tmp_path: Path) -> None:
    _patch_services(monkeypatch)
    _FakeSourceDataAttachmentExportService.remaining_failures_by_key = {("2026-03-20", "A"): 5}
    service = module.DayMetricStandaloneUploadService(_runtime_cfg(tmp_path, auto_switch=False))

    result = service.run_from_download(
        selected_dates=["2026-03-20"],
        building_scope="single",
        building="A",
        emit_log=lambda *_args: None,
    )

    row = result["results"][0]["buildings"][0]
    assert result["status"] == "failed"
    assert row["stage"] == "attachment"
    assert row["attempts"] == 5
    assert row["network_side"] == "external"
    assert _FakeDownloadService.ensure_external_calls == 5
    assert len(_FakeSourceDataAttachmentExportService.calls) == 5
    assert _FakeDayMetricExportService.run_calls == []
    assert _FakeNotifyService.calls[-1]["category"] == "upload"


def test_run_from_download_prefers_shared_day_cache_before_download(monkeypatch, tmp_path: Path) -> None:
    _patch_services(monkeypatch)
    shared_source = tmp_path / "shared_cache" / "A_2026-03-20.xlsx"
    shared_source.parent.mkdir(parents=True, exist_ok=True)
    shared_source.write_bytes(b"demo")

    class _FakeSharedBridgeStore:
        def __init__(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
            pass

        def ensure_ready(self):  # noqa: D401
            return None

    class _FakeSharedSourceCacheService:
        get_calls: list[dict] = []
        fill_calls: list[dict] = []

        def __init__(self, *, runtime_config, store, download_browser_pool, emit_log):  # noqa: ANN001
            self.runtime_config = runtime_config
            self.store = store
            self.download_browser_pool = download_browser_pool
            self.emit_log = emit_log

        def get_day_metric_by_date_entries(self, *, selected_dates, buildings):  # noqa: ANN001
            self.__class__.get_calls.append({"selected_dates": list(selected_dates), "buildings": list(buildings)})
            return [
                {
                    "duty_date": "2026-03-20",
                    "building": "A",
                    "file_path": str(shared_source),
                }
            ]

        def fill_day_metric_history(self, *, selected_dates, building_scope, building, emit_log):  # noqa: ANN001
            self.__class__.fill_calls.append(
                {
                    "selected_dates": list(selected_dates),
                    "building_scope": building_scope,
                    "building": building,
                }
            )
            return [
                {
                    "duty_date": "2026-03-20",
                    "building": "A",
                    "file_path": str(shared_source),
                }
            ]

    _FakeSharedSourceCacheService.get_calls = []
    _FakeSharedSourceCacheService.fill_calls = []
    monkeypatch.setattr(module, "SharedBridgeStore", _FakeSharedBridgeStore)
    monkeypatch.setattr(module, "SharedSourceCacheService", _FakeSharedSourceCacheService)

    cfg = _runtime_cfg(tmp_path, auto_switch=False)
    cfg["deployment"] = {"role_mode": "external"}
    cfg["shared_bridge"] = {"enabled": True, "root_dir": str(tmp_path / "shared")}
    service = module.DayMetricStandaloneUploadService(cfg)

    result = service.run_from_download(
        selected_dates=["2026-03-20"],
        building_scope="single",
        building="A",
        emit_log=lambda *_args: None,
    )

    assert result["status"] == "ok"
    assert result["internal"]["downloaded_file_count"] == 1
    assert result["internal"]["downloaded_files"] == [
        {
            "duty_date": "2026-03-20",
            "building": "A",
            "source_file": str(shared_source),
        }
    ]
    assert _FakeSharedSourceCacheService.fill_calls == [
        {
            "selected_dates": ["2026-03-20"],
            "building_scope": "single",
            "building": "A",
        }
    ]
    assert _FakeSharedSourceCacheService.get_calls == [{"selected_dates": ["2026-03-20"], "buildings": ["A"]}]
    assert _FakeDownloadService.run_calls == []
    assert _FakeDownloadService.ensure_internal_calls == 0
    assert len(_FakeSourceDataAttachmentExportService.calls) == 1
    assert len(_FakeDayMetricExportService.run_calls) == 1
    assert len(_FakeSourceCalcService.calls) == 1


def test_retry_unit_reuses_persisted_source_file_for_upload_stage(monkeypatch, tmp_path: Path) -> None:
    _patch_services(monkeypatch)
    _FakeDayMetricExportService.remaining_failures_by_key = {("2026-03-20", "A"): 5}
    service = module.DayMetricStandaloneUploadService(_runtime_cfg(tmp_path, auto_switch=False))

    first = service.run_from_download(
        selected_dates=["2026-03-20"],
        building_scope="single",
        building="A",
        emit_log=lambda *_args: None,
    )
    row = first["results"][0]["buildings"][0]
    assert row["stage"] == "upload"
    assert row["status"] == "failed"
    assert len(_FakeDownloadService.run_calls) == 1

    _FakeDayMetricExportService.remaining_failures_by_key = {}
    retry = service.retry_unit(
        mode="from_download",
        duty_date="2026-03-20",
        building="A",
        emit_log=lambda *_args: None,
    )

    retried_row = retry["results"][0]["buildings"][0]
    assert retry["status"] == "ok"
    assert retried_row["status"] == "ok"
    assert retried_row["stage"] == "upload"
    assert len(_FakeDownloadService.run_calls) == 1
    payload = json.loads(service._failed_units_state_path().read_text(encoding="utf-8"))
    assert payload["units"] == []


def test_run_from_file_uses_direct_export_without_output_file(monkeypatch, tmp_path: Path) -> None:
    _patch_services(monkeypatch)
    source_file = tmp_path / "input.xlsx"
    source_file.write_bytes(b"demo")
    service = module.DayMetricStandaloneUploadService(_runtime_cfg(tmp_path, auto_switch=True))

    result = service.run_from_file(
        building="A",
        duty_date="2026-03-24",
        file_path=str(source_file),
        emit_log=lambda *_args: None,
    )

    row = result["results"][0]["buildings"][0]
    assert result["status"] == "ok"
    assert row["output_file"] == ""
    assert row["retryable"] is False
    assert _FakeSourceDataAttachmentExportService.calls[0]["data_file"] == str(source_file)
    assert _FakeSourceCalcService.calls[0]["duty_date"] == "2026-03-24"
    assert "city_power" in _FakeDayMetricExportService.run_calls[0]["metric_origin_context"]["by_metric_id"]


def test_run_from_file_skips_attachment_when_source_calc_fails(monkeypatch, tmp_path: Path) -> None:
    _patch_services(monkeypatch)

    class _FailingSourceCalcService:
        calls: list[dict] = []

        def __init__(self, cfg):  # noqa: ANN001
            self.cfg = cfg

        def calculate(self, *, building, data_file, duty_date):  # noqa: ANN001
            self.__class__.calls.append(
                {
                    "building": building,
                    "data_file": data_file,
                    "duty_date": duty_date,
                }
            )
            raise ValueError("交接班源文件E列无有效数据")

    source_file = tmp_path / "input.xlsx"
    source_file.write_bytes(b"demo")
    monkeypatch.setattr(module, "DayMetricSourceCalcService", _FailingSourceCalcService)
    service = module.DayMetricStandaloneUploadService(_runtime_cfg(tmp_path, auto_switch=False))

    result = service.run_from_file(
        building="A",
        duty_date="2026-03-24",
        file_path=str(source_file),
        emit_log=lambda *_args: None,
    )

    row = result["results"][0]["buildings"][0]
    assert result["status"] == "failed"
    assert row["stage"] == "extract"
    assert _FakeSourceDataAttachmentExportService.calls == []
    assert _FakeDayMetricExportService.run_calls == []
