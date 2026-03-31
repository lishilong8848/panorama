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


class _FakeExtractService:
    def __init__(self, cfg):  # noqa: ANN001
        self.cfg = cfg

    def extract(self, *, building, data_file):  # noqa: ANN001
        return {
            "hits": {
                "city_power": {
                    "row_index": 12,
                    "b_norm": "A-401",
                    "c_norm": "R01",
                    "b_text": "A-401",
                    "c_text": "R01",
                }
            },
            "effective_config": {
                "building": building,
                "data_file": data_file,
                "cell_mapping": {"city_power": "D6"},
            },
        }


class _FakeFillService:
    calls: list[dict] = []

    def __init__(self, cfg):  # noqa: ANN001
        self.cfg = cfg

    def fill(
        self,
        *,
        building,
        data_file,
        hits,
        effective_config,
        date_ref_override,
        write_output_file,
        emit_log,  # noqa: ANN001
    ):
        self.__class__.calls.append(
            {
                "building": building,
                "data_file": data_file,
                "date_ref_override": date_ref_override,
                "write_output_file": write_output_file,
            }
        )
        return {
            "output_file": "",
            "resolved_values_by_id": {"day_metric": 1},
            "final_cell_values": {"D6": 11},
            "hits": hits,
            "effective_config": effective_config,
            "source_file": data_file,
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
    serialize_calls: list[dict] = []
    workflow_log: list[str] = []
    remaining_failures_by_key: dict[tuple[str, str], int] = {}

    def __init__(self, cfg):  # noqa: ANN001
        self.cfg = cfg

    def serialize_metric_origin_context(self, *, hits, effective_config):  # noqa: ANN001
        payload = {"by_metric_id": hits, "by_target_cell": effective_config.get("cell_mapping", {})}
        self.__class__.serialize_calls.append({"hits": hits, "effective_config": effective_config, "payload": payload})
        return payload

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
    _FakeFillService.calls = []
    _FakeSourceDataAttachmentExportService.calls = []
    _FakeSourceDataAttachmentExportService.workflow_log = []
    _FakeSourceDataAttachmentExportService.remaining_failures_by_key = {}
    _FakeDayMetricExportService.run_calls = []
    _FakeDayMetricExportService.serialize_calls = []
    _FakeDayMetricExportService.workflow_log = []
    _FakeDayMetricExportService.remaining_failures_by_key = {}
    monkeypatch.setattr(module, "WebhookNotifyService", _FakeNotifyService)
    monkeypatch.setattr(module, "HandoverDownloadService", _FakeDownloadService)
    monkeypatch.setattr(module, "HandoverExtractService", _FakeExtractService)
    monkeypatch.setattr(module, "HandoverFillService", _FakeFillService)
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
    assert _FakeDownloadService.prepare_internal_calls == 1
    assert _FakeDownloadService.ensure_external_calls == 5
    assert len(_FakeDownloadService.run_calls) == 2
    assert all(call["switch_network"] is False for call in _FakeDownloadService.run_calls)
    assert all(call["reuse_cached"] is True for call in _FakeDownloadService.run_calls)
    assert _FakeDownloadService.run_calls[0]["start_time"] == "2026-03-20 12:00:00"
    assert _FakeDownloadService.run_calls[0]["end_time"] == "2026-03-20 12:20:00"
    assert len(_FakeSourceDataAttachmentExportService.calls) == 2
    assert len(_FakeDayMetricExportService.run_calls) == 2
    assert all(call["write_output_file"] is False for call in _FakeFillService.calls)
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
    assert _FakeFillService.calls[0]["write_output_file"] is False
    assert _FakeDayMetricExportService.run_calls[0]["metric_origin_context"]["by_target_cell"] == {"city_power": "D6"}
